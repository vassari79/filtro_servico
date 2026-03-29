#include <WiFi.h>
#include <HTTPClient.h>
#include <WebServer.h>
#include <WiFiClientSecure.h>
#include <UrlEncode.h>
#include <LittleFS.h>
#include <time.h>
#include "credentials.h"

// Dynamic users: added at runtime via /addUser (admin-only)
#define MAX_DYNAMIC_USERS 10
String dynamicUsers[MAX_DYNAMIC_USERS];
String dynamicUserNames[MAX_DYNAMIC_USERS];
int numDynamicUsers = 0;

// --- Flow sensor ---
#define FLOW_PIN    27
#define BUZZER_PIN  26

// --- Auto-rotation ---
#define MAX_HISTORY_ENTRIES 650
#define MAX_ARCHIVE_FILES  3
// Pulses per liter: YF-S201(1/2")=450, FS400A G1(1")=60
// FS400A G1 DN25: F(Hz) = 4.8 * Q(L/min) → 4.8*60 = 288 pulses/liter
// Adjust after calibrating with a known volume if needed
#define CALIBRATION_FACTOR 288.0
#define MIN_FLOW_RATE 0.2       // L/min minimum to count as real flow
#define FLOW_TIMEOUT 3000

volatile uint32_t pulseCount = 0;
unsigned long lastPulseTime  = 0;

float  flowRate      = 0.0;
float  totalLiters   = 0.0;
float  sessionLiters = 0.0;
float  runMax        = 0.0;
float  lastRunMax    = 0.0;
float  lastRunLiters = 0.0;

bool   flowing       = false;

unsigned long runStartTime   = 0;
unsigned long lastCalcTime   = 0;
unsigned long lastWiFiReconnectAttempt = 0;
unsigned long wifiFailingSince = 0;
unsigned long loopStartTime = 0;
const unsigned long CALC_INTERVAL           = 1000;
const unsigned long WIFI_RECONNECT_INTERVAL = 15000;
const unsigned long WIFI_CONNECT_TIMEOUT    = 30000;
const unsigned long WIFI_RESET_TIMEOUT      = 120000;

// --- Web server ---
WebServer webServer(80);
bool fsAvailable = false;

int historyCount = 0;
int archiveCount = 0;
String pendingTelegramMsg = "";
long lastTgUpdateId = 0;
unsigned long lastTgCheck = 0;
const unsigned long TG_CHECK_INTERVAL = 10000;

// Per-user notification state: 0=unmuted, 1=muted, 2=digest
// Stored per-tier; all default to muted (1)
int adminNotifyMode[sizeof(ADMIN_USERS) / sizeof(ADMIN_USERS[0])];
String adminDigestBuf[sizeof(ADMIN_USERS) / sizeof(ADMIN_USERS[0])];
int presetNotifyMode[sizeof(PRESET_USERS) / sizeof(PRESET_USERS[0])];
String presetDigestBuf[sizeof(PRESET_USERS) / sizeof(PRESET_USERS[0])];
int dynamicNotifyMode[MAX_DYNAMIC_USERS];
String dynamicDigestBuf[MAX_DYNAMIC_USERS];
const unsigned long DIGEST_INTERVAL = 3UL * 3600UL * 1000UL; // 3 hours in ms
unsigned long lastDigestFlush = 0;

// --- User management helpers ---
bool isAdmin(const String& id) {
    for (int i = 0; i < NUM_ADMIN_USERS; i++) {
        if (ADMIN_USERS[i] == id) return true;
    }
    return false;
}

bool isUserAllowed(const String& id) {
    if (isAdmin(id)) return true;
    for (int i = 0; i < NUM_PRESET_USERS; i++) {
        if (PRESET_USERS[i] == id) return true;
    }
    for (int i = 0; i < numDynamicUsers; i++) {
        if (dynamicUsers[i] == id) return true;
    }
    return false;
}

int getUserNotifyMode(const String& id) {
    for (int i = 0; i < NUM_ADMIN_USERS; i++) {
        if (ADMIN_USERS[i] == id) return adminNotifyMode[i];
    }
    for (int i = 0; i < NUM_PRESET_USERS; i++) {
        if (PRESET_USERS[i] == id) return presetNotifyMode[i];
    }
    for (int i = 0; i < numDynamicUsers; i++) {
        if (dynamicUsers[i] == id) return dynamicNotifyMode[i];
    }
    return 1; // unknown = muted
}

void setUserNotifyMode(const String& id, int mode) {
    for (int i = 0; i < NUM_ADMIN_USERS; i++) {
        if (ADMIN_USERS[i] == id) { adminNotifyMode[i] = mode; return; }
    }
    for (int i = 0; i < NUM_PRESET_USERS; i++) {
        if (PRESET_USERS[i] == id) { presetNotifyMode[i] = mode; return; }
    }
    for (int i = 0; i < numDynamicUsers; i++) {
        if (dynamicUsers[i] == id) { dynamicNotifyMode[i] = mode; return; }
    }
}

String* getUserDigestBuf(const String& id) {
    for (int i = 0; i < NUM_ADMIN_USERS; i++) {
        if (ADMIN_USERS[i] == id) return &adminDigestBuf[i];
    }
    for (int i = 0; i < NUM_PRESET_USERS; i++) {
        if (PRESET_USERS[i] == id) return &presetDigestBuf[i];
    }
    for (int i = 0; i < numDynamicUsers; i++) {
        if (dynamicUsers[i] == id) return &dynamicDigestBuf[i];
    }
    return nullptr;
}

bool addDynamicUser(const String& id, const String& name) {
    if (isUserAllowed(id)) return false;
    if (numDynamicUsers >= MAX_DYNAMIC_USERS) return false;
    dynamicUsers[numDynamicUsers] = id;
    dynamicUserNames[numDynamicUsers] = name;
    dynamicNotifyMode[numDynamicUsers] = 1; // muted by default
    dynamicDigestBuf[numDynamicUsers] = "";
    numDynamicUsers++;
    return true;
}

bool removeDynamicUser(const String& id) {
    for (int i = 0; i < numDynamicUsers; i++) {
        if (dynamicUsers[i] == id) {
            for (int j = i; j < numDynamicUsers - 1; j++) {
                dynamicUsers[j] = dynamicUsers[j + 1];
                dynamicUserNames[j] = dynamicUserNames[j + 1];
                dynamicNotifyMode[j] = dynamicNotifyMode[j + 1];
                dynamicDigestBuf[j] = dynamicDigestBuf[j + 1];
            }
            numDynamicUsers--;
            dynamicUsers[numDynamicUsers] = "";
            dynamicUserNames[numDynamicUsers] = "";
            dynamicNotifyMode[numDynamicUsers] = 1;
            dynamicDigestBuf[numDynamicUsers] = "";
            return true;
        }
    }
    return false;
}

void saveUserData() {
    if (!fsAvailable) return;
    // Save dynamic users
    File f = LittleFS.open("/users.txt", "w");
    if (f) {
        for (int i = 0; i < numDynamicUsers; i++) {
            f.println(dynamicUsers[i] + "," + String(dynamicNotifyMode[i]) + "," + dynamicUserNames[i]);
        }
        f.close();
    }
    // Save admin + preset notify modes
    File af = LittleFS.open("/userprefs.txt", "w");
    if (af) {
        for (int i = 0; i < NUM_ADMIN_USERS; i++) {
            af.println(ADMIN_USERS[i] + "," + String(adminNotifyMode[i]));
        }
        for (int i = 0; i < NUM_PRESET_USERS; i++) {
            af.println(PRESET_USERS[i] + "," + String(presetNotifyMode[i]));
        }
        af.close();
    }
}

void loadUserData() {
    if (!fsAvailable) return;
    // Load dynamic users
    if (LittleFS.exists("/users.txt")) {
        File f = LittleFS.open("/users.txt", "r");
        if (f) {
            numDynamicUsers = 0;
            while (f.available() && numDynamicUsers < MAX_DYNAMIC_USERS) {
                String line = f.readStringUntil('\n');
                line.trim();
                if (line.length() == 0) continue;
                int c1 = line.indexOf(',');
                int c2 = (c1 >= 0) ? line.indexOf(',', c1 + 1) : -1;
                if (c1 > 0) {
                    dynamicUsers[numDynamicUsers] = line.substring(0, c1);
                    int mode = line.substring(c1 + 1, c2 > 0 ? c2 : line.length()).toInt();
                    if (mode < 0 || mode > 2) mode = 1;
                    dynamicNotifyMode[numDynamicUsers] = mode;
                    dynamicUserNames[numDynamicUsers] = (c2 > 0) ? line.substring(c2 + 1) : "";
                } else {
                    dynamicUsers[numDynamicUsers] = line;
                    dynamicNotifyMode[numDynamicUsers] = 1;
                    dynamicUserNames[numDynamicUsers] = "";
                }
                dynamicDigestBuf[numDynamicUsers] = "";
                numDynamicUsers++;
            }
            f.close();
        }
    }
    // Load admin + preset notify modes
    if (LittleFS.exists("/userprefs.txt")) {
        File af = LittleFS.open("/userprefs.txt", "r");
        if (af) {
            while (af.available()) {
                String line = af.readStringUntil('\n');
                line.trim();
                if (line.length() == 0) continue;
                int comma = line.indexOf(',');
                if (comma > 0) {
                    String id = line.substring(0, comma);
                    int mode = line.substring(comma + 1).toInt();
                    if (mode < 0 || mode > 2) mode = 0;
                    for (int i = 0; i < NUM_ADMIN_USERS; i++) {
                        if (ADMIN_USERS[i] == id) { adminNotifyMode[i] = mode; break; }
                    }
                    for (int i = 0; i < NUM_PRESET_USERS; i++) {
                        if (PRESET_USERS[i] == id) { presetNotifyMode[i] = mode; break; }
                    }
                }
            }
            af.close();
        }
    }
    // Migrate old per-user mute files if they exist
    for (int i = 0; i < NUM_ADMIN_USERS; i++) {
        String path = "/mute_" + ADMIN_USERS[i] + ".txt";
        if (LittleFS.exists(path)) {
            File f = LittleFS.open(path, "r");
            if (f) { int m = f.readString().toInt(); if (m >= 0 && m <= 2) adminNotifyMode[i] = m; f.close(); }
            LittleFS.remove(path);
        }
    }
    for (int i = 0; i < NUM_PRESET_USERS; i++) {
        String path = "/mute_" + PRESET_USERS[i] + ".txt";
        if (LittleFS.exists(path)) {
            File f = LittleFS.open(path, "r");
            if (f) { int m = f.readString().toInt(); if (m >= 0 && m <= 2) presetNotifyMode[i] = m; f.close(); }
            LittleFS.remove(path);
        }
    }
}

String notifyModeLabel(int mode) {
    switch (mode) {
        case 0: return "&#128264; UNMUTED";
        case 1: return "&#128263; MUTED";
        case 2: return "&#128340; DIGEST (cada 3h)";
        default: return "?";
    }
}

// NTP time
void setupNTP() {
    configTime(-3 * 3600, 0, "pool.ntp.org", "time.nist.gov"); // UTC-3 (Brazil)
    Serial.print("Waiting for NTP time");
    time_t now = time(nullptr);
    int tries = 0;
    while (now < 1000000000 && tries < 20) {
        delay(500);
        Serial.print(".");
        now = time(nullptr);
        tries++;
    }
    Serial.println(now > 1000000000 ? " OK" : " FAILED");
}

String getTimestamp() {
    time_t now = time(nullptr);
    if (now < 1000000000) return "no-time";
    struct tm* t = localtime(&now);
    char buf[20];
    snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:%02d",
             t->tm_year + 1900, t->tm_mon + 1, t->tm_mday,
             t->tm_hour, t->tm_min, t->tm_sec);
    return String(buf);
}

void saveTotalLiters() {
    File f = LittleFS.open("/totalliters.txt", "w");
    if (f) { f.print(totalLiters, 3); f.close(); }
}

void loadTotalLiters() {
    File f = LittleFS.open("/totalliters.txt", "r");
    if (f) { totalLiters = f.readString().toFloat(); f.close(); }
}

void saveLastUpdateId() {
    if (!fsAvailable) return;
    File f = LittleFS.open("/lastupdateid.txt", "w");
    if (f) { f.print(lastTgUpdateId); f.close(); }
}

void loadLastUpdateId() {
    File f = LittleFS.open("/lastupdateid.txt", "r");
    if (f) { lastTgUpdateId = f.readString().toInt(); f.close(); }
}

void autoArchiveIfNeeded();

void appendRunToFlash(float maxFlow, float volume) {
    if (!fsAvailable) return;
    File f = LittleFS.open("/current.csv", "a");
    if (!f) return;
    f.print(getTimestamp()); f.print(",");
    f.print(maxFlow, 2); f.print(","); f.print(volume, 3); f.print(","); f.println(totalLiters, 3);
    f.close();
    saveTotalLiters();
    historyCount++;
    if (historyCount >= MAX_HISTORY_ENTRIES) {
        autoArchiveIfNeeded();
    }
}

void autoArchiveIfNeeded() {
    if (!fsAvailable || historyCount < MAX_HISTORY_ENTRIES) return;

    // Archive current.csv
    archiveCount++;
    String src = "/current.csv";
    String dst = "/archive_" + String(archiveCount) + ".csv";
    if (!LittleFS.rename(src, dst)) {
        File fin = LittleFS.open(src, "r");
        File fout = LittleFS.open(dst, "w");
        if (fin && fout) {
            while (fin.available()) {
                String line = fin.readStringUntil('\n');
                line.trim();
                if (line.length() == 0) continue;
                fout.println(line);
            }
        }
        if (fin)  fin.close();
        if (fout) fout.close();
        LittleFS.remove(src);
    }
    File fc = LittleFS.open("/archivecount.txt", "w");
    if (fc) { fc.print(archiveCount); fc.close(); }

    int savedCount = historyCount;
    historyCount = 0;

    // Send the archived file to all admins
    for (int i = 0; i < NUM_ADMIN_USERS; i++) {
        sendTelegramDocumentTo(ADMIN_USERS[i], dst,
            "Auto-archive: " + String(savedCount) + " corridas -> archive_" + String(archiveCount));
    }

    // Delete old archives beyond MAX_ARCHIVE_FILES
    int oldest = archiveCount - MAX_ARCHIVE_FILES;
    for (int i = oldest; i >= 1; i--) {
        String old = "/archive_" + String(i) + ".csv";
        if (LittleFS.exists(old)) {
            LittleFS.remove(old);
            Serial.println("Auto-rotation: deleted " + old);
        }
    }

    Serial.printf("Auto-archived %d entries -> %s, kept last %d archives\n",
                  savedCount, dst.c_str(), MAX_ARCHIVE_FILES);
}

void countHistoryFromFlash() {
    historyCount = 0;
    if (!fsAvailable) return;
    if (!LittleFS.exists("/current.csv")) return;
    File f = LittleFS.open("/current.csv", "r");
    if (!f) return;
    while (f.available()) {
        String line = f.readStringUntil('\n');
        line.trim();
        if (line.length() > 0) historyCount++;
    }
    f.close();
}

void handleRoot() {
    webServer.setContentLength(CONTENT_LENGTH_UNKNOWN);
    webServer.send(200, "text/html", "");

    webServer.sendContent(
        "<!DOCTYPE html><html><head>"
        "<meta charset='UTF-8'>"
        "<meta http-equiv='refresh' content='3'>"
        "<title>Monitor de Flujo</title>"
        "<style>"
        "body{font-family:monospace;background:#111;color:#eee;padding:20px;}"
        "h2{color:#4cf;}"
        "table{border-collapse:collapse;width:100%;max-width:600px;}"
        "th{background:#333;padding:8px 14px;text-align:left;}"
        "td{padding:6px 14px;border-bottom:1px solid #333;}"
        ".live{background:#1a2a1a;border-radius:8px;padding:14px;margin-bottom:20px;max-width:500px;}"
        ".label{color:#aaa;font-size:0.85em;}"
        ".val{font-size:1.3em;color:#4f4;}"
        ".idle{color:#fa4;}"
        ".btn{background:#c33;color:#fff;border:none;padding:10px 22px;font-size:1em;"
        "border-radius:6px;cursor:pointer;margin-top:16px;}"
        ".arch a{color:#8af;text-decoration:none;margin-right:14px;}"
        "</style></head><body>"
        "<h2>Monitor de Flujo</h2>"
    );

    // Live status
    webServer.sendContent("<div class='live'>");
    if (flowing) {
        bool warmedUp = (millis() - runStartTime >= 2000);
        webServer.sendContent(
            "<div class='label'>Estado</div><div class='val'>&#128167; FLUYENDO</div>"
            "<div class='label'>Caudal actual</div><div class='val'>" + String(flowRate, 1) + " L/min</div>"
            "<div class='label'>Maximo corrida" + String(warmedUp ? "" : " *") + "</div><div class='val'>" + String(runMax, 2) + " L/min</div>"
            "<div class='label'>Volumen corrida</div><div class='val'>" + String(sessionLiters, 3) + " L</div>"
        );
    } else {
        webServer.sendContent(
            "<div class='label'>Estado</div><div class='val idle'>IDLE</div>"
            "<div class='label'>Maximo ultima corrida</div><div class='val'>" + String(lastRunMax, 2) + " L/min</div>"
            "<div class='label'>Volumen ultima corrida</div><div class='val'>" + String(lastRunLiters, 3) + " L</div>"
        );
    }
    webServer.sendContent("</div>");

    // Full history table
    if (historyCount > 0 && fsAvailable && LittleFS.exists("/current.csv")) {
        webServer.sendContent("<h2>Historial (" + String(historyCount) + " corridas)</h2>");
        webServer.sendContent("<table><tr><th>#</th><th>Fecha</th><th>Maximo (L/min)</th><th>Volumen (L)</th></tr>");
        File f = LittleFS.open("/current.csv", "r");
        if (f) {
            const int BUF = 200;
            String* lines = new String[BUF];
            int total = 0;
            while (f.available()) {
                String line = f.readStringUntil('\n');
                line.trim();
                if (line.length() > 0) { lines[total % BUF] = line; total++; }
            }
            f.close();
            int show = total < BUF ? total : BUF;
            for (int i = show - 1; i >= 0; i--) {
                int idx = (total - show + i) % BUF;
                // New format: datetime,maxFlow,volume,cumVol
                // Old format: maxFlow,volume,cumVol
                String line = lines[idx];
                int c1 = line.indexOf(',');
                if (c1 < 0) continue;
                String dateStr = "";
                String rest = line;
                // Detect new format: first field contains '-' (date) or space
                String firstField = line.substring(0, c1);
                if (firstField.indexOf('-') >= 0) {
                    // New format: datetime,max,vol,cumVol
                    dateStr = firstField;
                    rest = line.substring(c1 + 1);
                    c1 = rest.indexOf(',');
                    if (c1 < 0) continue;
                }
                webServer.sendContent(
                    "<tr><td>" + String(total - (show - 1 - i)) + "</td>"
                    "<td>" + dateStr + "</td>"
                    "<td>" + rest.substring(0, c1) + "</td>"
                    "<td>" + rest.substring(c1 + 1) + "</td></tr>"
                );
            }
            delete[] lines;
        }
        webServer.sendContent("</table>");
        webServer.sendContent(
            "<div style='margin-top:18px;'>"
            "<a href='/clear-confirm'><button class='btn'>&#128190; Guardar y borrar historial</button></a>"
            "<a href='/chart'><button class='btn' style='background:#226;border-color:#8af;color:#8af;margin-left:8px;'>&#128200; Grafico</button></a>"
            "</div>"
        );
    } else if (historyCount == 0) {
        webServer.sendContent("<p style='color:#888'>Sin datos todavia.</p>");
    }

    // Archive links
    if (archiveCount > 0) {
        webServer.sendContent("<h2>Archivos guardados</h2><div class='arch'>");
        for (int i = archiveCount; i >= 1; i--) {
            webServer.sendContent(
                "<a href='/archive?n=" + String(i) + "' target='_blank'>"
                "<button style='background:#226;color:#8af;border:1px solid #8af;padding:7px 16px;"
                "border-radius:5px;cursor:pointer;margin:4px;font-size:0.9em;'>&#128190; Archivo "
                + String(i) + "</button></a>"
            );
        }
        webServer.sendContent("</div>");
    }

    webServer.sendContent("</body></html>");
    webServer.sendContent("");
}

void handleClearConfirm() {
    int n = historyCount;
    String html =
        "<!DOCTYPE html><html><head><meta charset='UTF-8'>"
        "<title>Confirmar</title>"
        "<style>body{font-family:monospace;background:#111;color:#eee;padding:30px;}"
        ".btn{padding:12px 28px;font-size:1em;border-radius:6px;cursor:pointer;margin:10px 8px 0 0;border:none;}"
        ".yes{background:#c33;color:#fff;} .no{background:#444;color:#eee;}</style></head><body>"
        "<h2>&#9888; Confirmar</h2>"
        "<p>Guardar <b>" + String(n) + "</b> corridas en archivo y borrar el historial?</p>"
        "<form method='POST' action='/clear'>"
        "<button class='btn yes' type='submit'>&#128190; Si, guardar y borrar</button>"
        "</form>"
        "<a href='/'><button class='btn no' type='button'>Cancelar</button></a>"
        "</body></html>";
    webServer.send(200, "text/html", html);
}

void handleClear() {
    if (historyCount == 0) {
        webServer.sendHeader("Location", "/");
        webServer.send(303, "text/plain", "");
        return;
    }
    archiveCount++;
    String src = "/current.csv";
    String dst = "/archive_" + String(archiveCount) + ".csv";
    if (fsAvailable) {
        if (!LittleFS.rename(src, dst)) {
            File fin = LittleFS.open(src, "r");
            File fout = LittleFS.open(dst, "w");
            if (fin && fout) {
                fout.print("corrida,max_Lmin,volumen_L\n");
                int row = 1;
                while (fin.available()) {
                    String line = fin.readStringUntil('\n');
                    line.trim();
                    if (line.length() == 0) continue;
                    fout.print(row++); fout.print(","); fout.println(line);
                }
            }
            if (fin)  fin.close();
            if (fout) fout.close();
            LittleFS.remove(src);
        }
        File fc = LittleFS.open("/archivecount.txt", "w");
        if (fc) { fc.print(archiveCount); fc.close(); }
    }
    String ip = WiFi.localIP().toString();
    pendingTelegramMsg = "<b>Historial guardado</b> (" + String(historyCount) + " corridas)\n"
                         "<a href=\"http://" + ip + "/archive?n=" + String(archiveCount) + "\">" + ip + "/archive?n=" + String(archiveCount) + "</a>";
    historyCount = 0;
    webServer.sendHeader("Location", "/");
    webServer.send(303, "text/plain", "");
}

void handleArchive() {
    if (!webServer.hasArg("n")) {
        webServer.send(400, "text/plain", "Missing n");
        return;
    }
    int n = webServer.arg("n").toInt();
    String filename = "/archive_" + String(n) + ".csv";
    if (!fsAvailable || !LittleFS.exists(filename)) {
        webServer.send(404, "text/plain", "Not found");
        return;
    }
    File f = LittleFS.open(filename, "r");
    if (!f) { webServer.send(500, "text/plain", "Error"); return; }

    webServer.setContentLength(CONTENT_LENGTH_UNKNOWN);
    webServer.send(200, "text/html", "");
    webServer.sendContent(
        "<!DOCTYPE html><html><head><meta charset='UTF-8'>"
        "<title>Archivo " + String(n) + "</title>"
        "<style>"
        "body{font-family:monospace;background:#111;color:#eee;padding:20px;}"
        "h2{color:#4cf;}"
        "table{border-collapse:collapse;width:100%;max-width:600px;}"
        "th{background:#333;padding:8px 14px;text-align:left;}"
        "td{padding:6px 14px;border-bottom:1px solid #333;text-align:right;}"
        "td:first-child{text-align:left;}"
        ".back{display:inline-block;margin-bottom:18px;color:#8af;text-decoration:none;}"
        ".dl{display:inline-block;margin-left:18px;color:#8af;font-size:0.9em;}"
        "</style></head><body>"
        "<a class='back' href='javascript:history.back()'>&#8592; Volver</a>"
        "<a class='dl' href='/archive-dl?n=" + String(n) + "'>&#128229; Descargar CSV</a>"
        "<h2>Archivo " + String(n) + "</h2>"
        "<table><tr><th>#</th><th>Max (L/min)</th><th>Volumen (L)</th></tr>"
    );

    int row = 1;
    while (f.available()) {
        String line = f.readStringUntil('\n');
        line.trim();
        if (line.length() == 0) continue;
        int comma = line.indexOf(',');
        if (comma < 0) continue;
        String maxVal = line.substring(0, comma);
        String volVal = line.substring(comma + 1);
        webServer.sendContent(
            "<tr><td>" + String(row++) + "</td>"
            "<td>" + maxVal + "</td>"
            "<td>" + volVal + "</td></tr>"
        );
    }
    f.close();

    webServer.sendContent("</table></body></html>");
    webServer.sendContent("");
}

void handleArchiveDownload() {
    if (!webServer.hasArg("n")) {
        webServer.send(400, "text/plain", "Missing n");
        return;
    }
    int n = webServer.arg("n").toInt();
    String filename = "/archive_" + String(n) + ".csv";
    if (!fsAvailable || !LittleFS.exists(filename)) {
        webServer.send(404, "text/plain", "Not found");
        return;
    }
    File f = LittleFS.open(filename, "r");
    webServer.sendHeader("Content-Disposition", "attachment; filename=archive_" + String(n) + ".csv");
    webServer.streamFile(f, "text/csv");
    f.close();
}

void handleDataCSV() {
    if (!fsAvailable || !LittleFS.exists("/current.csv")) {
        webServer.send(404, "text/plain", "No data");
        return;
    }
    File f = LittleFS.open("/current.csv", "r");
    webServer.streamFile(f, "text/csv");
    f.close();
}

void handleApiStatus() {
    String json = "{";
    json += "\"flowing\":" + String(flowing ? "true" : "false") + ",";
    json += "\"flowRate\":" + String(flowRate, 2) + ",";
    json += "\"runMax\":" + String(flowing ? runMax : lastRunMax, 2) + ",";
    json += "\"sessionLiters\":" + String(flowing ? sessionLiters : lastRunLiters, 3) + ",";
    json += "\"totalLiters\":" + String(totalLiters, 1) + ",";
    json += "\"historyCount\":" + String(historyCount) + ",";
    json += "\"archiveCount\":" + String(archiveCount) + ",";
    json += "\"heap\":" + String(ESP.getFreeHeap()) + ",";
    json += "\"uptime\":" + String(millis() / 1000);
    json += "}";
    webServer.send(200, "application/json", json);
}

void handleChart() {
    webServer.setContentLength(CONTENT_LENGTH_UNKNOWN);
    webServer.send(200, "text/html", "");
    webServer.sendContent(
        "<!DOCTYPE html><html><head><meta charset='UTF-8'>"
        "<title>Filter Health</title>"
        "<script src='https://cdn.jsdelivr.net/npm/chart.js'></script>"
        "<style>body{background:#111;color:#eee;font-family:sans-serif;margin:20px;}"
        "canvas{background:#1a1a2e;border-radius:10px;}"
        "select{background:#222;color:#eee;border:1px solid #555;padding:6px 12px;border-radius:5px;font-size:1em;margin:10px 0;}"
        "a{color:#8af;}</style></head><body>"
        "<h1>&#128200; Filter Health</h1>"
        "<select id='src'><option value='/data.csv'>Actual</option>"
    );
    for (int i = archiveCount; i >= 1; i--) {
        webServer.sendContent(
            "<option value='/archive-dl?n=" + String(i) + "'>Archivo " + String(i) + "</option>"
        );
    }
    webServer.sendContent(
        "</select>"
        "<button id='sendTg' style='background:#0088cc;color:#fff;border:none;padding:8px 16px;"
        "border-radius:5px;cursor:pointer;margin-left:10px;font-size:1em;'>&#9993; Enviar a Telegram</button>"
        "<canvas id='chart' width='800' height='400'></canvas>"
        "<p><a href='/'>&#8592; Volver</a></p>"
        "<script>"
        "let myChart=null;"
        "function loadChart(url){"
        "fetch(url).then(r=>r.text()).then(t=>{"
        "const lines=t.trim().split('\\n');"
        "const maxF=[],dates=[];"
        "lines.forEach(l=>{"
        "const p=l.split(',');"
        "if(p.length>=4){dates.push(p[0]);maxF.push(parseFloat(p[1]));}"
        "else if(p.length>=3){maxF.push(parseFloat(p[0]));dates.push('');}"
        "else if(p.length>=2){maxF.push(parseFloat(p[0]));dates.push('');}"
        "});"
        "const labels=dates.map((d,i)=>d||('#'+(i+1)));"
        "if(myChart)myChart.destroy();"
        "myChart=new Chart(document.getElementById('chart'),{"
        "type:'line',"
        "data:{labels:labels,datasets:[{label:'Max Flow (L/min)',data:maxF,"
        "borderColor:'rgba(33,150,243,0.9)',backgroundColor:'rgba(33,150,243,0.3)',"
        "pointRadius:4,fill:true,tension:0.2}]},"
        "options:{responsive:true,plugins:{legend:{labels:{color:'#eee'}}},"
        "scales:{x:{title:{display:true,text:'Fecha/Hora',color:'#aaa'},ticks:{color:'#aaa',maxRotation:45},grid:{color:'#333'}},"
        "y:{title:{display:true,text:'L/min',color:'#aaa'},ticks:{color:'#aaa'},grid:{color:'#333'}}}}"
        "});});"
        "}"
        "document.getElementById('src').addEventListener('change',function(){loadChart(this.value);});"
        "loadChart('/data.csv');"
        "document.getElementById('sendTg').addEventListener('click',function(){"
        "const sel=document.getElementById('src');"
        "fetch('/send-chart?src='+encodeURIComponent(sel.value))"
        ".then(r=>r.text()).then(t=>alert(t));"
        "});"
        "</script></body></html>"
    );
    webServer.sendContent("");
}

void handleSendChart() {
    String src = webServer.arg("src");
    String csvPath;
    String caption;
    if (src == "/data.csv" || src.length() == 0) {
        csvPath = "/current.csv";
        caption = "Filter Health - Actual";
    } else {
        int idx = src.indexOf("n=");
        String n = (idx >= 0) ? src.substring(idx + 2) : "?";
        csvPath = "/archive_" + n + ".csv";
        caption = "Filter Health - Archivo " + n;
    }
    String chartUrl = buildChartUrl(csvPath);
    if (chartUrl.length() == 0) {
        webServer.send(200, "text/plain", "No hay datos");
        return;
    }
    sendTelegramPhoto(chartUrl, caption);
    webServer.send(200, "text/plain", "Grafico enviado a Telegram!");
}

void IRAM_ATTR pulseISR() {
    static uint32_t lastPulseISR = 0;
    uint32_t now = micros();
    if (now - lastPulseISR >= 350) {    // 0.35ms debounce
        pulseCount++;
        lastPulseISR = now;
    }
}

void setupBuzzer() {
    pinMode(BUZZER_PIN, OUTPUT);
    digitalWrite(BUZZER_PIN, LOW);
}

void beep(int duration) {
    digitalWrite(BUZZER_PIN, HIGH);
    delay(duration);
    digitalWrite(BUZZER_PIN, LOW);
}

bool connectToWiFi() {
    if (WiFi.status() == WL_CONNECTED) {
        Serial.println("Already connected to WiFi");
        Serial.print("IP address: ");
        Serial.println(WiFi.localIP());
        return true;
    }

    unsigned long startTime = millis();
    WiFi.begin(ssid, password);

    while (WiFi.status() != WL_CONNECTED && millis() - startTime < WIFI_CONNECT_TIMEOUT) {
        Serial.print("Connecting to WiFi... status: ");
        Serial.println(WiFi.status());
        beep(35);
        delay(965);
    }

    if (WiFi.status() == WL_CONNECTED) {
        Serial.println("WiFi connected");
        Serial.print("IP address: ");
        Serial.println(WiFi.localIP());
        beep(200); delay(75);
        beep(200); delay(75);
        beep(200);
        return true;
    } else {
        Serial.println("WiFi connection failed. Timeout reached.");
        return false;
    }
}

// Send to a specific chat_id
void sendTelegramTo(const String& toId, String message) {
    if (WiFi.status() != WL_CONNECTED || toId.length() == 0) return;
    WiFiClientSecure client;
    client.setInsecure();
    HTTPClient http;
    String url = "https://api.telegram.org/bot" + botToken
                 + "/sendMessage?chat_id=" + toId
                 + "&parse_mode=HTML"
                 + "&text=" + urlEncode(message);
    http.begin(client, url);
    http.setTimeout(5000);
    int code = http.GET();
    Serial.println("sendTg->" + toId + ": HTTP " + String(code));
    if (code <= 0) Serial.println("sendTg error: " + http.errorToString(code));
    http.end();
}

// Send to all users (for command responses, ignores mute)
void sendTelegram(String message) {
    for (int i = 0; i < NUM_ADMIN_USERS; i++) sendTelegramTo(ADMIN_USERS[i], message);
    for (int i = 0; i < NUM_PRESET_USERS; i++) sendTelegramTo(PRESET_USERS[i], message);
    for (int i = 0; i < numDynamicUsers; i++) sendTelegramTo(dynamicUsers[i], message);
}

// Helper: iterate all user IDs for auto-message / digest / flush
void forEachUser(void (*fn)(const String& id, int mode, String* digestBuf)) {
    for (int i = 0; i < NUM_ADMIN_USERS; i++) fn(ADMIN_USERS[i], adminNotifyMode[i], &adminDigestBuf[i]);
    for (int i = 0; i < NUM_PRESET_USERS; i++) fn(PRESET_USERS[i], presetNotifyMode[i], &presetDigestBuf[i]);
    for (int i = 0; i < numDynamicUsers; i++) fn(dynamicUsers[i], dynamicNotifyMode[i], &dynamicDigestBuf[i]);
}

static String _autoMsgPayload;

void _sendAutoMsgCb(const String& id, int mode, String* digestBuf) {
    if (mode == 0) {
        sendTelegramTo(id, _autoMsgPayload);
    } else if (mode == 2) {
        if (digestBuf->length() > 0) *digestBuf += "\n---\n";
        *digestBuf += _autoMsgPayload;
    }
}

// Send auto-message: unmuted get it now, digest users buffer it, muted skip
void sendTelegramAutoMsg(String message) {
    _autoMsgPayload = message;
    forEachUser(_sendAutoMsgCb);
}

void _flushDigestCb(const String& id, int mode, String* digestBuf) {
    if (mode == 2 && digestBuf->length() > 0) {
        String msg = "&#128340; <b>Resumen (ultimas 3h)</b>\n\n" + *digestBuf;
        sendTelegramTo(id, msg);
        *digestBuf = "";
    }
}

// Flush digest buffers for all digest-mode users
void flushDigestBuffers() {
    forEachUser(_flushDigestCb);
}

void sendTelegramPhotoTo(const String& toId, String photoUrl, String caption) {
    if (WiFi.status() != WL_CONNECTED || toId.length() == 0) return;
    WiFiClientSecure client;
    client.setInsecure();
    HTTPClient http;
    String apiUrl = "https://api.telegram.org/bot" + botToken + "/sendPhoto";
    String body = "chat_id=" + toId + "&photo=" + urlEncode(photoUrl) + "&caption=" + urlEncode(caption);
    http.begin(client, apiUrl);
    http.setTimeout(5000);
    http.addHeader("Content-Type", "application/x-www-form-urlencoded");
    int code = http.POST(body);
    Serial.println("sendPhoto->" + toId + ": HTTP " + String(code));
    http.end();
}

void sendTelegramPhoto(String photoUrl, String caption) {
    for (int i = 0; i < NUM_ADMIN_USERS; i++) sendTelegramPhotoTo(ADMIN_USERS[i], photoUrl, caption);
    for (int i = 0; i < NUM_PRESET_USERS; i++) sendTelegramPhotoTo(PRESET_USERS[i], photoUrl, caption);
    for (int i = 0; i < numDynamicUsers; i++) sendTelegramPhotoTo(dynamicUsers[i], photoUrl, caption);
}

// Max bytes per CSV chunk to send (~100KB, half of safe heap)
#define MAX_CSV_CHUNK 102400

// Send a file from LittleFS as a Telegram document (multipart upload)
void sendTelegramDocumentTo(const String& toId, const String& filePath, const String& caption) {
    if (WiFi.status() != WL_CONNECTED || toId.length() == 0) return;
    if (!fsAvailable || !LittleFS.exists(filePath)) {
        sendTelegramTo(toId, "Archivo no encontrado: " + filePath);
        return;
    }

    File f = LittleFS.open(filePath, "r");
    if (!f) return;
    size_t fileSize = f.size();

    // If file fits in one chunk, send directly
    if (fileSize <= MAX_CSV_CHUNK) {
        String content = f.readString();
        f.close();

        // Extract filename from path
        String fname = filePath;
        int lastSlash = fname.lastIndexOf('/');
        if (lastSlash >= 0) fname = fname.substring(lastSlash + 1);

        String boundary = "----ESP32Boundary";
        String bodyStart = "--" + boundary + "\r\n"
            "Content-Disposition: form-data; name=\"chat_id\"\r\n\r\n" + toId + "\r\n"
            "--" + boundary + "\r\n"
            "Content-Disposition: form-data; name=\"caption\"\r\n\r\n" + caption + "\r\n"
            "--" + boundary + "\r\n"
            "Content-Disposition: form-data; name=\"document\"; filename=\"" + fname + "\"\r\n"
            "Content-Type: text/csv\r\n\r\n";
        String bodyEnd = "\r\n--" + boundary + "--\r\n";

        WiFiClientSecure client;
        client.setInsecure();
        HTTPClient http;
        String apiUrl = "https://api.telegram.org/bot" + botToken + "/sendDocument";
        http.begin(client, apiUrl);
        http.setTimeout(5000);
        http.addHeader("Content-Type", "multipart/form-data; boundary=" + boundary);
        int code = http.POST(bodyStart + content + bodyEnd);
        Serial.println("sendDoc->" + toId + ": HTTP " + String(code) + " size=" + String(fileSize));
        http.end();
        return;
    }

    // File too big — split into chunks by lines
    int part = 1;
    String chunk = "";
    int lineCount = 0;
    while (f.available()) {
        String line = f.readStringUntil('\n');
        line.trim();
        if (line.length() == 0) continue;

        if (chunk.length() + line.length() + 2 > MAX_CSV_CHUNK && chunk.length() > 0) {
            // Send this chunk
            String fname = filePath;
            int lastSlash = fname.lastIndexOf('/');
            if (lastSlash >= 0) fname = fname.substring(lastSlash + 1);
            int dotPos = fname.lastIndexOf('.');
            String base = (dotPos > 0) ? fname.substring(0, dotPos) : fname;
            String ext  = (dotPos > 0) ? fname.substring(dotPos) : ".csv";
            String partName = base + "_part" + String(part) + ext;
            String partCaption = caption + " (parte " + String(part) + ")";

            String boundary = "----ESP32Boundary";
            String bodyStart = "--" + boundary + "\r\n"
                "Content-Disposition: form-data; name=\"chat_id\"\r\n\r\n" + toId + "\r\n"
                "--" + boundary + "\r\n"
                "Content-Disposition: form-data; name=\"caption\"\r\n\r\n" + partCaption + "\r\n"
                "--" + boundary + "\r\n"
                "Content-Disposition: form-data; name=\"document\"; filename=\"" + partName + "\"\r\n"
                "Content-Type: text/csv\r\n\r\n";
            String bodyEnd = "\r\n--" + boundary + "--\r\n";

            WiFiClientSecure client;
            client.setInsecure();
            HTTPClient http;
            http.begin(client, "https://api.telegram.org/bot" + botToken + "/sendDocument");
            http.setTimeout(5000);
            http.addHeader("Content-Type", "multipart/form-data; boundary=" + boundary);
            int code = http.POST(bodyStart + chunk + bodyEnd);
            Serial.println("sendDoc part" + String(part) + "->" + toId + ": HTTP " + String(code));
            http.end();

            chunk = "";
            part++;
            delay(500); // avoid rate limiting
        }
        chunk += line + "\n";
        lineCount++;
    }
    f.close();

    // Send remaining chunk
    if (chunk.length() > 0) {
        String fname = filePath;
        int lastSlash = fname.lastIndexOf('/');
        if (lastSlash >= 0) fname = fname.substring(lastSlash + 1);
        int dotPos = fname.lastIndexOf('.');
        String base = (dotPos > 0) ? fname.substring(0, dotPos) : fname;
        String ext  = (dotPos > 0) ? fname.substring(dotPos) : ".csv";
        String partName = (part > 1) ? base + "_part" + String(part) + ext : fname;
        String partCaption = (part > 1) ? caption + " (parte " + String(part) + ")" : caption;

        String boundary = "----ESP32Boundary";
        String bodyStart = "--" + boundary + "\r\n"
            "Content-Disposition: form-data; name=\"chat_id\"\r\n\r\n" + toId + "\r\n"
            "--" + boundary + "\r\n"
            "Content-Disposition: form-data; name=\"caption\"\r\n\r\n" + partCaption + "\r\n"
            "--" + boundary + "\r\n"
            "Content-Disposition: form-data; name=\"document\"; filename=\"" + partName + "\"\r\n"
            "Content-Type: text/csv\r\n\r\n";
        String bodyEnd = "\r\n--" + boundary + "--\r\n";

        WiFiClientSecure client;
        client.setInsecure();
        HTTPClient http;
        http.begin(client, "https://api.telegram.org/bot" + botToken + "/sendDocument");
        http.setTimeout(5000);
        http.addHeader("Content-Type", "multipart/form-data; boundary=" + boundary);
        int code = http.POST(bodyStart + chunk + bodyEnd);
        Serial.println("sendDoc part" + String(part) + "->" + toId + ": HTTP " + String(code));
        http.end();
    }
    if (part > 1) {
        sendTelegramTo(toId, "Archivo dividido en " + String(part) + " partes.");
    }
}

String buildChartUrl(String csvPath) {
    if (!fsAvailable || !LittleFS.exists(csvPath)) return "";
    File f = LittleFS.open(csvPath, "r");
    if (!f) return "";

    // ESP32 has plenty of RAM — use up to 300 points (QuickChart URL length limit)
    String dp = "";
    String labels = "";
    int count = 0;
    while (f.available() && count < 300) {
        String line = f.readStringUntil('\n');
        line.trim();
        if (line.length() == 0) continue;
        int c1 = line.indexOf(',');
        if (c1 < 0) continue;
        // Detect new format: first field is datetime (contains '-')
        String firstField = line.substring(0, c1);
        String dataFields = line;
        String dateLabel = "";
        if (firstField.indexOf('-') >= 0) {
            dateLabel = firstField;
            dataFields = line.substring(c1 + 1); // skip datetime
        }
        int d1 = dataFields.indexOf(',');
        if (d1 < 0) continue;
        float maxF = dataFields.substring(0, d1).toFloat();
        if (count > 0) { dp += ","; labels += ","; }
        dp += String(maxF, 1);
        // Use short date label (MM-DD HH:MM) for readability
        if (dateLabel.length() >= 16) {
            labels += "'" + dateLabel.substring(5, 16) + "'";
        } else {
            labels += "'" + String(count + 1) + "'";
        }
        count++;
    }
    f.close();

    if (count == 0) return "";
    Serial.println("buildChart: " + String(count) + " pts, dp_len=" + String(dp.length()));

    String cfg = "{type:'line',data:{labels:[" + labels + "],datasets:[{label:'Flow',"
                 "data:[" + dp + "],"
                 "borderColor:'#2196F3',backgroundColor:'rgba(33,150,243,0.3)',"
                 "pointRadius:3,fill:true}]},"
                 "options:{scales:{x:{title:{display:true,text:'Fecha/Hora'}},"
                 "y:{title:{display:true,text:'L/min'}}}}}";

    String url = "https://quickchart.io/chart?bkg=%23111&w=600&h=400&c=" + urlEncode(cfg);
    Serial.println("buildChart: url_len=" + String(url.length()));
    return url;
}

void checkTelegramCommands() {
    if (WiFi.status() != WL_CONNECTED) return;

    // Per-update: track sender + command
    struct TgCmd {
        String senderId;
        String command;  // "graph", "status", "help", "csv", "mute", "unmute"
        String arg;
    };
    const int MAX_CMDS = 10;
    TgCmd cmds[MAX_CMDS];
    int cmdCount = 0;

    {
        WiFiClientSecure client;
        client.setInsecure();
        HTTPClient http;

        String url = "https://api.telegram.org/bot" + botToken
                     + "/getUpdates?timeout=0&limit=5&offset=" + String(lastTgUpdateId + 1);
        http.begin(client, url);
        http.setTimeout(5000);
        int code = http.GET();
        if (code != 200) { http.end(); return; }

        String body = http.getString();
        http.end();

        int searchFrom = 0;
        while (true) {
            int uidPos = body.indexOf("\"update_id\":", searchFrom);
            if (uidPos < 0) break;

            int numStart = uidPos + 12;
            int numEnd = body.indexOf(',', numStart);
            if (numEnd < 0) numEnd = body.indexOf('}', numStart);
            long uid = body.substring(numStart, numEnd).toInt();
            if (uid > lastTgUpdateId) { lastTgUpdateId = uid; saveLastUpdateId(); }

            int nextUid = body.indexOf("\"update_id\":", numEnd);
            int blockEnd = (nextUid > 0) ? nextUid : body.length();
            String block = body.substring(uidPos, blockEnd);

            // Extract sender chat_id from "chat":{"id":NNN
            String senderId = "";
            int chatPos = block.indexOf("\"chat\":{\"id\":");
            if (chatPos >= 0) {
                int idStart = chatPos + 13;
                int idEnd = block.indexOf(',', idStart);
                if (idEnd < 0) idEnd = block.indexOf('}', idStart);
                senderId = block.substring(idStart, idEnd);
                senderId.trim();
            }

            // Extract text
            String text = "";
            int textPos = block.indexOf("\"text\":\"", 0);
            if (textPos >= 0) {
                int textStart = textPos + 8;
                int textEnd = block.indexOf("\"", textStart);
                text = block.substring(textStart, textEnd);
            }

            // Parse command and arg
            if (cmdCount < MAX_CMDS && senderId.length() > 0) {
                String cmd = "";
                String arg = "";
                int spacePos = text.indexOf(' ');
                if (spacePos >= 0) { arg = text.substring(spacePos + 1); arg.trim(); }

                if (text.startsWith("/unmute"))       cmd = "unmute";
                else if (text.startsWith("/digest"))    cmd = "digest";
                else if (text.startsWith("/mute"))      cmd = "mute";
                else if (text.startsWith("/graph"))     cmd = "graph";
                else if (text.startsWith("/status"))    cmd = "status";
                else if (text.startsWith("/csv"))       cmd = "csv";
                else if (text.startsWith("/files"))     cmd = "files";
                else if (text.startsWith("/addUser"))   cmd = "addUser";
                else if (text.startsWith("/removeUser")) cmd = "removeUser";
                else if (text.startsWith("/listUsers")) cmd = "listUsers";
                else if (text.startsWith("/clear"))    cmd = "clear";
                else if (text.startsWith("/wipe"))     cmd = "wipe";
                else if (text.startsWith("/help") || text.startsWith("/start")) cmd = "help";

                if (cmd.length() > 0) {
                    cmds[cmdCount].senderId = senderId;
                    cmds[cmdCount].command = cmd;
                    cmds[cmdCount].arg = arg;
                    cmdCount++;
                }
            }

            searchFrom = numEnd + 1;
        }
    }

    // Process commands — respond only to whitelisted users
    for (int c = 0; c < cmdCount; c++) {
        String sid = cmds[c].senderId;
        if (!isUserAllowed(sid)) {
            Serial.println("Unauthorized user: " + sid);
            continue;
        }
        String cmd = cmds[c].command;
        String arg = cmds[c].arg;

        if (cmd == "graph") {
            String csvPath, caption;
            if (arg.length() > 0 && arg.toInt() > 0) {
                csvPath = "/archive_" + arg + ".csv";
                caption = "Filter Health - Archivo " + arg;
            } else {
                csvPath = "/current.csv";
                caption = "Filter Health - Actual";
            }
            String chartUrl = buildChartUrl(csvPath);
            if (chartUrl.length() > 0) {
                sendTelegramPhotoTo(sid, chartUrl, caption);
            } else {
                sendTelegramTo(sid, "No hay datos para graficar");
            }
        }

        if (cmd == "status") {
            String msg = "<b>Status</b>\n";
            msg += flowing ? "&#128167; Flowing: " + String(flowRate, 1) + " L/min\n" : "IDLE\n";
            msg += "Total: " + String(totalLiters, 1) + " L\n";
            msg += "Corridas: " + String(historyCount) + "\n";
            msg += "Archivos: " + String(archiveCount) + "\n";
            if (historyCount > 0 && fsAvailable && LittleFS.exists("/current.csv")) {
                File f = LittleFS.open("/current.csv", "r");
                if (f) {
                    const int BUF = 5;
                    float maxVals[BUF];
                    float volVals[BUF];
                    int total = 0;
                    while (f.available()) {
                        String line = f.readStringUntil('\n');
                        line.trim();
                        if (line.length() == 0) continue;
                        int c1 = line.indexOf(',');
                        if (c1 < 0) continue;
                        // Detect new format: first field is datetime (contains '-')
                        String dataFields = line;
                        if (line.substring(0, c1).indexOf('-') >= 0) {
                            dataFields = line.substring(c1 + 1);
                        }
                        int d1 = dataFields.indexOf(',');
                        if (d1 < 0) continue;
                        int idx = total % BUF;
                        maxVals[idx] = dataFields.substring(0, d1).toFloat();
                        int d2 = dataFields.indexOf(',', d1 + 1);
                        volVals[idx] = (d2 > 0) ? dataFields.substring(d1 + 1, d2).toFloat() : dataFields.substring(d1 + 1).toFloat();
                        total++;
                    }
                    f.close();
                    int show = total < BUF ? total : BUF;
                    msg += "\n<b>Ultimas " + String(show) + ":</b>\n";
                    for (int i = show - 1; i >= 0; i--) {
                        int idx = (total - show + i) % BUF;
                        int num = total - (show - 1 - i);
                        msg += String(num) + ". " + String(maxVals[idx], 2) + " L/min  " + String(volVals[idx], 2) + " L\n";
                    }
                }
            }
            msg += "\nHeap: " + String(ESP.getFreeHeap() / 1024) + "KB";
            msg += "\nFlash: " + String(LittleFS.usedBytes() / 1024) + "/" + String(LittleFS.totalBytes() / 1024) + "KB";
            msg += "\nIP: " + WiFi.localIP().toString();
            msg += "\n" + notifyModeLabel(getUserNotifyMode(sid));
            sendTelegramTo(sid, msg);
        }

        if (cmd == "mute") {
            setUserNotifyMode(sid, 1);
            saveUserData();
            sendTelegramTo(sid, "&#128263; Notificaciones silenciadas. Solo respondere a comandos.\n/unmute o /digest para cambiar.");
        }

        if (cmd == "unmute") {
            setUserNotifyMode(sid, 0);
            saveUserData();
            sendTelegramTo(sid, "&#128264; Notificaciones activadas. Recibiras mensajes automaticos.");
        }

        if (cmd == "digest") {
            setUserNotifyMode(sid, 2);
            String* dbuf = getUserDigestBuf(sid);
            if (dbuf) *dbuf = "";
            saveUserData();
            sendTelegramTo(sid, "&#128340; Modo resumen activado. Recibiras un resumen cada 3 horas.\n/unmute o /mute para cambiar.");
        }

        if (cmd == "addUser" && isAdmin(sid)) {
            if (arg.length() == 0) {
                sendTelegramTo(sid, "Usage: /addUser [chat_id] [name]");
            } else {
                int space = arg.indexOf(' ');
                String newId = (space > 0) ? arg.substring(0, space) : arg;
                String newName = (space > 0) ? arg.substring(space + 1) : "";
                newName.trim();
                if (isAdmin(newId)) {
                    sendTelegramTo(sid, "Ya es admin.");
                } else if (addDynamicUser(newId, newName)) {
                    saveUserData();
                    String reply = "Usuario " + newId;
                    if (newName.length() > 0) reply += " (" + newName + ")";
                    reply += " agregado.";
                    sendTelegramTo(sid, reply);
                } else if (numDynamicUsers >= MAX_DYNAMIC_USERS) {
                    sendTelegramTo(sid, "Lista llena (max " + String(MAX_DYNAMIC_USERS) + ").");
                } else {
                    sendTelegramTo(sid, "Usuario " + newId + " ya esta autorizado.");
                }
            }
        }

        if (cmd == "removeUser" && isAdmin(sid)) {
            if (arg.length() == 0) {
                sendTelegramTo(sid, "Usage: /removeUser [chat_id]");
            } else if (isAdmin(arg)) {
                sendTelegramTo(sid, "No se puede eliminar un admin.");
            } else if (removeDynamicUser(arg)) {
                saveUserData();
                sendTelegramTo(sid, "Usuario " + arg + " eliminado.");
            } else {
                sendTelegramTo(sid, "Usuario " + arg + " no encontrado en lista dinamica.");
            }
        }

        if (cmd == "listUsers" && isAdmin(sid)) {
            String msg = "<b>Admins:</b>\n";
            for (int j = 0; j < NUM_ADMIN_USERS; j++) {
                msg += "  " + ADMIN_NAMES[j] + " (" + ADMIN_USERS[j] + ") " + notifyModeLabel(adminNotifyMode[j]) + "\n";
            }
            msg += "\n<b>Preset:</b>\n";
            for (int j = 0; j < NUM_PRESET_USERS; j++) {
                msg += "  " + PRESET_NAMES[j] + " (" + PRESET_USERS[j] + ") " + notifyModeLabel(presetNotifyMode[j]) + "\n";
            }
            if (numDynamicUsers > 0) {
                msg += "\n<b>Agregados:</b>\n";
                for (int j = 0; j < numDynamicUsers; j++) {
                    String name = dynamicUserNames[j].length() > 0 ? dynamicUserNames[j] : dynamicUsers[j];
                    msg += "  " + name + " (" + dynamicUsers[j] + ") " + notifyModeLabel(dynamicNotifyMode[j]) + "\n";
                }
            } else {
                msg += "\nSin usuarios agregados.";
            }
            sendTelegramTo(sid, msg);
        }

        if (cmd == "clear" && isAdmin(sid)) {
            if (historyCount == 0) {
                sendTelegramTo(sid, "No hay historial para guardar.");
            } else {
                archiveCount++;
                String src = "/current.csv";
                String dst = "/archive_" + String(archiveCount) + ".csv";
                if (fsAvailable) {
                    if (!LittleFS.rename(src, dst)) {
                        File fin = LittleFS.open(src, "r");
                        File fout = LittleFS.open(dst, "w");
                        if (fin && fout) {
                            fout.print("corrida,max_Lmin,volumen_L\n");
                            int row = 1;
                            while (fin.available()) {
                                String line = fin.readStringUntil('\n');
                                line.trim();
                                if (line.length() == 0) continue;
                                fout.print(row++); fout.print(","); fout.println(line);
                            }
                        }
                        if (fin)  fin.close();
                        if (fout) fout.close();
                        LittleFS.remove(src);
                    }
                    File fc = LittleFS.open("/archivecount.txt", "w");
                    if (fc) { fc.print(archiveCount); fc.close(); }
                }
                String ip = WiFi.localIP().toString();
                sendTelegramTo(sid, "<b>Historial guardado</b> (" + String(historyCount) + " corridas)\n"
                    "<a href=\"http://" + ip + "/archive?n=" + String(archiveCount) + "\">" + ip + "/archive?n=" + String(archiveCount) + "</a>");
                historyCount = 0;
            }
        }

        if (cmd == "wipe" && isAdmin(sid)) {
            sendTelegramTo(sid, "&#9888; Flash borrado. Reiniciando...");
            // Save the advanced update ID before format so after reboot it starts past this command
            lastTgUpdateId++;
            // format() wipes the file, so we write it to NVS-style workaround: just advance the ID.
            // The format will clear lastupdateid.txt, but we persist it BEFORE formatting:
            saveLastUpdateId();
            delay(500);
            LittleFS.format();
            // Re-save after format (file was wiped, write it fresh):
            File fid = LittleFS.open("/lastupdateid.txt", "w");
            if (fid) { fid.print(lastTgUpdateId); fid.close(); }
            ESP.restart();
        }

        if (cmd == "help") {
            String msg = "<b>Comandos disponibles</b>\n\n";
            msg += "/status - Estado actual del filtro\n";
            msg += "/graph - Grafico de corridas actuales\n";
            msg += "/graph N - Grafico del archivo N\n";
            msg += "/csv - Descargar CSV actual\n";
            msg += "/csv N - Descargar archivo N\n";
            msg += "/files - Listar archivos en flash\n";
            msg += "/mute - Silenciar notificaciones\n";
            msg += "/unmute - Recibir todo en tiempo real\n";
            msg += "/digest - Resumen cada 3 horas\n";
            msg += "/help - Mostrar esta ayuda\n";
            if (isAdmin(sid)) {
                msg += "\n<b>Admin:</b>\n";
                msg += "/addUser [chat_id] [nombre] - Autorizar usuario\n";
                msg += "/removeUser [chat_id] - Eliminar usuario\n";
                msg += "/listUsers - Listar usuarios autorizados\n";
                msg += "/clear - Guardar y borrar historial\n";
                msg += "/wipe - Borrar TODO el flash y reiniciar\n";
            }
            msg += "\nEstado: " + notifyModeLabel(getUserNotifyMode(sid));
            sendTelegramTo(sid, msg);
        }

        if (cmd == "csv") {
            if (arg.length() > 0 && arg.toInt() > 0) {
                int n = arg.toInt();
                if (n <= archiveCount) {
                    String path = "/archive_" + arg + ".csv";
                    sendTelegramDocumentTo(sid, path, "Archivo " + arg);
                } else {
                    sendTelegramTo(sid, "Archivo " + arg + " no existe");
                }
            } else {
                // No arg: send current.csv + list archives
                if (historyCount > 0 && fsAvailable && LittleFS.exists("/current.csv")) {
                    sendTelegramDocumentTo(sid, "/current.csv", "Actual (" + String(historyCount) + " corridas)");
                } else {
                    sendTelegramTo(sid, "Sin datos actuales.");
                }
                if (archiveCount > 0) {
                    String msg = "<b>Archivos guardados:</b> " + String(archiveCount) + "\n";
                    msg += "Usa /csv N para descargar (ej: /csv 1)";
                    sendTelegramTo(sid, msg);
                }
            }
        }

        if (cmd == "files") {
            if (!fsAvailable) {
                sendTelegramTo(sid, "Flash no disponible.");
            } else {
                String msg = "<b>Archivos en flash</b>\n\n";
                File root = LittleFS.open("/");
                File file = root.openNextFile();
                int count = 0;
                size_t totalSize = 0;
                while (file) {
                    size_t sz = file.size();
                    totalSize += sz;
                    msg += String(file.name()) + "  (" + String(sz) + " B)\n";
                    count++;
                    file = root.openNextFile();
                }
                msg += "\n" + String(count) + " archivos, " + String(totalSize / 1024) + " KB total";
                msg += "\nFlash libre: " + String(LittleFS.totalBytes() / 1024 - LittleFS.usedBytes() / 1024) + " KB";
                sendTelegramTo(sid, msg);
            }
        }
    }
}

void setup() {
    Serial.begin(115200);
    delay(500);
    Serial.println("=== BOOT ===");

    setupBuzzer();

    WiFi.mode(WIFI_STA);
    WiFi.setAutoReconnect(true);
    Serial.println("Connecting WiFi...");
    connectToWiFi();
    Serial.println("NTP...");
    setupNTP();

    Serial.println("LittleFS...");
    if (!LittleFS.begin(true)) {  // true = format on fail
        Serial.println("LittleFS mount failed");
    }
    fsAvailable = true;
    Serial.printf("LittleFS: %u used / %u total bytes (%.1f%% free)\n",
                  LittleFS.usedBytes(), LittleFS.totalBytes(),
                  100.0 * (1.0 - (float)LittleFS.usedBytes() / LittleFS.totalBytes()));
    File fc = LittleFS.open("/archivecount.txt", "r");
    if (fc) { archiveCount = fc.readString().toInt(); fc.close(); }
    loadTotalLiters();
    loadLastUpdateId();
    countHistoryFromFlash();
    Serial.println("Loading users...");
    // Initialize default notify modes, then load saved prefs
    for (int i = 0; i < NUM_ADMIN_USERS; i++) { adminNotifyMode[i] = 0; adminDigestBuf[i] = ""; }
    for (int i = 0; i < NUM_PRESET_USERS; i++) { presetNotifyMode[i] = 0; presetDigestBuf[i] = ""; }
    for (int i = 0; i < MAX_DYNAMIC_USERS; i++) { dynamicNotifyMode[i] = 1; dynamicDigestBuf[i] = ""; }
    loadUserData();
    Serial.println("Users loaded. Dynamic: " + String(numDynamicUsers));

    Serial.println("Starting web server...");
    webServer.on("/", handleRoot);
    webServer.on("/clear-confirm", HTTP_GET, handleClearConfirm);
    webServer.on("/clear", HTTP_POST, handleClear);
    webServer.on("/archive", handleArchive);
    webServer.on("/archive-dl", handleArchiveDownload);
    webServer.on("/data.csv", handleDataCSV);
    webServer.on("/api/status", handleApiStatus);
    webServer.on("/chart", handleChart);
    webServer.on("/send-chart", handleSendChart);
    webServer.begin();
    Serial.println("Web server started on " + WiFi.localIP().toString());

    pinMode(FLOW_PIN, INPUT_PULLUP);
    attachInterrupt(digitalPinToInterrupt(FLOW_PIN), pulseISR, FALLING);

    unsigned long now = millis();
    lastCalcTime             = now;
    lastPulseTime            = now;
    lastWiFiReconnectAttempt = now;

    Serial.println("Heap after setup: " + String(ESP.getFreeHeap() / 1024) + "KB");
    pendingTelegramMsg = "Monitor de flujo online (ESP32)\n" + WiFi.localIP().toString();
    loopStartTime = millis();
    lastDigestFlush = millis();
    Serial.println("=== READY ===");
}

void loop() {
    unsigned long now = millis();

    webServer.handleClient();

    // --- Pending Telegram (auto-msg, respects per-user mute) ---
    if (pendingTelegramMsg.length() > 0 && (now - loopStartTime > 5000)) {
        sendTelegramAutoMsg(pendingTelegramMsg);
        pendingTelegramMsg = "";
    }

    // --- Check Telegram commands (every 10s, only when idle) ---
    if (!flowing && (now - lastTgCheck >= TG_CHECK_INTERVAL) && (now - loopStartTime > 10000)) {
        lastTgCheck = now;
        checkTelegramCommands();
    }

    // --- Flush digest buffers every 3 hours ---
    if (now - lastDigestFlush >= DIGEST_INTERVAL) {
        lastDigestFlush = now;
        flushDigestBuffers();
    }

    // --- WiFi reconnect if dropped ---
    if (WiFi.status() != WL_CONNECTED) {
        if (wifiFailingSince == 0) wifiFailingSince = now;
        if (now - lastWiFiReconnectAttempt >= WIFI_RECONNECT_INTERVAL) {
            lastWiFiReconnectAttempt = now;
            Serial.println("WiFi lost, calling WiFi.begin()...");
            WiFi.begin(ssid, password);
        }
        if (now - wifiFailingSince >= WIFI_RESET_TIMEOUT) {
            Serial.println("WiFi failed for 2 minutes. Restarting...");
            beep(1000);
            ESP.restart();
        }
    } else {
        wifiFailingSince = 0;
        lastWiFiReconnectAttempt = 0;
    }

    // --- Recalculate every second ---
    if (now - lastCalcTime >= CALC_INTERVAL) {
        uint32_t pulses;
        noInterrupts();
        pulses = pulseCount;
        pulseCount = 0;
        interrupts();

        float elapsed   = (now - lastCalcTime) / 1000.0;
        float litersNow = pulses / CALIBRATION_FACTOR;
        flowRate        = litersNow / elapsed * 60.0;  // L/min

        if (pulses > 0) {
            Serial.println("Pulses: " + String(pulses) + " flow: " + String(flowRate, 2) + " L/min");
        }

        if (pulses >= 3 && flowRate >= MIN_FLOW_RATE) {
            lastPulseTime = now;
            totalLiters  += litersNow;
            if (!flowing) {
                flowing      = true;
                runStartTime = now;
            }
            sessionLiters += litersNow;
            if (flowRate > runMax) runMax = flowRate;
        }

        lastCalcTime = now;
    }

    // --- 3s silence -> run ended ---
    if (flowing && (now - lastPulseTime >= FLOW_TIMEOUT)) {
        flowing  = false;
        flowRate = 0.0;

        lastRunMax    = runMax;
        lastRunLiters = sessionLiters;

        appendRunToFlash(runMax, sessionLiters);
        historyCount++;

        pendingTelegramMsg = "<b>IDLE</b>\n"
                     "Maximo: "  + String(runMax, 2)        + " L/min\n"
                     "Volumen: " + String(sessionLiters, 3) + " L";

        runMax        = 0.0;
        sessionLiters = 0.0;
    }

    delay(1);
}
