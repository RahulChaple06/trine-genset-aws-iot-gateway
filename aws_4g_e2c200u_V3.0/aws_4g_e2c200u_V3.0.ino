/*
   ESP32-WROOM-32D + Quectel EC200U + DSE4520
   4G + GNSS + CAN (TWAI) + MQTT (AWS IoT Core over TLS)
   ---------------------------------------------------
   ✅ PUBLIC / GENERIC TEMPLATE (NO REAL CREDENTIALS)

   - Put your real AWS endpoint/topic/deviceId and certificates in secrets.h (NOT committed)
   - This file remains safe to publish for study/learning.
*/

#include <Arduino.h>
#include "driver/twai.h"   // CAN / TWAI driver (for DSE4520)

// ======================================================
//                OPTIONAL SECRETS INCLUDE
// ======================================================
/*
  Create a local file named: secrets.h (do NOT commit)
  It should define:
    AWS_HOST, AWS_CLIENT_ID, AWS_TOPIC, DEVICE_ID
    AWS_ROOT_CA, AWS_CLIENT_CERT, AWS_CLIENT_KEY
*/
#if __has_include("secrets.h")
  #include "secrets.h"
#else
  // ----------- SAFE PLACEHOLDERS (PUBLIC) -----------
  static const char* AWS_HOST     = "YOUR_AWS_IOT_ENDPOINT-ats.iot.YOUR_REGION.amazonaws.com";
  static const char* AWS_CLIENT_ID= "YOUR_THING_NAME";
  static const char* AWS_TOPIC    = "YOUR/TELEMETRY/TOPIC";
  static const char* DEVICE_ID    = "YOUR_DEVICE_ID";

  // NOTE: Replace with Amazon Root CA 1 + your device cert/key in secrets.h
  const char* AWS_ROOT_CA = R"EOF(
-----BEGIN CERTIFICATE-----
YOUR_AWS_ROOT_CA_HERE
-----END CERTIFICATE-----
)EOF";

  const char* AWS_CLIENT_CERT = R"KEY(
-----BEGIN CERTIFICATE-----
YOUR_DEVICE_CERTIFICATE_HERE
-----END CERTIFICATE-----
)KEY";

  const char* AWS_CLIENT_KEY = R"KEY(
-----BEGIN RSA PRIVATE KEY-----
YOUR_PRIVATE_KEY_HERE
-----END RSA PRIVATE KEY-----
)KEY";
#endif

// ======================================================
//                     PIN MAP
// ======================================================

// ---------- UART between ESP32 and EC200U ----------
HardwareSerial Modem(1);          // UART1
#define MODEM_RX   21             // EC200U TX -> ESP32 RX1
#define MODEM_TX   22             // EC200U RX <- ESP32 TX1

// ---------- EC200U control pins ----------
#define MODEM_PWRKEY  4           // PWRKEY control (active LOW)
#define MODEM_RST     2           // Hardware reset (active LOW)

// ---------- Tower status inputs ----------
#define LED1_PIN 32   // INPUT_PULLDOWN
#define LED2_PIN 33   // INPUT_PULLDOWN
#define LED3_PIN 35   // input-only
#define LED4_PIN 34   // input-only

// ---------- RYG traffic light module ----------
#define R_LED_PIN 25
#define Y_LED_PIN 26
#define G_LED_PIN 27

// ======================================================
//                CAN / TWAI CONFIG
// ======================================================
static const gpio_num_t CAN_TX_PIN = (gpio_num_t)17;
static const gpio_num_t CAN_RX_PIN = (gpio_num_t)16;

// J1939 PGNs (examples for DSE streams)
static const uint32_t PGN_VEP1  = 0x00FEF7;   // Battery Voltage
static const uint32_t PGN_GAAC  = 0x00FE06;   // AC V-LN, Frequency, Current
static const uint32_t PGN_GTACP = 0x00FE05;   // Real / Apparent Power
static const uint32_t PGN_GTE   = 0x00FDFA;   // Energy Export

static const uint32_t CAN_ID_FUEL_CONFIG = 0x18FF50E5;

// ======================================================
//               HEALTH / LED LOGIC
// ======================================================
unsigned long lastCanRxMs = 0;
const unsigned long CAN_OK_TIMEOUT_MS = 2000;
const float BAT_OK_THRESHOLD = 11.0f;

unsigned long lastGreenToggleMs = 0;
bool greenOn = false;
const unsigned long GREEN_BLINK_INTERVAL_MS = 500;

// ======================================================
//               MODEM / LTE / MQTT FLAGS
// ======================================================
bool gSimReady        = false;
bool gLteRegistered   = false;
bool gLteSignalOk     = false;
bool gLastPublishOk   = false;

bool gModemPowered = false;
bool mqttReady = false;

// DG ON  => telemetry 1 min
// DG OFF => telemetry 15 min + power-down between shots
const float DG_ON_VOLT_THRESHOLD = 150.0f;
const unsigned long DG_ON_TELEMETRY_MS  = 1UL  * 60UL * 1000UL;
const unsigned long DG_OFF_TELEMETRY_MS = 15UL * 60UL * 1000UL;

bool    gDgOnPrev        = false;
bool    gDgOnState       = false;
uint8_t gDgOnStreak      = 0;
uint8_t gDgOffStreak     = 0;
const uint8_t DG_STREAK_THRESHOLD = 5;

// ======================================================
//               AWS MQTT (EC200U side)
// ======================================================
static const int AWS_PORT = 8883;
static const int MQTT_CLIENT_IDX = 0;
static const int SSL_CTX_ID      = 1;
static const int PDP_CID         = 1;

// MQTT reconnect throttling (optional)
unsigned long lastMqttRetryMs = 0;
const unsigned long MQTT_RETRY_INTERVAL_MS = 30000;

// ======================================================
//               TELEMETRY SCHEDULING
// ======================================================
unsigned long lastTelemetry = 0;

// ======================================================
//                    LIVE VALUES
// ======================================================
float gBatVolts      = 0.0f;
float gFuelPercent   = 0.0f;
float gAvgLNVolts    = 0.0f;
float gFrequencyHz   = 0.0f;
float gAvgCurrentA   = 0.0f;
float gActivePowerKW = 0.0f;
float gEnergyKWh     = 0.0f;

bool  gOilPressureActive = false;
bool  gOilTempActive     = false;

unsigned long lastBatMs        = 0;
unsigned long lastFuelMs       = 0;
unsigned long lastAcMs         = 0;
unsigned long lastPowerMs      = 0;
unsigned long lastEnergyMs     = 0;
unsigned long lastOilPressMs   = 0;
unsigned long lastOilTempMs    = 0;

bool gCanDataReady = false;

unsigned long lastPrintMs  = 0;
const unsigned long PRINT_INTERVAL_MS = 2000;
const unsigned long VALUE_TIMEOUT_MS  = 2000;

// ======================================================
//                    HELPERS
// ======================================================
uint32_t pgnFromId(uint32_t id) {
  uint8_t pf = (id >> 16) & 0xFF;
  uint8_t ps = (id >> 8)  & 0xFF;
  if (pf < 0xF0) return (uint32_t)pf << 8;
  return ((uint32_t)pf << 8) | ps;
}
static inline uint16_t le16(const uint8_t *d, uint8_t idx) {
  return (uint16_t)d[idx] | ((uint16_t)d[idx + 1] << 8);
}
static inline uint32_t le32u(const uint8_t *d, uint8_t idx) {
  return  (uint32_t)d[idx]
        | ((uint32_t)d[idx + 1] << 8)
        | ((uint32_t)d[idx + 2] << 16)
        | ((uint32_t)d[idx + 3] << 24);
}
static inline int32_t le32s(const uint8_t *d, uint8_t idx) {
  return (int32_t)le32u(d, idx);
}

float safeValue(float v, unsigned long lastMs, unsigned long now,
                unsigned long timeoutMs = VALUE_TIMEOUT_MS) {
  if (lastMs == 0) return 0.0f;
  if (now - lastMs > timeoutMs) return 0.0f;
  return v;
}
bool safeFlag(bool flag, unsigned long lastMs, unsigned long now,
              unsigned long timeoutMs = VALUE_TIMEOUT_MS) {
  if (lastMs == 0) return false;
  if (now - lastMs > timeoutMs) return false;
  return flag;
}

// ======================================================
//       DG ON/OFF decision from multiple CAN readings
// ======================================================
void updateDgStateFromSample() {
  if (lastAcMs == 0 && lastPowerMs == 0) return;

  bool sampleOn = false;

  if (lastAcMs > 0) {
    if (gAvgLNVolts > DG_ON_VOLT_THRESHOLD) sampleOn = true;
    if (gFrequencyHz > 45.0f && gFrequencyHz < 65.0f) sampleOn = true;
    if (gAvgCurrentA > 5.0f) sampleOn = true;
  }
  if (lastPowerMs > 0 && gActivePowerKW > 0.5f) sampleOn = true;

  if (sampleOn) {
    if (gDgOnStreak < 255) gDgOnStreak++;
    gDgOffStreak = 0;
  } else {
    if (gDgOffStreak < 255) gDgOffStreak++;
    gDgOnStreak = 0;
  }

  if (!gDgOnState && gDgOnStreak >= DG_STREAK_THRESHOLD) gDgOnState = true;
  else if (gDgOnState && gDgOffStreak >= DG_STREAK_THRESHOLD) gDgOnState = false;
}

// ======================================================
//                GNSS placeholders (kept generic)
// ======================================================
int    gLastGpsSats = -1;
String gLastGpsLat;
String gLastGpsLon;
String gLastGpsTs;
bool   gHasGpsFix   = false;

int gCurrentGpsSats = -1;
unsigned long lastGpsUpdateMs = 0;

// ======================================================
//                     SNAPSHOT
// ======================================================
void printSnapshot() {
  unsigned long now = millis();

  float batV   = safeValue(gBatVolts,      lastBatMs,      now);
  float fuel   = safeValue(gFuelPercent,   lastFuelMs,     now);
  float vLN    = safeValue(gAvgLNVolts,    lastAcMs,       now);
  float freq   = safeValue(gFrequencyHz,   lastAcMs,       now);
  float iAvg   = safeValue(gAvgCurrentA,   lastAcMs,       now);
  float pKW    = safeValue(gActivePowerKW, lastPowerMs,    now);
  float eKWh   = safeValue(gEnergyKWh,     lastEnergyMs,   now);

  bool oilPress = safeFlag(gOilPressureActive, lastOilPressMs, now);
  bool oilTemp  = safeFlag(gOilTempActive,     lastOilTempMs,  now);

  int led1 = digitalRead(LED1_PIN);
  int led2 = digitalRead(LED2_PIN);
  int led3 = digitalRead(LED3_PIN);
  int led4 = digitalRead(LED4_PIN);

  Serial.println(F("---------- DSE4520 Snapshot ----------"));
  Serial.print (F("Battery Voltage (V): "));   Serial.println(batV, 2);
  Serial.print (F("Fuel Level (%):      "));   Serial.println(fuel, 1);
  Serial.print (F("L-N Voltage (V):     "));   Serial.println(vLN, 1);
  Serial.print (F("Frequency (Hz):      "));   Serial.println(freq, 2);
  Serial.print (F("Average Current (A): "));   Serial.println(iAvg, 1);
  Serial.print (F("Active Power (kW):   "));   Serial.println(pKW, 2);
  Serial.print (F("Energy Export (kWh): "));   Serial.println(eKWh, 3);

  Serial.print (F("Oil Pressure Switch: "));
  Serial.println(oilPress ? F("Check") : F("Good"));

  Serial.print (F("Oil Temp Switch:     "));
  Serial.println(oilTemp ? F("Check") : F("Good"));

  Serial.print (F("LED_1: ")); Serial.print(led1);
  Serial.print (F(" | LED_2: ")); Serial.print(led2);
  Serial.print (F(" | LED_3: ")); Serial.print(led3);
  Serial.print (F(" | LED_4: ")); Serial.println(led4);

  Serial.print(F("SIM: "));  Serial.println(gSimReady ? F("READY") : F("NOT READY"));
  Serial.print(F("LTE: "));  Serial.println(gLteRegistered ? F("REGISTERED") : F("NOT REGISTERED"));
  Serial.print(F("MQTT: "));
  if (!mqttReady) Serial.println(F("NOT READY"));
  else if (gLastPublishOk) Serial.println(F("READY (last publish OK)"));
  else Serial.println(F("READY (last publish FAILED)"));

  Serial.println();
}

// ======================================================
//                 MODEM AT HELPERS
// ======================================================
bool modemReadLine(String &out, uint32_t timeout = 1000) {
  out = "";
  uint32_t start = millis();
  while ((millis() - start) < timeout) {
    while (Modem.available()) {
      char c = (char)Modem.read();
      if (c == '\r') continue;
      if (c == '\n') {
        if (out.length() > 0) return true;
        else continue;
      }
      out += c;
    }
    delay(1);
  }
  return (out.length() > 0);
}

bool sendAT(const char *cmd,
            const char *expect1 = "OK",
            const char *expect2 = nullptr,
            uint32_t timeout = 1000) {
  Serial.print(">> "); Serial.println(cmd);
  while (Modem.available()) Modem.read();

  Modem.print(cmd);
  Modem.print("\r");

  uint32_t start = millis();
  String line;
  while ((millis() - start) < timeout) {
    if (!modemReadLine(line, timeout - (millis() - start))) continue;
    Serial.print("<< "); Serial.println(line);

    if (line.indexOf("ERROR") >= 0) return false;
    if (expect1 && line.indexOf(expect1) >= 0) return true;
    if (expect2 && line.indexOf(expect2) >= 0) return true;
  }
  return false;
}

bool sendATGetLine(const char *cmd,
                   const char *prefix,
                   String &result,
                   uint32_t timeout = 1000) {
  Serial.print(">> "); Serial.println(cmd);
  while (Modem.available()) Modem.read();

  Modem.print(cmd);
  Modem.print("\r");

  uint32_t start = millis();
  String line;
  while ((millis() - start) < timeout) {
    if (!modemReadLine(line, timeout - (millis() - start))) continue;
    Serial.print("<< "); Serial.println(line);

    if (line.indexOf("ERROR") >= 0) return false;
    if (line.startsWith(prefix)) { result = line; return true; }
  }
  return false;
}

bool waitForURC(const char *prefix, String &out, uint32_t timeout) {
  uint32_t start = millis();
  out = "";
  while ((millis() - start) < timeout) {
    String line;
    if (!modemReadLine(line, timeout - (millis() - start))) continue;
    Serial.print("[URC] "); Serial.println(line);

    if (line.indexOf("ERROR") >= 0) return false;
    if (line.indexOf(prefix) >= 0) { out = line; return true; }
  }
  return false;
}

bool waitForPrompt(char prompt, uint32_t timeoutMs) {
  uint32_t start = millis();
  while ((millis() - start) < timeoutMs) {
    while (Modem.available()) {
      char c = (char)Modem.read();
      Serial.write(c);
      if (c == prompt) { Serial.println(); return true; }
    }
    delay(1);
  }
  return false;
}

bool waitForQMTOPEN_OK(String &out, uint32_t timeoutMs) {
  uint32_t start = millis();
  out = "";
  while ((millis() - start) < timeoutMs) {
    String line;
    if (!modemReadLine(line, timeoutMs - (millis() - start))) continue;

    Serial.print("[URC] "); Serial.println(line);
    if (line.indexOf("ERROR") >= 0) { out = line; return false; }
    if (line.indexOf("+QMTOPEN:") < 0) continue;

    out = line;
    if (line.indexOf("+QMTOPEN: 0,0") >= 0) return true;
    return false;
  }
  return false;
}

// ======================================================
//          EC200U CONTROL (PWRKEY / RST)
// ======================================================
void modemPowerOnPulse() {
  digitalWrite(MODEM_PWRKEY, LOW);
  delay(2200);
  digitalWrite(MODEM_PWRKEY, HIGH);
}
void modemPowerOffPulse() {
  digitalWrite(MODEM_PWRKEY, LOW);
  delay(3200);
  digitalWrite(MODEM_PWRKEY, HIGH);
}
void modemHardwareReset() {
  digitalWrite(MODEM_RST, LOW);
  delay(150);
  digitalWrite(MODEM_RST, HIGH);
}

// ======================================================
//                MODEM BRING-UP
// ======================================================
bool waitForSIMReady() {
  gSimReady = false;
  for (int i = 0; i < 10; ++i) {
    if (sendAT("AT+CPIN?", "READY", nullptr, 2000)) {
      gSimReady = true;
      return true;
    }
    delay(1000);
  }
  return false;
}

bool waitForLTERegistered(uint32_t timeout) {
  gLteRegistered = false;
  uint32_t start = millis();
  while ((millis() - start) < timeout) {
    String line;
    if (sendATGetLine("AT+CEREG?", "+CEREG:", line, 2000)) {
      int firstComma = line.indexOf(',');
      if (firstComma >= 0) {
        int secondComma = line.indexOf(',', firstComma + 1);
        String statStr = (secondComma > 0)
          ? line.substring(firstComma + 1, secondComma)
          : line.substring(firstComma + 1);
        statStr.trim();
        int stat = statStr.toInt();
        if (stat == 1 || stat == 5) { gLteRegistered = true; return true; }
      }
    }
    delay(2000);
  }
  return false;
}

bool initModem() {
  if (!sendAT("AT", "OK", nullptr, 2000)) { gModemPowered = false; return false; }
  sendAT("ATE0", "OK", nullptr, 2000);
  sendAT("AT+CMEE=2", "OK", nullptr, 2000);
  if (!waitForSIMReady()) return false;
  if (!waitForLTERegistered(60000UL)) return false;
  sendAT("AT+QNWINFO", "OK", nullptr, 5000);
  return true;
}

// ======================================================
//             PDP CONTEXT (APN-agnostic)
// ======================================================
static const char* PDP_APN = ""; // empty => SIM/operator default

bool setupPDP() {
  sendAT("AT+QIDEACT=1", "OK", nullptr, 60000);

  if (PDP_APN && strlen(PDP_APN) > 0) {
    char cmd[96];
    snprintf(cmd, sizeof(cmd), "AT+CGDCONT=1,\"IP\",\"%s\"", PDP_APN);
    sendAT(cmd, "OK", nullptr, 5000);
    snprintf(cmd, sizeof(cmd), "AT+QICSGP=1,1,\"%s\",\"\",\"\",1", PDP_APN);
    sendAT(cmd, "OK", nullptr, 5000);
  }

  if (!sendAT("AT+QIACT=1", "OK", nullptr, 150000)) return false;
  sendAT("AT+QIACT?", "OK", nullptr, 5000);
  return true;
}

// ======================================================
//           TLS CERT UPLOAD + SSL CONTEXT
// ======================================================
bool uploadPemToUFS(const char *ufsPath, const char *pem) {
  char cmd[160];
  int len = strlen(pem);

  snprintf(cmd, sizeof(cmd), "AT+QFDEL=\"%s\"", ufsPath);
  sendAT(cmd, "OK", nullptr, 5000);

  snprintf(cmd, sizeof(cmd), "AT+QFUPL=\"%s\",%d,60,0", ufsPath, len);

  while (Modem.available()) Modem.read();
  Modem.print(cmd); Modem.print("\r");

  String line;
  uint32_t start = millis();
  bool gotConnect = false;
  while ((millis() - start) < 5000) {
    if (!modemReadLine(line, 5000 - (millis() - start))) continue;
    if (line.indexOf("CONNECT") >= 0) { gotConnect = true; break; }
    if (line.indexOf("ERROR") >= 0) return false;
  }
  if (!gotConnect) return false;

  Modem.write((const uint8_t*)pem, len);

  bool gotOK = false;
  start = millis();
  while ((millis() - start) < 60000) {
    if (!modemReadLine(line, 60000 - (millis() - start))) continue;
    if (line.indexOf("OK") >= 0) { gotOK = true; break; }
    if (line.indexOf("ERROR") >= 0) return false;
  }
  return gotOK;
}

bool configureSSLContext() {
  static bool filesUploaded = false;

  if (!filesUploaded) {
    if (!uploadPemToUFS("UFS:aws_root_ca.pem",   AWS_ROOT_CA))     return false;
    if (!uploadPemToUFS("UFS:aws_client.pem",    AWS_CLIENT_CERT)) return false;
    if (!uploadPemToUFS("UFS:aws_client.key",    AWS_CLIENT_KEY))  return false;
    filesUploaded = true;
  }

  char cmd[192];

  snprintf(cmd, sizeof(cmd), "AT+QSSLCFG=\"cacert\",%d,\"UFS:aws_root_ca.pem\"", SSL_CTX_ID);
  if (!sendAT(cmd, "OK", nullptr, 5000)) return false;

  snprintf(cmd, sizeof(cmd), "AT+QSSLCFG=\"clientcert\",%d,\"UFS:aws_client.pem\"", SSL_CTX_ID);
  if (!sendAT(cmd, "OK", nullptr, 5000)) return false;

  snprintf(cmd, sizeof(cmd), "AT+QSSLCFG=\"clientkey\",%d,\"UFS:aws_client.key\"", SSL_CTX_ID);
  if (!sendAT(cmd, "OK", nullptr, 5000)) return false;

  snprintf(cmd, sizeof(cmd), "AT+QSSLCFG=\"seclevel\",%d,2", SSL_CTX_ID);
  if (!sendAT(cmd, "OK", nullptr, 5000)) return false;

  snprintf(cmd, sizeof(cmd), "AT+QSSLCFG=\"sslversion\",%d,3", SSL_CTX_ID);
  if (!sendAT(cmd, "OK", nullptr, 5000)) return false;

  snprintf(cmd, sizeof(cmd), "AT+QSSLCFG=\"ignorelocaltime\",%d,1", SSL_CTX_ID);
  if (!sendAT(cmd, "OK", nullptr, 5000)) return false;

  return true;
}

bool setupMQTT() {
  if (!configureSSLContext()) return false;

  // Bind MQTT to PDP
  char cmd[96];
  snprintf(cmd, sizeof(cmd), "AT+QMTCFG=\"pdpcid\",%d,%d", MQTT_CLIENT_IDX, PDP_CID);
  if (!sendAT(cmd, "OK", nullptr, 5000)) return false;

  // MQTT 3.1.1
  if (!sendAT("AT+QMTCFG=\"version\",0,4", "OK", nullptr, 5000)) return false;

  // Bind SSL ctx
  snprintf(cmd, sizeof(cmd), "AT+QMTCFG=\"ssl\",%d,%d,1", MQTT_CLIENT_IDX, SSL_CTX_ID);
  if (!sendAT(cmd, "OK", nullptr, 5000)) return false;

  // Open + Connect
  snprintf(cmd, sizeof(cmd), "AT+QMTOPEN=%d,\"%s\",%d", MQTT_CLIENT_IDX, AWS_HOST, AWS_PORT);
  if (!sendAT(cmd, "OK", nullptr, 60000)) return false;

  String urc;
  if (!waitForQMTOPEN_OK(urc, 120000)) return false;

  snprintf(cmd, sizeof(cmd), "AT+QMTCONN=%d,\"%s\"", MQTT_CLIENT_IDX, AWS_CLIENT_ID);
  if (!sendAT(cmd, "OK", nullptr, 10000)) return false;

  urc = "";
  if (!waitForURC("+QMTCONN:", urc, 120000)) return false;
  if (urc.indexOf("+QMTCONN: 0,0,0") == -1) return false;

  return true;
}

bool bringUpModemAndMQTT() {
  if (gModemPowered && mqttReady && gSimReady && gLteRegistered) return true;

  if (!gModemPowered) {
    modemPowerOnPulse();
    delay(5000);
    gModemPowered = true;
  }

  if (!initModem()) return false;
  if (!setupPDP()) return false;

  mqttReady = setupMQTT();
  gLastPublishOk = false;
  return mqttReady;
}

void powerDownModemForSleep() {
  if (!gModemPowered) return;
  modemPowerOffPulse();
  gModemPowered = false;
  mqttReady = false;
  gLastPublishOk = false;
  gSimReady = false;
  gLteRegistered = false;
}

// ======================================================
//                  MQTT PUBLISH
// ======================================================
bool mqttPublishTelemetry(const String &timestamp,
                          const String &gpsLat,
                          const String &gpsLon,
                          int lteDbm,
                          int gpsSats) {
  unsigned long now = millis();

  float batV = (lastBatMs > 0) ? gBatVolts : 0.0f;
  float fuel = (lastFuelMs > 0) ? gFuelPercent : 0.0f;
  float eKWh = (lastEnergyMs > 0) ? gEnergyKWh : 0.0f;

  float vLN  = safeValue(gAvgLNVolts,    lastAcMs,    now);
  float freq = safeValue(gFrequencyHz,   lastAcMs,    now);
  float iAvg = safeValue(gAvgCurrentA,   lastAcMs,    now);
  float pKW  = safeValue(gActivePowerKW, lastPowerMs, now);

  bool oilPress = safeFlag(gOilPressureActive, lastOilPressMs, now);
  bool oilTemp  = safeFlag(gOilTempActive,     lastOilTempMs,  now);

  int led1 = digitalRead(LED1_PIN) ? 1 : 0;
  int led2 = digitalRead(LED2_PIN) ? 1 : 0;
  int led3 = digitalRead(LED3_PIN) ? 1 : 0;
  int led4 = digitalRead(LED4_PIN) ? 1 : 0;

  String payload = "{";
  payload += "\"TIMESTAMP\":\"" + timestamp + "\",";
  payload += "\"DID\":\"" + String(DEVICE_ID) + "\",";
  payload += "\"4G_RSSI\":" + String(lteDbm) + ",";
  payload += "\"GPS_SATS\":" + String(gpsSats) + ",";
  payload += "\"BATTERY_VOLTAGE\":" + String(batV, 2) + ",";
  payload += "\"SINGLE_PH_VOLTAGE\":" + String(vLN, 1) + ",";
  payload += "\"LINE_CURRENT\":" + String(iAvg, 1) + ",";
  payload += "\"ACTIVE_POWER\":" + String(pKW, 2) + ",";
  payload += "\"FREQUENCY\":" + String(freq, 2) + ",";
  payload += "\"ENERGY\":" + String(eKWh, 3) + ",";
  payload += "\"FUEL_TANK_LEVEL\":" + String(fuel, 1) + ",";
  payload += "\"OIL_PRESSURE\":" + String(oilPress ? 1 : 0) + ",";
  payload += "\"OIL_TEMP\":" + String(oilTemp ? 1 : 0) + ",";
  payload += "\"LED_1\":" + String(led1) + ",";
  payload += "\"LED_2\":" + String(led2) + ",";
  payload += "\"LED_3\":" + String(led3) + ",";
  payload += "\"LED_4\":" + String(led4) + ",";
  payload += "\"GPS_LAT\":\"" + gpsLat + "\",";
  payload += "\"GPS_LONG\":\"" + gpsLon + "\"";
  payload += "}";

  int len = payload.length();

  String cmd = String("AT+QMTPUB=") +
               String(MQTT_CLIENT_IDX) + ",0,0,0,\"" +
               AWS_TOPIC + "\"," + String(len);

  while (Modem.available()) Modem.read();
  Modem.print(cmd); Modem.print("\r");

  if (!waitForPrompt('>', 2000)) return false;

  Modem.print(payload);
  return true;
}

// ======================================================
//                  STATUS LED (RYG)
// ======================================================
bool isCanOk() {
  unsigned long now = millis();
  if (lastCanRxMs == 0) return false;
  return (now - lastCanRxMs) <= CAN_OK_TIMEOUT_MS;
}

void updateStatusRYG() {
  unsigned long now = millis();

  float batV = safeValue(gBatVolts, lastBatMs, now);
  bool canOk = isCanOk();

  bool okGreen = (batV > BAT_OK_THRESHOLD) && canOk;
  if (okGreen) {
    if (now - lastGreenToggleMs >= GREEN_BLINK_INTERVAL_MS) {
      lastGreenToggleMs = now;
      greenOn = !greenOn;
      digitalWrite(G_LED_PIN, greenOn ? HIGH : LOW);
    }
  } else {
    digitalWrite(G_LED_PIN, HIGH);
    greenOn = true;
  }

  static unsigned long lastYellowToggleMs = 0;
  static bool yellowOn = false;
  const unsigned long YELLOW_BLINK_INTERVAL_MS = 500;

  bool gpsValid = (gCurrentGpsSats >= 0);
  if (gpsValid) {
    if (now - lastYellowToggleMs >= YELLOW_BLINK_INTERVAL_MS) {
      lastYellowToggleMs = now;
      yellowOn = !yellowOn;
      digitalWrite(Y_LED_PIN, yellowOn ? HIGH : LOW);
    }
  } else {
    digitalWrite(Y_LED_PIN, HIGH);
    yellowOn = true;
  }

  static unsigned long lastRedToggleMs = 0;
  static bool redOn = false;
  const unsigned long RED_BLINK_INTERVAL_MS = 500;

  bool redOk = mqttReady && gLastPublishOk;
  if (redOk) {
    if (now - lastRedToggleMs >= RED_BLINK_INTERVAL_MS) {
      lastRedToggleMs = now;
      redOn = !redOn;
      digitalWrite(R_LED_PIN, redOn ? HIGH : LOW);
    }
  } else {
    digitalWrite(R_LED_PIN, HIGH);
    redOn = true;
  }
}

// ======================================================
//                     SETUP / LOOP
// ======================================================
void setup() {
  Serial.begin(115200);
  delay(500);

  pinMode(MODEM_PWRKEY, OUTPUT);
  pinMode(MODEM_RST, OUTPUT);
  digitalWrite(MODEM_PWRKEY, HIGH);
  digitalWrite(MODEM_RST, HIGH);

  pinMode(LED1_PIN, INPUT_PULLDOWN);
  pinMode(LED2_PIN, INPUT_PULLDOWN);
  pinMode(LED3_PIN, INPUT);
  pinMode(LED4_PIN, INPUT);

  pinMode(R_LED_PIN, OUTPUT);
  pinMode(Y_LED_PIN, OUTPUT);
  pinMode(G_LED_PIN, OUTPUT);
  digitalWrite(R_LED_PIN, LOW);
  digitalWrite(Y_LED_PIN, LOW);
  digitalWrite(G_LED_PIN, LOW);

  Modem.begin(115200, SERIAL_8N1, MODEM_RX, MODEM_TX);
  delay(2000);

  twai_general_config_t g_config = {
    .mode = TWAI_MODE_NORMAL,
    .tx_io = CAN_TX_PIN,
    .rx_io = CAN_RX_PIN,
    .clkout_io = (gpio_num_t)TWAI_IO_UNUSED,
    .bus_off_io = (gpio_num_t)TWAI_IO_UNUSED,
    .tx_queue_len = 5,
    .rx_queue_len = 5,
    .alerts_enabled = TWAI_ALERT_NONE,
    .clkout_divider = 0,
    .intr_flags = ESP_INTR_FLAG_LEVEL1
  };

  twai_timing_config_t t_config = TWAI_TIMING_CONFIG_250KBITS();
  twai_filter_config_t f_config = TWAI_FILTER_CONFIG_ACCEPT_ALL();

  if (twai_driver_install(&g_config, &t_config, &f_config) != ESP_OK) {
    Serial.println(F("ERROR: twai_driver_install failed"));
    while (true) delay(1000);
  }
  if (twai_start() != ESP_OK) {
    Serial.println(F("ERROR: twai_start failed"));
    while (true) delay(1000);
  }

  lastTelemetry   = millis();
  lastMqttRetryMs = millis();
  lastPrintMs     = millis();
}

void loop() {
  // 1) Read CAN
  twai_message_t msg;
  esp_err_t res = twai_receive(&msg, pdMS_TO_TICKS(10));
  if (res == ESP_OK && msg.extd) {
    lastCanRxMs = millis();
    uint32_t canID = msg.identifier;
    uint8_t  dlc   = msg.data_length_code;
    uint8_t *data  = msg.data;

    if (canID == CAN_ID_FUEL_CONFIG && dlc >= 2) {
      gFuelPercent = (float)data[0];
      if (gFuelPercent > 100.0f) gFuelPercent = 100.0f;
      lastFuelMs = millis();

      uint8_t flags = data[1];
      gOilPressureActive = (flags & 0x01u) != 0;
      gOilTempActive     = (flags & 0x02u) != 0;
      lastOilPressMs = millis();
      lastOilTempMs  = millis();
    }

    uint32_t pgn = pgnFromId(canID);

    if (pgn == PGN_VEP1 && dlc >= 6) {
      uint16_t rawBat = le16(data, 4);
      gBatVolts = rawBat * 0.05f;
      lastBatMs = millis();
    }

    if (pgn == PGN_GAAC && dlc >= 8) {
      gAvgLNVolts  = (float)le16(data, 2);
      gFrequencyHz = (float)le16(data, 4) / 128.0f;
      gAvgCurrentA = (float)le16(data, 6);
      lastAcMs     = millis();
    }

    if (pgn == PGN_GTACP && dlc >= 8) {
      double realW = (double)le32s(data, 0) - 2000000000.0;
      gActivePowerKW = realW / 1000.0;
      lastPowerMs    = millis();
    }

    if (pgn == PGN_GTE && dlc >= 4) {
      gEnergyKWh   = (float)le32u(data, 0);
      lastEnergyMs = millis();
    }

    if (!gCanDataReady && lastBatMs > 0 && lastFuelMs > 0) {
      gCanDataReady = true;
      Serial.println(F("CAN telemetry ready: battery & fuel received."));
    }

    updateDgStateFromSample();
  }

  // 2) Print snapshot
  if (millis() - lastPrintMs >= PRINT_INTERVAL_MS) {
    lastPrintMs = millis();
    printSnapshot();
  }

  unsigned long now = millis();

  // 3) DG state → telemetry period
  bool dgOn = gDgOnState;

  if (dgOn != gDgOnPrev) {
    if (!dgOn) powerDownModemForSleep();
    gDgOnPrev = dgOn;
  }

  unsigned long telemetryPeriod = dgOn ? DG_ON_TELEMETRY_MS : DG_OFF_TELEMETRY_MS;

  // 4) Telemetry scheduler
  if ((now - lastTelemetry) >= telemetryPeriod) {
    lastTelemetry = now;

    if (!gCanDataReady) goto drain_urc;

    if (!bringUpModemAndMQTT()) {
      if (!dgOn) powerDownModemForSleep();
      goto drain_urc;
    }

    // Keep GNSS/time logic in your private repo; here we publish empty placeholders:
    String ts = "";
    String lat = "";
    String lon = "";
    int lteDbm = -999;
    int gpsSats = -1;

    bool ok = mqttPublishTelemetry(ts, lat, lon, lteDbm, gpsSats);
    gLastPublishOk = ok;
    if (!ok) mqttReady = false;

    if (!dgOn) powerDownModemForSleep();
  }

drain_urc:
  // (Optional) URC drain – keep minimal for public code
  while (Modem.available()) {
    String line;
    if (!modemReadLine(line, 10)) break;
    if (!line.length()) continue;

    if (line.indexOf("+QMTOPEN:") >= 0 && line.indexOf("+QMTOPEN: 0,0") < 0) {
      mqttReady = false; gLastPublishOk = false;
    }
    if (line.indexOf("+QMTPUB:") >= 0) {
      if (line.indexOf(",0,0") >= 0) gLastPublishOk = true;
      else gLastPublishOk = false;
    }
    if (line.startsWith("+CPIN:")) {
      gSimReady = (line.indexOf("READY") >= 0);
      if (!gSimReady) { gLteRegistered = false; mqttReady = false; gLastPublishOk = false; }
    }
    if (line.startsWith("+CEREG:")) {
      int firstComma = line.indexOf(',');
      if (firstComma >= 0) {
        String statStr = line.substring(firstComma + 1);
        statStr.trim();
        int stat = statStr.toInt();
        gLteRegistered = (stat == 1 || stat == 5);
        if (!gLteRegistered) { mqttReady = false; gLastPublishOk = false; }
      }
    }
  }

  updateStatusRYG();
}