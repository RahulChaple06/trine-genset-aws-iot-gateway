// GPS plain

/*
  ESP32-WROOM-32D + Quectel EC200U-CN
  Simple GNSS test: print satellites + latitude + longitude

  UART1:
    MODEM_RX = GPIO21  (EC200U TX)
    MODEM_TX = GPIO22  (EC200U RX)

  Control pins (adjust if your board is different):
    PWRKEY = GPIO4 (active LOW)
    RST    = GPIO2 (active LOW)

  GNSS:
    AT+QGPS=1       -> turn GNSS ON
    AT+QGPSLOC=2    -> get location (with nsat)
    +QGPSLOC: <UTC>,<lat>,<lon>,<hdop>,<alt>,<fix>,<cog>,<spkm>,<spkn>,<date>,<nsat>
*/

#include <Arduino.h>

HardwareSerial Modem(1);   // UART1

#define MODEM_RX    21
#define MODEM_TX    22
#define MODEM_PWRKEY 4
#define MODEM_RST    2

// ==================== Basic helpers ====================

bool modemReadLine(String &out, uint32_t timeout = 1000) {
  out = "";
  uint32_t start = millis();
  while ((millis() - start) < timeout) {
    while (Modem.available()) {
      char c = (char)Modem.read();
      if (c == '\r') continue;
      if (c == '\n') {
        if (out.length() > 0) {
          return true;
        } else {
          // skip empty line
          continue;
        }
      }
      out += c;
    }
  }
  return (out.length() > 0);
}

bool sendAT(const char *cmd,
            const char *expect1 = "OK",
            const char *expect2 = nullptr,
            uint32_t timeout = 1000) {
  Serial.print(">> ");
  Serial.println(cmd);

  // flush old input
  while (Modem.available()) Modem.read();

  Modem.print(cmd);
  Modem.print("\r");

  uint32_t start = millis();
  String line;

  while ((millis() - start) < timeout) {
    if (!modemReadLine(line, timeout - (millis() - start))) {
      continue;
    }
    Serial.print("<< ");
    Serial.println(line);

    if (line.indexOf("ERROR") >= 0) {
      return false;
    }
    if (expect1 && line.indexOf(expect1) >= 0) {
      return true;
    }
    if (expect2 && line.indexOf(expect2) >= 0) {
      return true;
    }
  }
  return false;
}

// ==================== Modem control ====================

void modemPowerOnPulse() {
  // Low >= 2s to power on from power-down
  digitalWrite(MODEM_PWRKEY, LOW);
  delay(2200);
  digitalWrite(MODEM_PWRKEY, HIGH);
}

void modemHardwareReset() {
  digitalWrite(MODEM_RST, LOW);
  delay(150);
  digitalWrite(MODEM_RST, HIGH);
}

bool waitForSIMReady() {
  for (int i = 0; i < 10; ++i) {
    if (sendAT("AT+CPIN?", "READY", nullptr, 2000)) {
      Serial.println("SIM is ready.");
      return true;
    }
    Serial.println("SIM not ready yet, waiting...");
    delay(1000);
  }
  return false;
}

bool initModem() {
  Serial.println("Bringing up EC200U...");

  if (!sendAT("AT", "OK", nullptr, 2000)) {
    Serial.println("No response to AT.");
    return false;
  }

  sendAT("ATE0", "OK", nullptr, 2000);   // echo off
  sendAT("AT+CMEE=2", "OK", nullptr, 2000); // verbose errors

  if (!waitForSIMReady()) {
    Serial.println("SIM not ready.");
    return false;
  }

  // OPTIONAL: network info (not strictly needed for GNSS)
  sendAT("AT+QNWINFO", "OK", nullptr, 5000);

  return true;
}

// ==================== GNSS functions ====================

bool startGNSS() {
  // Turn GNSS ON in stand-alone mode
  // AT+QGPS=1  OR AT+QGPS=1,1,1,0,1 (full parameter form)
  if (!sendAT("AT+QGPS=1", "OK", nullptr, 5000)) {
    Serial.println("Failed to start GNSS (QGPS).");
    return false;
  }
  Serial.println("GNSS started (QGPS=1).");
  return true;
}

// Parse +QGPSLOC line and extract lat, lon, nsat
bool parseQGPSLOC(const String &line, String &lat, String &lon, int &sats) {
  int colon = line.indexOf(':');
  if (colon < 0) return false;

  String data = line.substring(colon + 1);
  data.trim();

  int fieldIdx = 0;
  int last = 0;
  String nsatStr;

  while (true) {
    int comma = data.indexOf(',', last);
    String token;
    if (comma < 0) token = data.substring(last);
    else          token = data.substring(last, comma);
    token.trim();

    if (fieldIdx == 1) lat = token;     // latitude
    else if (fieldIdx == 2) lon = token; // longitude
    else if (fieldIdx == 10) nsatStr = token; // nsat

    if (comma < 0) break;
    last = comma + 1;
    fieldIdx++;
  }

  if (lat.length() == 0 || lon.length() == 0) {
    return false;
  }
  sats = nsatStr.toInt();
  return true;
}

bool readGNSS(String &lat, String &lon, int &sats) {
  Serial.println(">> AT+QGPSLOC=2");
  while (Modem.available()) Modem.read();   // flush

  Modem.print("AT+QGPSLOC=2\r");

  uint32_t start = millis();
  String line;
  bool gotLoc = false;

  while ((millis() - start) < 5000) {
    if (!modemReadLine(line, 5000)) continue;
    Serial.print("<< ");
    Serial.println(line);

    if (line.startsWith("+QGPSLOC:")) {
      gotLoc = true;
      break;
    }

    if (line.indexOf("+CME ERROR:") >= 0) {
      // 505 – GPS disabled, 516 – no fix (per Quectel docs / vendors)
      Serial.println("No valid GNSS fix yet (CME ERROR).");
      return false;
    }
  }

  if (!gotLoc) {
    Serial.println("No QGPSLOC response.");
    return false;
  }

  if (!parseQGPSLOC(line, lat, lon, sats)) {
    Serial.println("Failed to parse QGPSLOC line.");
    return false;
  }

  return true;
}

// ==================== Arduino setup/loop ====================

void setup() {
  Serial.begin(115200);
  delay(500);

  Serial.println();
  Serial.println("ESP32 + EC200U-CN | GNSS test (sats + lat/lon)");

  pinMode(MODEM_PWRKEY, OUTPUT);
  pinMode(MODEM_RST, OUTPUT);
  digitalWrite(MODEM_PWRKEY, HIGH);
  digitalWrite(MODEM_RST, HIGH);

  Modem.begin(115200, SERIAL_8N1, MODEM_RX, MODEM_TX);
  delay(2000); // allow modem boot

  // Pulse PWRKEY if your module needs it (uncomment if required)
  // modemPowerOnPulse();
  // delay(5000);

  if (!initModem()) {
    Serial.println("Modem init failed. Check wiring/SIM.");
    return;
  }

  if (!startGNSS()) {
    Serial.println("Could not start GNSS. Check that your module/variant has GNSS enabled.");
    return;
  }

  Serial.println("Now polling GNSS every 5 seconds...");
}

void loop() {
  static unsigned long last = 0;
  unsigned long now = millis();

  if (now - last >= 5000) {
    last = now;

    String lat, lon;
    int sats = -1;

    if (readGNSS(lat, lon, sats)) {
      Serial.print("✅ GNSS fix: sats=");
      Serial.print(sats);
      Serial.print("  lat=");
      Serial.print(lat);
      Serial.print("  lon=");
      Serial.println(lon);
    } else {
      Serial.println("❌ No valid GNSS fix yet.");
    }

    Serial.println("-----------------------------------");
  }
}