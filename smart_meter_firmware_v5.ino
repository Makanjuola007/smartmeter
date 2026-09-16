/*
 * Smart Prepaid Meter Firmware — dual-core task split + batched comms
 * WiFi + WiFiManager edition, v3: real-time stream + LED diagnostics
 *   + relay command/status split (fixes recharge relay flap bug)
 * ─────────────────────────────────────────────────────────────
 * v3 changes (fixing "relay shows ON in Firebase but stays OFF
 * on hardware after a top-up, and briefly flickers through IDLE"):
 *
 *  The device was reading back its OWN status field as if it were
 *  a command. Firebase's "units/unit_x/relay_state" was written
 *  BY the device (telemetry push, every 5 s) to report actual
 *  hardware state, AND read BY the device (relay poll, every 2 s,
 *  plus the SSE stream) as an instruction to set the relay. Since
 *  the poll runs more often than the push, a real state change
 *  (e.g. balance-triggered TOPUP flipping the relay ON) could get
 *  immediately stomped by a poll cycle that still saw the OLD
 *  ("false") value sitting in Firebase from before the push caught
 *  up — turning the relay back off and the status back to IDLE,
 *  while Firebase still displayed relay_state: true.
 *
 *  Fix: split the single field into two, so the device never reads
 *  back something it itself writes:
 *    - "relay_state"    -> STATUS ONLY. Device writes it (actual
 *                           hardware state). Device never reads it.
 *    - "relay_command"  -> COMMAND ONLY. Backend/app writes it
 *                           (desired state). Device only reads it,
 *                           never writes it.
 *  Update whatever writes relay commands today (dashboard/app) to
 *  write to "relay_command" instead of "relay_state".
 *
 * ─────────────────────────────────────────────────────────────
 * v2 changes (fixing "no data reaching backend" + adding real-time):
 *
 *  1. CommsTask stack raised 8192 -> 16384 bytes. WiFiClientSecure's
 *     TLS handshake is stack-hungry; 8192 is a common cause of a
 *     comms task that silently stalls/aborts HTTPS calls with no
 *     crash and no serial output to explain why - it just looks
 *     like "nothing ever arrives at Firebase".
 *
 *  2. WiFi.setSleep(false) after connecting. ESP32 WiFi power-save
 *     (modem sleep) is a common cause of dropped/delayed packets
 *     and slow/failed TLS handshakes - directly hurts both
 *     reliability and the "real-time" goal.
 *
 *  3. Explicit HTTPClient connect/response timeouts (5 s) so a
 *     stalled TLS session can't hang CommsTask indefinitely - it
 *     fails fast and retries on the next scheduler pass instead.
 *
 *  4. On-board LED status codes (no serial monitor needed):
 *       - 1 short flash  = telemetry/data SENT successfully
 *       - 2 short flashes = data RECEIVED successfully (stream
 *                            event or a GET)
 *       - 1 long flash    = a send/receive attempt FAILED
 *     See blinkSent()/blinkReceived()/blinkFail() below.
 *
 *  5. Real-time relay commands via Firebase's REST streaming API
 *     (Server-Sent Events) instead of 10 s polling. A dedicated
 *     StreamTask holds one persistent HTTPS connection open to
 *     /units.json with "Accept: text/event-stream" and reacts the
 *     instant Firebase pushes a change - typically well under a
 *     second, not "up to 10-30 s away". pollRelayStates() is kept
 *     as a periodic fallback safety net (every 30 s) in case the
 *     stream silently drops.
 *
 *  6. Faster default intervals now that we're not paying GPRS
 *     AT-command overhead per call (see the #define block below).
 *
 *  Libraries needed (Library Manager): "WiFiManager" by tzapu,
 *  "ArduinoJson" by Benoit Blanchon.
 * ─────────────────────────────────────────────────────────────
 *
 * v4 changes (adding NVS-backed persistence — see
 * preferences_manager.h / preferences_manager.cpp):
 *
 *  Everything used to live only in RAM (unit_balance_kwh[],
 *  unit_consumed_kwh[], unit_relay[], rate_naira_kwh). Any reset
 *  — brown-out, WDT panic, crash, deliberate reboot — threw all of
 *  it away and every unit came back at zero balance/OFF regardless
 *  of what they'd actually paid for or how much they'd used.
 *
 *  Fix: a dedicated storage module wraps the ESP32 Preferences
 *  (NVS) library. It is intentionally NOT folded into this file —
 *  Firebase/relay/sensor code never touches Preferences directly,
 *  it only calls save*()/checkpoint*()/get*() from
 *  preferences_manager.h. All new lines below are marked
 *  "NVS PERSISTENCE" so they're easy to find/diff against the
 *  previous version of this file.
 *
 *  - setup() now calls initializeStorage() and recovers every
 *    unit's balance/energy/relay state from flash BEFORE any task
 *    starts, and drives the shift register from that recovered
 *    state - so the meter enforces correct cutoffs from the very
 *    first sensor loop even if WiFi/Firebase is unreachable at
 *    boot (v3's connectWiFi() retry loop no longer matters for
 *    correctness, only for getting fresh data).
 *  - sensorTask() calls the throttled checkpointUnitBalance()/
 *    checkpointEnergy() every iteration (cheap - only actually
 *    writes to flash once drift/interval thresholds are crossed),
 *    and does an immediate, reliable save the moment a cutoff
 *    happens.
 *  - commsTask()'s top-up-restore branch does an immediate,
 *    reliable save of balance + relay state the moment a top-up is
 *    detected, and loadBalancesAndRate() persists a fresh tariff
 *    whenever the backend pushes one.
 *  - applyRelayCommand() (used by both the poll fallback and the
 *    SSE stream) persists relay flips immediately.
 * ─────────────────────────────────────────────────────────────
 *
 * v5 changes (namespacing Firebase paths under /meters/<id>):
 *
 *  Every path used to be written at the DATABASE ROOT ("/units",
 *  "/system", and the multi-path PATCH target ""). That's fine
 *  for a single meter, but doesn't scale to multiple physical
 *  meters sharing one Firebase project - there's nowhere to put
 *  a second meter's data without colliding with the first.
 *
 *  Fix: every path this device reads or writes is now prefixed
 *  with METER_BASE ("/meters/meter_001"). Nothing else changes -
 *  same fields, same read/write ownership split (relay_state vs
 *  relay_command), same push/poll/stream logic, same NVS
 *  persistence. This is a pure path-prefix change:
 *    /units                      -> /meters/meter_001/units
 *    /system/rate_naira_kwh      -> /meters/meter_001/system/rate_naira_kwh
 *    "" (multi-path PATCH root)  -> /meters/meter_001
 *    /units.json (SSE stream)    -> /meters/meter_001/units.json
 *
 *  To add a second meter, flash this same firmware with METER_ID
 *  changed to e.g. "meter_002" - each device then only ever reads
 *  and writes its own subtree.
 * ─────────────────────────────────────────────────────────────
 */

#include <Arduino.h>
#include <EmonLib.h>
#include <ArduinoJson.h>
#include <WiFi.h>
#include <WiFiClientSecure.h>
#include <HTTPClient.h>
#include <WiFiManager.h>          // tzapu/WiFiManager
#include "esp_idf_version.h"
#include "esp_task_wdt.h"
#include "preferences_manager.h"  // NVS PERSISTENCE - see preferences_manager.h/.cpp

// ─────────────────────────────────────────────
//  On-board status LED
//  Most ESP32 devkits: GPIO2. If your board has no onboard LED,
//  wire any spare GPIO to an LED (+ resistor) and change this pin.
// ─────────────────────────────────────────────
#define STATUS_LED_PIN  2
SemaphoreHandle_t ledMutex;

void blinkPattern(uint8_t count, uint16_t onMs, uint16_t offMs) {
  xSemaphoreTake(ledMutex, portMAX_DELAY);
  for (uint8_t i = 0; i < count; i++) {
    digitalWrite(STATUS_LED_PIN, HIGH);
    delay(onMs);
    digitalWrite(STATUS_LED_PIN, LOW);
    if (i < count - 1) delay(offMs);
  }
  xSemaphoreGive(ledMutex);
}

void blinkSent()     { blinkPattern(1, 60, 0);    }   // 1 short flash  - data sent OK
void blinkReceived()  { blinkPattern(2, 60, 100); }   // 2 short flashes - data received OK
void blinkFail()      { blinkPattern(1, 600, 0);  }   // 1 long flash   - send/receive failed

#define DATA_PIN   18
#define CLOCK_PIN  19
#define LATCH_PIN  23

byte relayState = 0b00000000;

void relayStateToBinStr(byte state, char out[9]) {
  for (uint8_t i = 0; i < 8; i++) {
    out[i] = (state & (1 << (7 - i))) ? '1' : '0';
  }
  out[8] = '\0';
}

void updateShiftRegister() {
  digitalWrite(LATCH_PIN, LOW);
  shiftOut(DATA_PIN, CLOCK_PIN, MSBFIRST, ~relayState);
  digitalWrite(LATCH_PIN, HIGH);
}

// ─────────────────────────────────────────────
//  WiFi / WiFiManager config
// ─────────────────────────────────────────────
#define WIFI_AP_NAME        "SmartMeter-Setup"   // captive-portal AP name
#define WIFI_AP_PASSWORD    ""                   // "" = open AP, or set an 8+ char password
#define WIFI_PORTAL_TIMEOUT_S  120               // give up on portal after 2 min, retry later
#define WIFI_CONNECT_TIMEOUT_S  12                // give up on a saved AP after 12 s, fall to portal

// StreamTask needs a second, permanently-open TLS session running
// alongside CommsTask's periodic calls. On a no-PSRAM board with
// an Arduino-ESP32 3.x core (no manual TLS buffer sizing - see
// configureSecureClient() below), that's two full-size sessions
// competing for ~250-300KB of heap, which is exactly why every
// HTTP call after the stream connected came back "status -1":
// there simply wasn't room for a second session. Leave this at 0
// unless you're on a PSRAM board (e.g. ESP32-WROVER), where a few
// MB of extra RAM makes room for a persistent stream connection.
#define ENABLE_REALTIME_STREAM  0
#define WIFI_RESET_PIN       4                   // hold LOW at boot to wipe saved WiFi creds

#define FIREBASE_HOST  "edey-bd34b-default-rtdb.firebaseio.com"
#define FIREBASE_AUTH  "k3lxYQGcJctYHYFRqJUKeeCRhy573MtNDvrurkVq"

// ── METER NAMESPACE (v5) ────────────────────────────────────
// Every Firebase path this device touches lives under this
// subtree. Change METER_ID per physical device when flashing
// more than one meter, so each one only ever reads/writes its
// own data (e.g. "meter_002", "meter_003", ...).
#define METER_ID    "SOL001007"
#define METER_BASE  "/meters/" METER_ID   // -> "/meters/meter_001"
// ── END METER NAMESPACE ──────────────────────────────────────

#define NUM_UNITS  7

const char* UNIT_KEY[NUM_UNITS] = {
  "unit_1","unit_2","unit_3","unit_4","unit_5","unit_6","unit_7"
};

#define PIN_V_SOLAR  34
#define PIN_V_MAINS  35

const uint8_t CT_PINS[NUM_UNITS] = { 32, 33, 36, 39, 25, 26, 27 };

#define VCAL_SOLAR   66.0
#define VCAL_MAINS   66.0
#define ICAL         2.3
#define PHASE        1.7
#define CROSSINGS    20
#define TIMEOUT_MS   2000

EnergyMonitor emon_v_solar;
EnergyMonitor emon_v_mains;
EnergyMonitor emon_i[NUM_UNITS];

// ─────────────────────────────────────────────
//  Shared state — ALWAYS access through dataMutex
// ─────────────────────────────────────────────
SemaphoreHandle_t dataMutex;

float rate_naira_kwh = 200.0f;

#define TELEMETRY_PUSH_INTERVAL_MS  5000UL  // combined units + system push
#if ENABLE_REALTIME_STREAM
#define RELAY_POLL_INTERVAL_MS     30000UL  // fallback only - StreamTask is primary path
#else
#define RELAY_POLL_INTERVAL_MS      2000UL  // primary path - fast poll stands in for push (see ENABLE_REALTIME_STREAM)
#endif
#define BALANCE_POLL_INTERVAL_MS   15000UL
#define WIFI_CHECK_INTERVAL_MS     30000UL
#define SENSOR_YIELD_MS              10UL
#define STREAM_IDLE_TIMEOUT_MS     45000UL  // reconnect stream if silent this long (only used if ENABLE_REALTIME_STREAM)

float  unit_irms[NUM_UNITS]         = {0};
float  unit_power_w[NUM_UNITS]      = {0};
float  unit_balance_kwh[NUM_UNITS]  = {0};
float  unit_consumed_kwh[NUM_UNITS] = {0};
float  unit_pending_delta_kwh[NUM_UNITS] = {0};
bool   unit_relay[NUM_UNITS];
String unit_status[NUM_UNITS];

float  v_solar_rms    = 0.0f;
float  v_mains_rms    = 0.0f;
String source_active  = "none";
bool   solar_available = false;
bool   mains_available = false;

bool   connectWiFi();
void   checkWifiResetRequestAtBoot();
bool   firebasePut(const String& path, const String& body);
bool   firebasePatch(const String& path, const String& body);
String firebaseGet(const String& path);
String fbURL(const String& path);
void   loadBalancesAndRate();
void   pollRelayStates();
void   pushTelemetryAndSystem();
void   setRelayLocked(uint8_t u, bool closed);   // caller must hold dataMutex
void   setRelay(uint8_t u, bool closed);          // takes mutex itself
void   setAllRelaysLocked(bool closed);
void   setAllRelays(bool closed);
void   applyRelayCommand(uint8_t u, bool requested);
String detectSource(float v_solar, float v_mains);
float  estimatePF(float irms);
void   sensorTask(void* pv);
void   commsTask(void* pv);
void   streamTask(void* pv);
void   handleUnitsPushEvent(const String& path, JsonVariant data);

void setup() {
  Serial.begin(115200);
  delay(100);

  // WiFiManager's captive portal (DNS + web server) runs inside
  // CommsTask, which is pinned to core 0. If it processes portal
  // requests for too long without yielding, the core-0 IDLE task
  // can starve and trip the Task Watchdog Timer, hard-rebooting
  // the board mid-connect.
  //
  // The legacy disableCore0WDT() call is broken on newer
  // Arduino-ESP32 cores (3.x, built on ESP-IDF 5): it tries to
  // remove the idle task from a watchdog list that no longer
  // works that way, and instead of disabling anything it floods
  // the serial log with "esp_task_wdt_reset(): task not found"
  // forever. So we detect which watchdog API is available and use
  // the correct one: on IDF5-based cores, reconfigure the TWDT to
  // stop watching idle tasks and use a longer timeout; on older
  // cores, fall back to the legacy disable call.
#if defined(ESP_IDF_VERSION_MAJOR) && ESP_IDF_VERSION_MAJOR >= 5
  {
    esp_task_wdt_config_t twdt_config = {
      .timeout_ms = 30000,     // generous - just stops false-positive panics
      .idle_core_mask = 0,     // don't watch idle tasks at all
      .trigger_panic = false,  // log instead of hard-reboot if ever exceeded
    };
    esp_task_wdt_reconfigure(&twdt_config);
  }
#else
  disableCore0WDT();
#endif

  pinMode(DATA_PIN,  OUTPUT);
  pinMode(CLOCK_PIN, OUTPUT);
  pinMode(LATCH_PIN, OUTPUT);
  relayState = 0;
  updateShiftRegister();
  Serial.println("[RELAY] Shift register initialised - all relays OFF.");

  pinMode(WIFI_RESET_PIN, INPUT_PULLUP);
  checkWifiResetRequestAtBoot();   // one-shot decision, made here and only here
  pinMode(STATUS_LED_PIN, OUTPUT);
  digitalWrite(STATUS_LED_PIN, LOW);
  ledMutex = xSemaphoreCreateMutex();

  for (uint8_t i = 0; i < NUM_UNITS; i++) {
    unit_relay[i]  = false;
    unit_status[i] = "IDLE";
  }

  analogSetAttenuation(ADC_11db);

  emon_v_solar.voltage(PIN_V_SOLAR, VCAL_SOLAR, PHASE);
  emon_v_mains.voltage(PIN_V_MAINS, VCAL_MAINS, PHASE);
  for (uint8_t i = 0; i < NUM_UNITS; i++) {
    emon_i[i].current(CT_PINS[i], ICAL);
  }

  dataMutex = xSemaphoreCreateMutex();

  // ── NVS PERSISTENCE ─────────────────────────────────────────
  // Bring up flash storage and recover every unit's last-known
  // balance/energy/relay state BEFORE any task starts touching
  // the shared arrays. This runs before WiFi is even attempted,
  // so the meter enforces correct balances/cutoffs from the very
  // first sensorTask loop regardless of internet availability
  // (see the v4 note at the top of this file).
  if (!initializeStorage()) {
    Serial.println("[BOOT] Persistent storage init FAILED - continuing with RAM-only defaults.");
  } else {
    xSemaphoreTake(dataMutex, portMAX_DELAY);
    for (uint8_t i = 0; i < NUM_UNITS; i++) {
      UnitPersistentData d;
      if (loadUnitData(i + 1, d)) {   // +1: array index -> apartment number (1-7)
        unit_balance_kwh[i]  = d.balance_kwh;
        unit_consumed_kwh[i] = d.total_energy_kwh;
        unit_relay[i]        = d.relay_state;
        unit_status[i]       = d.relay_state ? "ACTIVE"
                              : (d.balance_kwh <= 0.0f ? "CUTOFF" : "IDLE");
      }
    }
    rate_naira_kwh = getGlobalRate();
    xSemaphoreGive(dataMutex);
    Serial.println("[BOOT] Recovered unit balances/energy/relay state from NVS.");

    // Drive the shift register to match the just-recovered relay
    // states (a unit only comes back ON if it both has credit AND
    // was actually ON before the reset).
    xSemaphoreTake(dataMutex, portMAX_DELAY);
    for (uint8_t i = 0; i < NUM_UNITS; i++) {
      setRelayLocked(i, unit_relay[i] && unit_balance_kwh[i] > 0.0f);
    }
    xSemaphoreGive(dataMutex);
  }
  // ── END NVS PERSISTENCE ─────────────────────────────────────

  // SensorTask: time-critical, local only -> higher priority, core 1
  xTaskCreatePinnedToCore(sensorTask, "SensorTask", 4096,  NULL, 2, NULL, 1);
  // CommsTask: WiFi setup + batched Firebase HTTP -> core 0
  // (16384: WiFiClientSecure's TLS handshake needs real headroom -
  //  8192 is a common cause of HTTPS calls silently failing)
  xTaskCreatePinnedToCore(commsTask,  "CommsTask",  16384, NULL, 1, NULL, 0);
#if ENABLE_REALTIME_STREAM
  // StreamTask: holds the persistent Firebase SSE connection open
  // for instant relay-command reaction -> also core 0. Only safe
  // on boards with enough spare RAM for two concurrent TLS
  // sessions (see ENABLE_REALTIME_STREAM comment above).
  xTaskCreatePinnedToCore(streamTask, "StreamTask", 12288, NULL, 1, NULL, 0);
#endif

  Serial.println("[BOOT] Tasks started - SensorTask(core1) / CommsTask(core0).");
}

void loop() {
  // Everything happens in the two FreeRTOS tasks above.
  vTaskDelay(portMAX_DELAY);
}

// ─────────────────────────────────────────────
//  SensorTask — voltage/current sampling, energy & credit
//  accounting, local relay cutoff. Never touches WiFi/HTTP.
// ─────────────────────────────────────────────
void sensorTask(void* pv) {
  unsigned long lastMs = millis();

  for (;;) {
    emon_v_solar.calcVI(CROSSINGS, TIMEOUT_MS);
    emon_v_mains.calcVI(CROSSINGS, TIMEOUT_MS);
    float vs = emon_v_solar.Vrms;
    float vm = emon_v_mains.Vrms;
    bool  solar_ok = (vs > 50.0f);
    bool  mains_ok = (vm > 50.0f);
    String src     = detectSource(vs, vm);
    float v_active = mains_ok ? vm : vs;

    unsigned long now = millis();
    float dt_h = (float)(now - lastMs) / 3600000.0f;
    lastMs = now;

    for (uint8_t u = 0; u < NUM_UNITS; u++) {
      bool relay_on;
      xSemaphoreTake(dataMutex, portMAX_DELAY);
      relay_on = unit_relay[u];
      xSemaphoreGive(dataMutex);

      float irms = 0.0f;
      float p    = 0.0f;

      if (relay_on) {
        irms = emon_i[u].calcIrms(1480);
        float pf = estimatePF(irms);
        p = v_active * irms * pf;
      }

      float delta_kwh = (p * dt_h) / 1000.0f;

      bool justCutOff = false;   // NVS PERSISTENCE: track locally, save outside the mutex

      xSemaphoreTake(dataMutex, portMAX_DELAY);
      unit_irms[u]    = irms;
      unit_power_w[u] = p;

      if (relay_on) {
        unit_consumed_kwh[u]      += delta_kwh;
        unit_balance_kwh[u]       -= delta_kwh;
        unit_pending_delta_kwh[u] += delta_kwh;

        if (unit_balance_kwh[u] <= 0.0f) {
          unit_balance_kwh[u] = 0.0f;
          setRelayLocked(u, false);
          justCutOff = true;   // NVS PERSISTENCE
          Serial.printf("[CUTOFF] %s balance exhausted.\n", UNIT_KEY[u]);
        }
      }

      float snapshotBalance = unit_balance_kwh[u];   // NVS PERSISTENCE: read under mutex
      float snapshotEnergy  = unit_consumed_kwh[u];   // NVS PERSISTENCE: read under mutex
      xSemaphoreGive(dataMutex);

      // ── NVS PERSISTENCE ───────────────────────────────────
      // Never write to flash while dataMutex is held - a flash
      // write must not be able to stall any other task waiting
      // on that mutex. checkpoint*() is cheap to call every loop
      // (just a RAM comparison); it only actually touches flash
      // once the throttling thresholds are crossed. A cutoff is
      // saved immediately and reliably, bypassing the throttle,
      // since "relay is OFF, balance is 0" must never be lost.
      if (justCutOff) {
        saveRelayState(u + 1, false);
        saveUnitBalance(u + 1, 0.0f);
      } else {
        checkpointUnitBalance(u + 1, snapshotBalance);
      }
      checkpointEnergy(u + 1, snapshotEnergy);
      // ── END NVS PERSISTENCE ───────────────────────────────
    }

    xSemaphoreTake(dataMutex, portMAX_DELAY);
    v_solar_rms     = vs;
    v_mains_rms     = vm;
    solar_available = solar_ok;
    mains_available = mains_ok;
    source_active   = src;
    xSemaphoreGive(dataMutex);

    xSemaphoreTake(dataMutex, portMAX_DELAY);
    char binStr[9];
    relayStateToBinStr(relayState, binStr);
    Serial.printf("[SENSE] Src=%s  Vsolar=%.2fV  Vmains=%.2fV  Register=0b%s\n",
                  source_active.c_str(), v_solar_rms, v_mains_rms, binStr);
    for (uint8_t u = 0; u < NUM_UNITS; u++) {
      Serial.printf("  %s  I=%.3fA  P=%.1fW  Bal=%.4fkWh  %s  Relay=%s\n",
                    UNIT_KEY[u], unit_irms[u], unit_power_w[u],
                    unit_balance_kwh[u], unit_status[u].c_str(),
                    unit_relay[u] ? "ON" : "OFF");
    }
    xSemaphoreGive(dataMutex);

    vTaskDelay(pdMS_TO_TICKS(SENSOR_YIELD_MS));
  }
}

// ─────────────────────────────────────────────
//  CommsTask — WiFi connection + all Firebase traffic, on core 0.
//  Keeps the existing "most-overdue-task-wins" fairness scheduler.
// ─────────────────────────────────────────────
void commsTask(void* pv) {
  while (!connectWiFi()) {
    Serial.println("[WiFi] Retrying in 10 s...");
    vTaskDelay(pdMS_TO_TICKS(10000));
  }

  loadBalancesAndRate();

  xSemaphoreTake(dataMutex, portMAX_DELAY);
  for (uint8_t i = 0; i < NUM_UNITS; i++) {
    setRelayLocked(i, unit_balance_kwh[i] > 0.0f);
  }
  xSemaphoreGive(dataMutex);

  Serial.println("[BOOT] Smart meter ready.");

  unsigned long last_push_ms       = 0;
  unsigned long last_relay_poll_ms = 0;
  unsigned long last_balance_ms    = 0;
  unsigned long last_wifi_check_ms = 0;

  for (;;) {
    unsigned long now = millis();

    if (now - last_wifi_check_ms >= WIFI_CHECK_INTERVAL_MS) {
      if (WiFi.status() != WL_CONNECTED) {
        Serial.println("[WiFi] Lost - reconnecting...");
        connectWiFi();
      }
      last_wifi_check_ms = now;
    }

    // ---- Run whichever network task is MOST OVERDUE, not the
    // first checked, so no single task can starve the others. ----
    struct NetTask { unsigned long *last; unsigned long interval; uint8_t id; };
    NetTask tasks[3] = {
      { &last_push_ms,       TELEMETRY_PUSH_INTERVAL_MS, 0 },
      { &last_relay_poll_ms, RELAY_POLL_INTERVAL_MS,     1 },
      { &last_balance_ms,    BALANCE_POLL_INTERVAL_MS,   2 },
    };

    int8_t pick = -1;
    long   worstOverdue = 0;
    for (uint8_t i = 0; i < 3; i++) {
      long overdue = (long)(now - *tasks[i].last) - (long)tasks[i].interval;
      if (overdue >= 0 && (pick == -1 || overdue > worstOverdue)) {
        pick = i;
        worstOverdue = overdue;
      }
    }

    if (pick >= 0) {
      switch (tasks[pick].id) {
        case 0:
          pushTelemetryAndSystem();
          break;
        case 1:
          pollRelayStates();
          break;
        case 2: {
          loadBalancesAndRate();
          bool topupHappened[NUM_UNITS] = { false };   // NVS PERSISTENCE
          xSemaphoreTake(dataMutex, portMAX_DELAY);
          for (uint8_t u = 0; u < NUM_UNITS; u++) {
            if (!unit_relay[u] && unit_balance_kwh[u] > 0.0f) {
              setRelayLocked(u, true);
              topupHappened[u] = true;   // NVS PERSISTENCE
              Serial.printf("[TOPUP] %s recharged - relay restored.\n", UNIT_KEY[u]);
            }
          }
          xSemaphoreGive(dataMutex);

          // ── NVS PERSISTENCE ───────────────────────────────
          // A confirmed top-up must never be lost - save the new
          // balance and relay state immediately, outside the
          // mutex, bypassing the routine checkpoint throttle.
          for (uint8_t u = 0; u < NUM_UNITS; u++) {
            if (topupHappened[u]) {
              float bal;
              xSemaphoreTake(dataMutex, portMAX_DELAY);
              bal = unit_balance_kwh[u];
              xSemaphoreGive(dataMutex);
              saveUnitBalance(u + 1, bal);
              saveRelayState(u + 1, true);
            }
          }
          // ── END NVS PERSISTENCE ───────────────────────────
          break;
        }
      }
      *tasks[pick].last = now;
    }

    vTaskDelay(pdMS_TO_TICKS(200));
  }
}

// ─────────────────────────────────────────────
//  Relay control (unchanged logic — no network call here, relay
//  state rides along in the next batched telemetry push)
// ─────────────────────────────────────────────
void setRelayLocked(uint8_t u, bool closed) {
  if (closed) {
    bitSet(relayState, u);
  } else {
    bitClear(relayState, u);
  }
  updateShiftRegister();

  unit_relay[u]  = closed;
  unit_status[u] = closed ? "ACTIVE"
                 : (unit_balance_kwh[u] <= 0.0f ? "CUTOFF" : "IDLE");

  if (!closed) {
    unit_irms[u]    = 0.0f;
    unit_power_w[u] = 0.0f;
  }

  char binStrLog[9];
  relayStateToBinStr(relayState, binStrLog);
  Serial.printf("[RELAY] %s -> %s  Register=0b%s  Status=%s\n",
                UNIT_KEY[u], closed ? "ON" : "OFF",
                binStrLog, unit_status[u].c_str());
}

void setRelay(uint8_t u, bool closed) {
  xSemaphoreTake(dataMutex, portMAX_DELAY);
  setRelayLocked(u, closed);
  xSemaphoreGive(dataMutex);
}

void setAllRelaysLocked(bool closed) {
  relayState = closed ? 0b01111111 : 0b00000000;
  updateShiftRegister();
  for (uint8_t u = 0; u < NUM_UNITS; u++) {
    unit_relay[u]  = closed;
    unit_status[u] = closed ? "ACTIVE" : "CUTOFF";
    if (!closed) {
      unit_irms[u]    = 0.0f;
      unit_power_w[u] = 0.0f;
    }
  }
  char binStrAll[9];
  relayStateToBinStr(relayState, binStrAll);
  Serial.printf("[RELAY] All -> %s  Register=0b%s\n",
                closed ? "ON" : "OFF", binStrAll);
}

void setAllRelays(bool closed) {
  xSemaphoreTake(dataMutex, portMAX_DELAY);
  setAllRelaysLocked(closed);
  xSemaphoreGive(dataMutex);

  // NVS PERSISTENCE: a bulk relay command (e.g. "shut everything
  // off") is exactly the kind of event that must survive a reset.
  for (uint8_t u = 0; u < NUM_UNITS; u++) {
    saveRelayState(u + 1, closed);
  }
}

void applyRelayCommand(uint8_t u, bool requested) {
  xSemaphoreTake(dataMutex, portMAX_DELAY);
  bool has_credit = unit_balance_kwh[u] > 0.0f;
  bool target     = requested && has_credit;
  bool changed    = (target != unit_relay[u]);   // NVS PERSISTENCE
  if (changed) {
    setRelayLocked(u, target);
  }
  xSemaphoreGive(dataMutex);

  // NVS PERSISTENCE: remote/stream relay commands are rare events -
  // always save immediately, never throttled.
  if (changed) {
    saveRelayState(u + 1, target);
  }
}

// ─────────────────────────────────────────────
//  Combined telemetry + system push — ONE multi-path PATCH to
//  this meter's own subtree (METER_BASE, e.g. "/meters/meter_001").
//  Snapshot taken under the mutex; the HTTP call itself happens
//  with NO mutex held, so it never blocks SensorTask.
//
//  NOTE: "relay_state" here is a STATUS field only - it reports
//  what the hardware is actually doing. Nothing on the device
//  reads this field back (see pollRelayStates() / the SSE handler,
//  which now read "relay_command" instead). Keeping the read and
//  write sides on different keys is what prevents the device from
//  racing against its own status reports.
// ─────────────────────────────────────────────
void pushTelemetryAndSystem() {
  unsigned long ts = millis();

  float  s_irms[NUM_UNITS], s_power[NUM_UNITS], s_consumed[NUM_UNITS], s_pending[NUM_UNITS];
  bool   s_relay[NUM_UNITS];
  String s_status[NUM_UNITS];
  float  s_vsolar, s_vmains, s_rate;
  bool   s_solar_avail, s_mains_avail;
  String s_source;

  xSemaphoreTake(dataMutex, portMAX_DELAY);
  for (uint8_t u = 0; u < NUM_UNITS; u++) {
    s_irms[u]     = unit_irms[u];
    s_power[u]    = unit_power_w[u];
    s_consumed[u] = unit_consumed_kwh[u];
    s_pending[u]  = unit_pending_delta_kwh[u];
    s_relay[u]    = unit_relay[u];
    s_status[u]   = unit_status[u];
  }
  s_vsolar      = v_solar_rms;
  s_vmains      = v_mains_rms;
  s_solar_avail = solar_available;
  s_mains_avail = mains_available;
  s_source      = source_active;
  s_rate        = rate_naira_kwh;
  xSemaphoreGive(dataMutex);

  static char body[2816];
  size_t pos = 0;
  int n;

  n = snprintf(body + pos, sizeof(body) - pos,
    "{"
      "\"system/last_updated\":%lu,"
      "\"system/mains_available\":%s,"
      "\"system/mains_voltage\":%.5f,"
      "\"system/solar_available\":%s,"
      "\"system/solar_voltage\":%.5f,"
      "\"system/source_active\":\"%s\","
      "\"system/rate_naira_kwh\":%.0f",
    ts,
    s_mains_avail ? "true" : "false",
    s_vmains,
    s_solar_avail ? "true" : "false",
    s_vsolar,
    s_source.c_str(),
    s_rate
  );
  if (n > 0) pos += n;

  for (uint8_t u = 0; u < NUM_UNITS; u++) {
    if (pos >= sizeof(body) - 200) {
      Serial.println("[TELEM] Body buffer nearly full - truncating remaining units!");
      break;
    }

    float balanceIncrement = -s_pending[u];
    String prefix = "units/" + String(UNIT_KEY[u]) + "/";

    n = snprintf(body + pos, sizeof(body) - pos,
      ","
      "\"%slast_irms\":%.5f,"
      "\"%slast_power_w\":%.5f,"
      "\"%slast_updated\":%lu,"
      "\"%ssource\":\"%s\","
      "\"%sbalance_kwh\":{\".sv\":{\"increment\":%.5f}},"
      "\"%srelay_state\":%s,"
      "\"%sstatus\":\"%s\","
      "\"%stotal_consumed_kwh\":%.5f",
      prefix.c_str(), s_irms[u],
      prefix.c_str(), s_power[u],
      prefix.c_str(), ts,
      prefix.c_str(), s_source.c_str(),
      prefix.c_str(), balanceIncrement,
      prefix.c_str(), s_relay[u] ? "true" : "false",
      prefix.c_str(), s_status[u].c_str(),
      prefix.c_str(), s_consumed[u]
    );
    if (n > 0) pos += n;
  }

  if (pos < sizeof(body) - 1) {
    n = snprintf(body + pos, sizeof(body) - pos, "}");
    if (n > 0) pos += n;
  }

  // v5: PATCH target is now this meter's own subtree (METER_BASE)
  // instead of the database root (""). The body's keys ("system/...",
  // "units/unit_x/...") are still relative to that target, so the
  // resulting writes land at
  //   /meters/meter_001/system/...
  //   /meters/meter_001/units/unit_x/...
  // exactly mirroring the old root-relative layout, just nested.
  if (firebasePatch(METER_BASE, String(body))) {
    xSemaphoreTake(dataMutex, portMAX_DELAY);
    for (uint8_t u = 0; u < NUM_UNITS; u++) {
      unit_pending_delta_kwh[u] -= s_pending[u];
    }
    xSemaphoreGive(dataMutex);
    Serial.println("[TELEM] Units + system pushed in 1 HTTP call.");
  } else {
    Serial.println("[TELEM] Push failed - pending deltas retained for retry.");
  }
}

void loadBalancesAndRate() {
  String rate_str = firebaseGet(String(METER_BASE) + "/system/rate_naira_kwh");
  rate_str.trim();
  float new_rate = rate_naira_kwh;
  bool  got_rate = false;
  if (rate_str != "null" && rate_str.length()) {
    new_rate = rate_str.toFloat();
    got_rate = true;
  }

  String unitsJson = firebaseGet(String(METER_BASE) + "/units");
  if (unitsJson == "null" || unitsJson.length() < 5) {
    Serial.println("[FB] Failed to read /units");
    if (got_rate) {
      xSemaphoreTake(dataMutex, portMAX_DELAY);
      rate_naira_kwh = new_rate;
      xSemaphoreGive(dataMutex);
      saveGlobalRate(new_rate);   // NVS PERSISTENCE
      Serial.printf("[FB] Rate: %.0f NGN/kWh\n", new_rate);
    }
    return;
  }

  static DynamicJsonDocument doc(4096);
  doc.clear();
  DeserializationError err = deserializeJson(doc, unitsJson);
  if (err) {
    Serial.printf("[FB] JSON parse error: %s\n", err.c_str());
    return;
  }

  xSemaphoreTake(dataMutex, portMAX_DELAY);
  if (got_rate) rate_naira_kwh = new_rate;

  for (uint8_t u = 0; u < NUM_UNITS; u++) {
    const char* key = UNIT_KEY[u];
    if (!doc.containsKey(key)) continue;

    JsonObject unit = doc[key];
    if (unit.containsKey("balance_kwh")) {
      unit_balance_kwh[u] = unit["balance_kwh"].as<float>();
    }
    if (unit.containsKey("status")) {
      unit_status[u] = unit["status"].as<String>();
    }
  }
  xSemaphoreGive(dataMutex);

  if (got_rate) {
    saveGlobalRate(new_rate);   // NVS PERSISTENCE
    Serial.printf("[FB] Rate: %.0f NGN/kWh\n", new_rate);
  }
  for (uint8_t u = 0; u < NUM_UNITS; u++) {
    Serial.printf("[FB] %s  bal=%.4fkWh  status=%s\n",
                  UNIT_KEY[u], unit_balance_kwh[u], unit_status[u].c_str());
  }
}

// ─────────────────────────────────────────────
//  pollRelayStates() — FIX: reads "relay_command" (backend/app
//  owned, desired state) instead of "relay_state" (device owned,
//  actual state). Previously this read back the device's own
//  status field, which could still hold a stale value between
//  the 2 s poll cycle and the 5 s telemetry push - causing a
//  just-restored (e.g. TOPUP'd) relay to get immediately flipped
//  back off by this function reading its own old report.
// ─────────────────────────────────────────────
void pollRelayStates() {
  String unitsJson = firebaseGet(String(METER_BASE) + "/units");
  if (unitsJson == "null" || unitsJson.length() < 5) return;

  static DynamicJsonDocument doc(4096);
  doc.clear();
  DeserializationError err = deserializeJson(doc, unitsJson);
  if (err) {
    Serial.printf("[FB] JSON parse error (relay poll): %s\n", err.c_str());
    return;
  }

  for (uint8_t u = 0; u < NUM_UNITS; u++) {
    const char* key = UNIT_KEY[u];
    if (!doc.containsKey(key)) continue;

    JsonObject unit = doc[key];
    if (!unit.containsKey("relay_command")) continue;   // was "relay_state"

    bool requested = unit["relay_command"].as<bool>();
    applyRelayCommand(u, requested);
  }
}

// ─────────────────────────────────────────────
//  Helpers
// ─────────────────────────────────────────────
String detectSource(float v_solar, float v_mains) {
  if (v_mains > 50.0f) return "mains";
  if (v_solar > 50.0f) return "solar";
  return "none";
}

float estimatePF(float irms) {
  if (irms < 0.05f) return 1.0f;
  if (irms < 1.0f)  return 0.90f;
  if (irms < 5.0f)  return 0.88f;
  return 0.85f;
}

bool wifiEraseRequested = false;   // decided ONCE at boot, never touched again

// Checked ONCE in setup(), never again during runtime reconnects.
// Requires the pin held LOW for a full 2 seconds (debounced)
// before it counts. Previously, connectWiFi() re-checked this pin
// on EVERY reconnect attempt, including background reconnects
// triggered by WIFI_CHECK_INTERVAL_MS - so any noise/glitch on the
// pin at any point during runtime could silently erase your saved
// WiFi credentials, which is what was forcing you back into setup
// mode after it had worked fine initially.
void checkWifiResetRequestAtBoot() {
  if (digitalRead(WIFI_RESET_PIN) != LOW) return;

  Serial.println("[WiFi] Reset pin LOW at boot - hold for 2s to confirm erase...");
  unsigned long start = millis();
  while (millis() - start < 2000) {
    if (digitalRead(WIFI_RESET_PIN) != LOW) {
      Serial.println("[WiFi] Reset not confirmed (released early) - keeping saved credentials.");
      return;
    }
    delay(20);
  }
  Serial.println("[WiFi] Reset confirmed - saved WiFi credentials will be erased.");
  wifiEraseRequested = true;
}

// ─────────────────────────────────────────────
//  WiFi + WiFiManager
// ─────────────────────────────────────────────
bool connectWiFi() {
  // Erase only happens once, if checkWifiResetRequestAtBoot() set
  // the flag during setup() - never re-checked here on subsequent
  // reconnects, so a noisy/glitchy pin can no longer wipe saved
  // credentials mid-session.
  if (wifiEraseRequested) {
    Serial.println("[WiFi] Erasing saved credentials (one-time, requested at boot).");
    WiFiManager wmReset;
    wmReset.resetSettings();
    wifiEraseRequested = false;   // only ever do this once
  }

  WiFiManager wm;
  wm.setConfigPortalTimeout(WIFI_PORTAL_TIMEOUT_S);
  wm.setConnectTimeout(WIFI_CONNECT_TIMEOUT_S);

  Serial.println("[WiFi] Connecting (or opening setup portal if unconfigured)...");
  bool ok = strlen(WIFI_AP_PASSWORD)
              ? wm.autoConnect(WIFI_AP_NAME, WIFI_AP_PASSWORD)
              : wm.autoConnect(WIFI_AP_NAME);

  if (!ok) {
    Serial.println("[WiFi] autoConnect failed/timed out. Will retry.");
    return false;
  }

  // Power-save (modem sleep) delays/drops packets - kill it for a
  // "real-time" link. Costs a bit more current draw.
  WiFi.setSleep(false);

  Serial.printf("[WiFi] Connected. SSID=%s  IP=%s  RSSI=%d dBm  FreeHeap=%u\n",
                WiFi.SSID().c_str(), WiFi.localIP().toString().c_str(), WiFi.RSSI(),
                ESP.getFreeHeap());
  return true;
}

// ─────────────────────────────────────────────
//  Firebase REST layer over HTTPClient (replaces SIM800 AT+HTTP)
// ─────────────────────────────────────────────
// ─────────────────────────────────────────────
//  Secure-client helper
//  This board has NO PSRAM (see boot log: "PSRAM chip not found")
//  AND is running an Arduino-ESP32 3.x core, where WiFiClientSecure
//  is an alias for NetworkClientSecure - a rewritten class that,
//  unlike the old 2.x WiFiClientSecure, does NOT expose manual TLS
//  buffer sizing at all. So on this exact board+core combination
//  there is no supported way to shrink a TLS session's memory
//  footprint - each one costs the full ~35-50KB regardless.
//  With ~250-300KB total heap and no PSRAM, that means this board
//  can only reliably run ONE secure connection at a time. See
//  ENABLE_REALTIME_STREAM below for what that means in practice.
// ─────────────────────────────────────────────
#define MIN_FREE_HEAP_FOR_HTTP  30000   // skip an HTTP attempt below this - fail fast, don't fragment heap further

void configureSecureClient(WiFiClientSecure& client) {
  client.setInsecure();
}

bool heapOKForHttp(const char* who) {
  uint32_t freeHeap = ESP.getFreeHeap();
  if (freeHeap < MIN_FREE_HEAP_FOR_HTTP) {
    Serial.printf("[HEAP] %s skipped - only %u bytes free (need %u)\n",
                  who, freeHeap, MIN_FREE_HEAP_FOR_HTTP);
    return false;
  }
  return true;
}

String fbURL(const String& path) {
  String p = path;
  if (p.length() == 0 || p[0] != '/') p = "/" + p;
  return "https://" + String(FIREBASE_HOST) + p + ".json?auth=" + String(FIREBASE_AUTH);
}

String firebaseGet(const String& path) {
  if (WiFi.status() != WL_CONNECTED) { blinkFail(); return "null"; }
  if (!heapOKForHttp("GET")) { blinkFail(); return "null"; }

  WiFiClientSecure client;
  configureSecureClient(client);
  client.setTimeout(5000);

  HTTPClient https;
  https.setConnectTimeout(5000);
  https.setTimeout(5000);
  String url = fbURL(path);
  Serial.printf("[HTTP] GET %s\n", path.c_str());

  if (!https.begin(client, url)) {
    Serial.println("[HTTP] begin() failed");
    blinkFail();
    return "null";
  }

  int status = https.GET();
  String body = "null";
  if (status == 200) {
    body = https.getString();
    body.trim();
    blinkReceived();
  } else {
    Serial.printf("[HTTP] GET status %d\n", status);
    blinkFail();
  }
  https.end();
  return body;
}

bool firebasePut(const String& path, const String& body) {
  if (WiFi.status() != WL_CONNECTED) { blinkFail(); return false; }
  if (!heapOKForHttp("PUT")) { blinkFail(); return false; }

  WiFiClientSecure client;
  configureSecureClient(client);
  client.setTimeout(5000);

  HTTPClient https;
  https.setConnectTimeout(5000);
  https.setTimeout(5000);
  String url = fbURL(path);
  Serial.printf("[HTTP] PUT %s (%d bytes)\n", path.c_str(), body.length());

  if (!https.begin(client, url)) {
    Serial.println("[HTTP] begin() failed");
    blinkFail();
    return false;
  }
  https.addHeader("Content-Type", "application/json");

  int status = https.PUT((uint8_t*)body.c_str(), body.length());
  https.end();

  if (status != 200) {
    Serial.printf("[HTTP] PUT status %d\n", status);
    blinkFail();
    return false;
  }
  blinkSent();
  return true;
}

bool firebasePatch(const String& path, const String& body) {
  if (WiFi.status() != WL_CONNECTED) { blinkFail(); return false; }
  if (!heapOKForHttp("PATCH")) { blinkFail(); return false; }

  WiFiClientSecure client;
  configureSecureClient(client);
  client.setTimeout(5000);

  HTTPClient https;
  https.setConnectTimeout(5000);
  https.setTimeout(5000);
  String url = fbURL(path);
  Serial.printf("[HTTP] PATCH %s (%d bytes)\n", path.c_str(), body.length());

  if (!https.begin(client, url)) {
    Serial.println("[HTTP] begin() failed");
    blinkFail();
    return false;
  }
  https.addHeader("Content-Type", "application/json");

  int status = https.PATCH((uint8_t*)body.c_str(), body.length());
  https.end();

  if (status != 200) {
    Serial.printf("[HTTP] PATCH status %d\n", status);
    blinkFail();
    return false;
  }
  blinkSent();
  return true;
}

// ─────────────────────────────────────────────
//  StreamTask — real-time relay commands via Firebase's REST
//  streaming API (Server-Sent Events). Holds ONE persistent HTTPS
//  connection to this meter's own /units node
//  (METER_BASE + "/units.json") with "Accept: text/event-stream".
//  Firebase pushes an event the instant any unit's data changes -
//  no polling delay. pollRelayStates() in CommsTask still runs
//  every 30 s as a fallback in case this connection drops silently.
// ─────────────────────────────────────────────
void applyRelayFromVariant(const char* key, JsonVariant unitOrBool) {
  for (uint8_t u = 0; u < NUM_UNITS; u++) {
    if (strcmp(UNIT_KEY[u], key) != 0) continue;
    if (unitOrBool.is<bool>()) {
      applyRelayCommand(u, unitOrBool.as<bool>());
    } else if (unitOrBool.is<JsonObject>()) {
      JsonObject obj = unitOrBool.as<JsonObject>();
      // FIX: read "relay_command" (backend-owned desired state),
      // not "relay_state" (device-owned status). See note above
      // pollRelayStates() for why this matters.
      if (obj.containsKey("relay_command")) {
        applyRelayCommand(u, obj["relay_command"].as<bool>());
      }
    }
    return;
  }
}

// path is the Firebase-relative path from the SSE "data" payload,
// relative to the node we're streaming (METER_BASE + "/units"),
// e.g. "/", "/unit_3", or "/unit_3/relay_command".
void handleUnitsPushEvent(const String& path, JsonVariant data) {
  if (path == "/") {
    // Full resync of /units - data is an object keyed by unit id.
    if (!data.is<JsonObject>()) return;
    JsonObject root = data.as<JsonObject>();
    for (JsonPair kv : root) {
      applyRelayFromVariant(kv.key().c_str(), kv.value());
    }
    return;
  }

  // path looks like "/unit_3" or "/unit_3/relay_command"
  String p = path;
  if (p.startsWith("/")) p.remove(0, 1);
  int slash = p.indexOf('/');
  String unitKey = (slash >= 0) ? p.substring(0, slash) : p;
  String rest     = (slash >= 0) ? p.substring(slash + 1) : "";

  if (rest.length() == 0) {
    // Whole unit object replaced/patched
    applyRelayFromVariant(unitKey.c_str(), data);
  } else if (rest == "relay_command") {   // FIX: was "relay_state"
    applyRelayFromVariant(unitKey.c_str(), data);
  }
  // other sub-fields (balance_kwh, relay_state status, etc.) are
  // intentionally ignored here - loadBalancesAndRate() in CommsTask
  // owns balance_kwh, and relay_state is device-written status only.
}

void streamTask(void* pv) {
  static DynamicJsonDocument streamDoc(2048);

  for (;;) {
    if (WiFi.status() != WL_CONNECTED) {
      vTaskDelay(pdMS_TO_TICKS(2000));
      continue;
    }

    WiFiClientSecure client;
    configureSecureClient(client);
    client.setTimeout(STREAM_IDLE_TIMEOUT_MS);

    // v5: stream this meter's own /units node, not the DB root's.
    String url = String(METER_BASE) + "/units.json?auth=" + String(FIREBASE_AUTH);
    Serial.println("[STREAM] Connecting...");

    if (!client.connect(FIREBASE_HOST, 443)) {
      Serial.println("[STREAM] TLS connect failed - retrying in 5 s.");
      blinkFail();
      vTaskDelay(pdMS_TO_TICKS(5000));
      continue;
    }

    client.print(String("GET ") + url + " HTTP/1.1\r\n" +
                 "Host: " + FIREBASE_HOST + "\r\n" +
                 "Accept: text/event-stream\r\n" +
                 "Connection: keep-alive\r\n\r\n");

    // Skip HTTP status line + headers
    bool gotHeaders = false;
    unsigned long headerWait = millis();
    while (client.connected() && millis() - headerWait < 8000) {
      String line = client.readStringUntil('\n');
      if (line == "\r" || line.length() == 0) { gotHeaders = true; break; }
    }
    if (!gotHeaders) {
      Serial.println("[STREAM] No response headers - reconnecting.");
      client.stop();
      blinkFail();
      vTaskDelay(pdMS_TO_TICKS(3000));
      continue;
    }
    Serial.println("[STREAM] Connected - listening for real-time events.");

    String eventType = "";
    unsigned long lastActivity = millis();

    while (client.connected()) {
      if (millis() - lastActivity > STREAM_IDLE_TIMEOUT_MS) {
        Serial.println("[STREAM] Idle timeout - reconnecting.");
        break;
      }

      if (!client.available()) {
        vTaskDelay(pdMS_TO_TICKS(20));
        continue;
      }

      String line = client.readStringUntil('\n');
      lastActivity = millis();
      line.trim();

      if (line.length() == 0) {
        eventType = "";     // blank line = end of one SSE message
        continue;
      }
      if (line.startsWith(":")) continue;   // SSE comment/keep-alive

      if (line.startsWith("event:")) {
        eventType = line.substring(6);
        eventType.trim();
        continue;
      }

      if (line.startsWith("data:")) {
        String payload = line.substring(5);
        payload.trim();

        if (eventType == "put" || eventType == "patch") {
          streamDoc.clear();
          DeserializationError err = deserializeJson(streamDoc, payload);
          if (!err) {
            String path = streamDoc["path"].as<String>();
            JsonVariant data = streamDoc["data"];
            handleUnitsPushEvent(path, data);
            blinkReceived();
            Serial.printf("[STREAM] %s %s -> relay command applied\n",
                          eventType.c_str(), path.c_str());
          } else {
            Serial.printf("[STREAM] JSON parse error: %s\n", err.c_str());
          }
        } else if (eventType == "cancel" || eventType == "auth_revoked") {
          Serial.println("[STREAM] Server cancelled stream - reconnecting.");
          break;
        }
        // "keep-alive" events: just refresh lastActivity, nothing to parse
      }
    }

    client.stop();
    vTaskDelay(pdMS_TO_TICKS(1000));   // brief pause before reconnecting
  }
}
