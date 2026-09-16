/*
 * preferences_manager.cpp
 * See preferences_manager.h for the API contract this implements.
 */

#include "preferences_manager.h"
#include <Preferences.h>
#include <math.h>

static Preferences prefs;

// NVS namespace names are capped at 15 chars by the ESP-IDF API.
static const char* NVS_NAMESPACE = "meterdata";

// Ceiling on how many units this module will track. The firmware
// currently uses 7 (NUM_UNITS); this is set a bit higher so the
// same module keeps working if that ever grows, without needing
// to touch this file.
#define MAX_UNITS_SUPPORTED 16

// ── Checkpoint throttling ────────────────────────────────────
// sensorTask() calls checkpointUnitBalance()/checkpointEnergy()
// on every pass (as often as every ~10 ms once the sensor read
// settles). Writing to flash that often would wear out the NVS
// sector fast. A checkpoint only actually hits flash once EITHER
// enough time has passed OR the value has drifted enough since
// the last flash write - whichever comes first. Immediate,
// un-throttled saves (cutoff, top-up, remote relay command) go
// through saveUnitBalance()/saveRelayState() instead, bypassing
// this entirely.
#define CHECKPOINT_MIN_INTERVAL_MS   60000UL  // at most once a minute per field, per unit
#define BALANCE_DRIFT_THRESHOLD_KWH  0.01f    // ...or sooner if balance moved this much
#define ENERGY_DRIFT_THRESHOLD_KWH   0.01f    // ...or sooner if energy moved this much
// ──────────────────────────────────────────────────────────────

static float         lastSavedBalance[MAX_UNITS_SUPPORTED]  = {0};
static float         lastSavedEnergy[MAX_UNITS_SUPPORTED]   = {0};
static unsigned long lastBalanceSaveMs[MAX_UNITS_SUPPORTED] = {0};
static unsigned long lastEnergySaveMs[MAX_UNITS_SUPPORTED]  = {0};
static bool          haveBaseline[MAX_UNITS_SUPPORTED]      = {false};

static bool unitNumberValid(uint8_t unitNumber) {
  return unitNumber >= 1 && unitNumber <= MAX_UNITS_SUPPORTED;
}

// NVS keys are capped at 15 chars too, but these are tiny -
// "b"/"e"/"r" + unit number, e.g. "b1".."b16".
static void keyFor(const char* prefix, uint8_t unitNumber, char* out, size_t outLen) {
  snprintf(out, outLen, "%s%u", prefix, unitNumber);
}

bool initializeStorage() {
  // false = open read/write (not read-only).
  bool ok = prefs.begin(NVS_NAMESPACE, false);
  if (!ok) {
    Serial.println("[NVS] Preferences.begin() failed - flash storage unavailable.");
  }
  return ok;
}

bool loadUnitData(uint8_t unitNumber, UnitPersistentData &out) {
  if (!unitNumberValid(unitNumber)) {
    out.balance_kwh = 0.0f;
    out.total_energy_kwh = 0.0f;
    out.relay_state = false;
    return false;
  }

  char kBal[8], kErg[8], kRly[8];
  keyFor("b", unitNumber, kBal, sizeof(kBal));
  keyFor("e", unitNumber, kErg, sizeof(kErg));
  keyFor("r", unitNumber, kRly, sizeof(kRly));

  bool hasAny = prefs.isKey(kBal) || prefs.isKey(kErg) || prefs.isKey(kRly);

  out.balance_kwh      = prefs.getFloat(kBal, 0.0f);
  out.total_energy_kwh = prefs.getFloat(kErg, 0.0f);
  out.relay_state      = prefs.getBool(kRly, false);

  // Seed the checkpoint-throttling baseline with whatever we just
  // loaded, so the very first post-boot checkpoint call doesn't
  // immediately think the value has "drifted" from zero.
  uint8_t idx = unitNumber - 1;
  lastSavedBalance[idx]  = out.balance_kwh;
  lastSavedEnergy[idx]   = out.total_energy_kwh;
  lastBalanceSaveMs[idx] = millis();
  lastEnergySaveMs[idx]  = millis();
  haveBaseline[idx]      = true;

  return hasAny;
}

void saveUnitBalance(uint8_t unitNumber, float balance_kwh) {
  if (!unitNumberValid(unitNumber)) return;

  char kBal[8];
  keyFor("b", unitNumber, kBal, sizeof(kBal));
  prefs.putFloat(kBal, balance_kwh);

  uint8_t idx = unitNumber - 1;
  lastSavedBalance[idx]  = balance_kwh;
  lastBalanceSaveMs[idx] = millis();
  haveBaseline[idx]      = true;
}

void saveRelayState(uint8_t unitNumber, bool relay_state) {
  if (!unitNumberValid(unitNumber)) return;

  char kRly[8];
  keyFor("r", unitNumber, kRly, sizeof(kRly));
  prefs.putBool(kRly, relay_state);
}

void checkpointUnitBalance(uint8_t unitNumber, float balance_kwh) {
  if (!unitNumberValid(unitNumber)) return;
  uint8_t idx = unitNumber - 1;

  if (!haveBaseline[idx]) {
    // No baseline yet (e.g. loadUnitData() was never called for
    // this unit) - save once now to establish one.
    saveUnitBalance(unitNumber, balance_kwh);
    return;
  }

  unsigned long now = millis();
  bool intervalElapsed = (now - lastBalanceSaveMs[idx]) >= CHECKPOINT_MIN_INTERVAL_MS;
  bool driftedEnough   = fabsf(balance_kwh - lastSavedBalance[idx]) >= BALANCE_DRIFT_THRESHOLD_KWH;

  if (intervalElapsed || driftedEnough) {
    saveUnitBalance(unitNumber, balance_kwh);
  }
}

void checkpointEnergy(uint8_t unitNumber, float total_energy_kwh) {
  if (!unitNumberValid(unitNumber)) return;
  uint8_t idx = unitNumber - 1;

  unsigned long now = millis();
  bool intervalElapsed = (now - lastEnergySaveMs[idx]) >= CHECKPOINT_MIN_INTERVAL_MS;
  bool driftedEnough   = fabsf(total_energy_kwh - lastSavedEnergy[idx]) >= ENERGY_DRIFT_THRESHOLD_KWH;

  if (!haveBaseline[idx] || intervalElapsed || driftedEnough) {
    char kErg[8];
    keyFor("e", unitNumber, kErg, sizeof(kErg));
    prefs.putFloat(kErg, total_energy_kwh);
    lastSavedEnergy[idx]  = total_energy_kwh;
    lastEnergySaveMs[idx] = now;
  }
}

float getGlobalRate() {
  return prefs.getFloat("rate", 200.0f);   // 200.0f matches rate_naira_kwh's default in the .ino
}

void saveGlobalRate(float rate_naira_kwh) {
  prefs.putFloat("rate", rate_naira_kwh);
}
