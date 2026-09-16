#pragma once
/*
 * preferences_manager.h
 * ─────────────────────────────────────────────────────────────
 * Thin wrapper around the ESP32 "Preferences" (NVS) library.
 * The main firmware NEVER touches Preferences directly - it only
 * calls the functions declared here. That keeps flash-write
 * policy (what gets throttled, what gets saved immediately) in
 * one place, separate from the relay/sensor/Firebase logic.
 *
 * Per-unit data lives under short keys ("b1".."b7" for balance,
 * "e1".."e7" for total energy, "r1".."r7" for relay state) inside
 * a single NVS namespace, plus one global "rate" key for the
 * tariff. Unit numbers are 1-based (apartment numbers 1-7), to
 * match how the .ino already calls these functions (i + 1).
 * ─────────────────────────────────────────────────────────────
 */

#include <Arduino.h>

// Snapshot of one unit's persisted state, as recovered from flash.
struct UnitPersistentData {
  float balance_kwh;
  float total_energy_kwh;
  bool  relay_state;
};

// Bring up NVS storage. Call once in setup(), before any
// load/save/checkpoint call. Returns false if the underlying
// Preferences.begin() failed (e.g. corrupt NVS partition) - the
// firmware falls back to RAM-only defaults in that case.
bool initializeStorage();

// Recover a single unit's last-known balance/energy/relay state.
// Returns true if ANY persisted data was found for this unit
// (false on a first-ever boot / freshly erased flash, in which
// case 'out' is filled with safe zero/false defaults).
bool loadUnitData(uint8_t unitNumber, UnitPersistentData &out);

// Immediate, un-throttled writes. Used for events that must never
// be lost: a confirmed cutoff, a confirmed top-up, a remote/stream
// relay command, a bulk "all relays off" command.
void saveUnitBalance(uint8_t unitNumber, float balance_kwh);
void saveRelayState(uint8_t unitNumber, bool relay_state);

// Throttled writes. Safe/cheap to call every sensorTask loop -
// each call is just a RAM comparison against the last value
// actually written to flash. Only touches flash once enough time
// has passed OR the value has drifted enough since the last save
// (see the thresholds in preferences_manager.cpp), so continuous
// per-loop calls don't wear out the NVS flash sector.
void checkpointUnitBalance(uint8_t unitNumber, float balance_kwh);
void checkpointEnergy(uint8_t unitNumber, float total_energy_kwh);

// Global tariff (NGN per kWh), shared across all units.
float getGlobalRate();
void  saveGlobalRate(float rate_naira_kwh);
