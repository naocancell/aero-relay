import assert from "node:assert/strict";
import test from "node:test";
import { shouldLoadConfigAfterIpcReconnect } from "../src/startup.ts";

test("reloads missing config when IPC becomes available", () => {
  assert.equal(shouldLoadConfigAfterIpcReconnect(true, false, false), true);
});

test("does not reload config while IPC is unavailable", () => {
  assert.equal(shouldLoadConfigAfterIpcReconnect(false, false, false), false);
});

test("does not reload an already loaded config", () => {
  assert.equal(shouldLoadConfigAfterIpcReconnect(true, true, false), false);
});

test("does not start a duplicate config request", () => {
  assert.equal(shouldLoadConfigAfterIpcReconnect(true, false, true), false);
});
