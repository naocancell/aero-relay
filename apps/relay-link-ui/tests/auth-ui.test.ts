import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

const app = readFileSync(new URL("../src/App.vue", import.meta.url), "utf8");
const api = readFileSync(new URL("../src/api.ts", import.meta.url), "utf8");

test("authFetch explicitly sets X-Relay-Auth header to prevent browser cached Basic Auth interference", () => {
  assert.match(api, /headers\.set\("X-Relay-Auth",\s*currentAuthHeader\)/);
  assert.match(api, /headers\.set\("X-Relay-Auth",\s*"none"\)/);
  assert.match(api, /credentials:\s*"same-origin"/);
});

test("logout stops background tasks, clears session data, and prompts login when auth is enabled", () => {
  assert.match(app, /async function handleLogout\(\)\s*\{[\s\S]*?stopAllBackgroundTasks\(\);[\s\S]*?clearAuthCredentials\(\);[\s\S]*?clearSessionData\(\);/);
  assert.match(app, /if\s*\(!info\s*\|\|\s*info\.auth_enabled\)\s*\{[\s\S]*?openLoginModal\(\);/);
});

test("initial session check halts background polling before login when auth is enabled", () => {
  assert.match(app, /if\s*\(info\?\.auth_enabled\s*&&\s*!isSignedIn\.value\)\s*\{[\s\S]*?openLoginModal\(\);[\s\S]*?return;\s*\}/);
});

test("login modal defaults to empty username and does not close on backdrop click", () => {
  assert.match(app, /const loginUsername = ref\(""\);/);
  assert.match(app, /function openLoginModal\(\)\s*\{\s*loginUsername\.value = "";/);
  assert.match(app, /<div v-if="showLoginModal" class="modal-backdrop">/);
  assert.doesNotMatch(app, /modal-backdrop[^>]*@click\.self/);
});

test("targetAuth selects server.auth when activeRole is server even if relay section is present", () => {
  assert.match(app, /if\s*\(activeRole\.value\s*===\s*"server"\s*&&\s*form\.value\?\.server\)/);
  assert.match(app, /if\s*\(activeRole\.value\s*===\s*"relay"\s*&&\s*form\.value\?\.relay\)/);
});


