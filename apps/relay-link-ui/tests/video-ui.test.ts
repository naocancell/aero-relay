import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

const app = readFileSync(new URL("../src/App.vue", import.meta.url), "utf8");

test("video configuration is role-specific and camera URLs are masked", () => {
  assert.match(app, /function enableRelayVideo\(/);
  assert.match(app, /function enableServerVideo\(/);
  assert.match(app, /v-model="form\.relay\.video\.source_url"[^>]*type="password"/);
  assert.match(app, /v-model="form\.relay\.video\.publish_url"[^>]*type="password"/);
  assert.match(app, /v-model="form\.server\.video\.stream_path"/);
});

test("video worker state is visible without exposing its configured URLs", () => {
  assert.match(app, /videoStateLabel\(video\.state\)/);
  assert.match(app, /video\.last_error/);
  assert.doesNotMatch(app, /\{\{\s*form\.relay\.video\.source_url\s*\}\}/);
  assert.doesNotMatch(app, /\{\{\s*form\.relay\.video\.publish_url\s*\}\}/);
});
