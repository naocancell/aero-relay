export function shouldLoadConfigAfterIpcReconnect(
  ipcConnected: boolean,
  configLoaded: boolean,
  configLoadInFlight: boolean,
): boolean {
  return ipcConnected && !configLoaded && !configLoadInFlight;
}
