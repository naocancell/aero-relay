!include LogicLib.nsh

Var RelayLinkRole

!define RelayLinkPowerShell "$WINDIR\System32\WindowsPowerShell\v1.0\powershell.exe"
!define RelayLinkServiceScript "$INSTDIR\installer\install-relay-linkd.ps1"

!macro NSIS_HOOK_PREINSTALL
  MessageBox MB_ICONQUESTION|MB_YESNO|MB_DEFBUTTON1 "请选择运行模式：$\r$\n$\r$\n点击“是”安装服务端。$\r$\n点击“否”安装中继站。" IDYES server IDNO relay
server:
  StrCpy $RelayLinkRole "server"
  Goto role_selected
relay:
  StrCpy $RelayLinkRole "relay"
role_selected:
  IfFileExists "${RelayLinkServiceScript}" 0 no_previous_service
  nsExec::ExecToLog '"${RelayLinkPowerShell}" -NoProfile -NonInteractive -ExecutionPolicy Bypass -File "${RelayLinkServiceScript}" -UninstallService'
  Pop $0
no_previous_service:
!macroend

!macro NSIS_HOOK_POSTINSTALL
  ${If} $RelayLinkRole == "server"
    nsExec::ExecToLog '"${RelayLinkPowerShell}" -NoProfile -NonInteractive -ExecutionPolicy Bypass -File "${RelayLinkServiceScript}" -InstallRoot "$INSTDIR" -ResourceRoot "$INSTDIR" -Role "server"'
  ${Else}
    nsExec::ExecToLog '"${RelayLinkPowerShell}" -NoProfile -NonInteractive -ExecutionPolicy Bypass -File "${RelayLinkServiceScript}" -InstallRoot "$INSTDIR" -ResourceRoot "$INSTDIR" -Role "relay"'
  ${EndIf}
  Pop $0
  ${If} $0 != 0
    MessageBox MB_ICONSTOP "Relay Link 后台服务安装失败。请联系维护人员。"
    Abort
  ${EndIf}
!macroend

!macro NSIS_HOOK_PREUNINSTALL
  nsExec::ExecToLog '"${RelayLinkPowerShell}" -NoProfile -NonInteractive -ExecutionPolicy Bypass -File "${RelayLinkServiceScript}" -UninstallService'
  Pop $0
!macroend
