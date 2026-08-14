!include LogicLib.nsh
!include nsDialogs.nsh

Var Dialog
Var Hwnd_GroupBoxRole
Var Hwnd_RadioServer
Var Hwnd_RadioRelay
Var Hwnd_GroupBoxService
Var Hwnd_CheckRegisterService
Var Hwnd_CheckAutoStart

Var RelayLinkRole
Var RelayLinkRegisterService
Var RelayLinkAutoStart

!define RelayLinkPowerShell "$WINDIR\System32\WindowsPowerShell\v1.0\powershell.exe"
!define RelayLinkServiceScript "$INSTDIR\installer\install-relay-linkd.ps1"

Function OnRegisterServiceClicked
  Pop $0
  ${NSD_GetState} $Hwnd_CheckRegisterService $1
  ${If} $1 == ${BST_CHECKED}
    EnableWindow $Hwnd_CheckAutoStart 1
    ${NSD_Check} $Hwnd_CheckAutoStart
  ${Else}
    ${NSD_Uncheck} $Hwnd_CheckAutoStart
    EnableWindow $Hwnd_CheckAutoStart 0
  ${EndIf}
FunctionEnd

Function RelayLinkPageCreate
  !insertmacro MUI_HEADER_TEXT "运行模式与服务设置" "请选择 Relay Link 的运行角色和后台服务选项。"
  
  nsDialogs::Create 1018
  Pop $Dialog
  ${If} $Dialog == error
    Abort
  ${EndIf}

  ; 初次打开时设置默认值
  ${If} $RelayLinkRole == ""
    StrCpy $RelayLinkRole "server"
  ${EndIf}
  ${If} $RelayLinkRegisterService == ""
    StrCpy $RelayLinkRegisterService "1"
  ${EndIf}
  ${If} $RelayLinkAutoStart == ""
    StrCpy $RelayLinkAutoStart "1"
  ${EndIf}

  ; === 运行模式分组 ===
  ${NSD_CreateGroupBox} 10u 5u 280u 52u "运行角色"
  Pop $Hwnd_GroupBoxRole

  ${NSD_CreateRadioButton} 20u 20u 260u 12u "服务端 (Server) - 作为中心控制节点接收无人机数据"
  Pop $Hwnd_RadioServer

  ${NSD_CreateRadioButton} 20u 36u 260u 12u "中继站 (Relay) - 作为中继节点转发数据"
  Pop $Hwnd_RadioRelay

  ; === 后台服务配置分组 ===
  ${NSD_CreateGroupBox} 10u 62u 280u 55u "后台服务设置"
  Pop $Hwnd_GroupBoxService

  ${NSD_CreateCheckbox} 20u 77u 260u 12u "注册为 Windows 系统服务 (RelayLinkD)"
  Pop $Hwnd_CheckRegisterService

  ${NSD_CreateCheckbox} 20u 94u 260u 12u "开机自动启动后台服务"
  Pop $Hwnd_CheckAutoStart

  ; === 设置回显状态 ===
  ${If} $RelayLinkRole == "relay"
    ${NSD_Check} $Hwnd_RadioRelay
  ${Else}
    ${NSD_Check} $Hwnd_RadioServer
  ${EndIf}

  ${If} $RelayLinkRegisterService == "0"
    ${NSD_Uncheck} $Hwnd_CheckRegisterService
    EnableWindow $Hwnd_CheckAutoStart 0
  ${Else}
    ${NSD_Check} $Hwnd_CheckRegisterService
    EnableWindow $Hwnd_CheckAutoStart 1
  ${EndIf}

  ${If} $RelayLinkAutoStart == "0"
    ${NSD_Uncheck} $Hwnd_CheckAutoStart
  ${Else}
    ${NSD_Check} $Hwnd_CheckAutoStart
  ${EndIf}

  ${NSD_OnClick} $Hwnd_CheckRegisterService OnRegisterServiceClicked

  nsDialogs::Show
FunctionEnd

Function RelayLinkPageLeave
  ${NSD_GetState} $Hwnd_RadioServer $0
  ${If} $0 == ${BST_CHECKED}
    StrCpy $RelayLinkRole "server"
  ${Else}
    StrCpy $RelayLinkRole "relay"
  ${EndIf}

  ${NSD_GetState} $Hwnd_CheckRegisterService $0
  ${If} $0 == ${BST_CHECKED}
    StrCpy $RelayLinkRegisterService "1"
  ${Else}
    StrCpy $RelayLinkRegisterService "0"
  ${EndIf}

  ${NSD_GetState} $Hwnd_CheckAutoStart $0
  ${If} $0 == ${BST_CHECKED}
    StrCpy $RelayLinkAutoStart "1"
  ${Else}
    StrCpy $RelayLinkAutoStart "0"
  ${EndIf}
FunctionEnd

; 将自定义配置页面挂载到安装向导中
Page custom RelayLinkPageCreate RelayLinkPageLeave

!macro NSIS_HOOK_PREINSTALL
  IfFileExists "${RelayLinkServiceScript}" 0 no_previous_service
  nsExec::ExecToLog '"${RelayLinkPowerShell}" -NoProfile -NonInteractive -ExecutionPolicy Bypass -File "${RelayLinkServiceScript}" -UninstallService'
  Pop $0
no_previous_service:
!macroend

!macro NSIS_HOOK_POSTINSTALL
  ${If} $RelayLinkRegisterService == "0"
    nsExec::ExecToLog '"${RelayLinkPowerShell}" -NoProfile -NonInteractive -ExecutionPolicy Bypass -File "${RelayLinkServiceScript}" -InstallRoot "$INSTDIR" -ResourceRoot "$INSTDIR" -Role "$RelayLinkRole" -NoServiceRegistration'
  ${Else}
    ${If} $RelayLinkAutoStart == "1"
      nsExec::ExecToLog '"${RelayLinkPowerShell}" -NoProfile -NonInteractive -ExecutionPolicy Bypass -File "${RelayLinkServiceScript}" -InstallRoot "$INSTDIR" -ResourceRoot "$INSTDIR" -Role "$RelayLinkRole" -StartType "auto"'
    ${Else}
      nsExec::ExecToLog '"${RelayLinkPowerShell}" -NoProfile -NonInteractive -ExecutionPolicy Bypass -File "${RelayLinkServiceScript}" -InstallRoot "$INSTDIR" -ResourceRoot "$INSTDIR" -Role "$RelayLinkRole" -StartType "demand"'
    ${EndIf}
  ${EndIf}
  Pop $0
  ${If} $0 != 0
    MessageBox MB_ICONSTOP "Relay Link 安装及配置初始化失败。请联系维护人员。"
    Abort
  ${EndIf}
!macroend

!macro NSIS_HOOK_PREUNINSTALL
  nsExec::ExecToLog '"${RelayLinkPowerShell}" -NoProfile -NonInteractive -ExecutionPolicy Bypass -File "${RelayLinkServiceScript}" -UninstallService'
  Pop $0
!macroend
