# Managed media tools

The project pins MediaMTX to `v1.18.2`; Windows and Linux releases must use the
same tag with the platform and CPU architecture-specific executable.

The release process stages the audited third-party executables here before the
NSIS bundle is built:

```text
media/ffmpeg/ffmpeg.exe
media/mediamtx/mediamtx.exe
```

`relay-linkd` starts these paths directly and owns their lifetime. Do not add a
second service, scheduled task, or start-up shortcut for either executable.

Use `deploy/windows/stage-media-tools.ps1` to copy approved binaries and their
sibling DLL dependencies into this folder for a video-enabled release.
