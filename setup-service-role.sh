#!/usr/bin/env bash
exec bash "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/deploy/linux/setup-service-role.sh" "$@"
