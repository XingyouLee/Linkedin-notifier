#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

APP_NAME="LinkedinNotifierApp"
BUNDLE_ID="local.levi.LinkedinNotifierApp"
VERSION="0.1.0"
BUILD_DIR="${APP_ROOT}/.build"
DIST_DIR="${APP_ROOT}/dist"
APP_BUNDLE="${DIST_DIR}/${APP_NAME}.app"
CONTENTS_DIR="${APP_BUNDLE}/Contents"
MACOS_DIR="${CONTENTS_DIR}/MacOS"
RESOURCES_DIR="${CONTENTS_DIR}/Resources"
FRAMEWORKS_DIR="${CONTENTS_DIR}/Frameworks"
PROJECT_ROOT="${APP_ROOT}"
ICON_SOURCE="${APP_ROOT}/Sources/LinkedinNotifierApp/Resources/AppIcon.png"
ICON_NAME="AppIcon"

cd "${APP_ROOT}"

while [[ "${PROJECT_ROOT}" != "/" && ! -f "${PROJECT_ROOT}/.astro/config.yaml" ]]; do
  PROJECT_ROOT="$(dirname "${PROJECT_ROOT}")"
done

if [[ ! -f "${PROJECT_ROOT}/.astro/config.yaml" ]]; then
  echo "Could not locate the Astro project root from ${APP_ROOT}." >&2
  exit 1
fi

swift build --disable-sandbox -c release

EXECUTABLE_PATH="$(find "${BUILD_DIR}" -path "*/release/${APP_NAME}" -type f | head -n 1)"

if [[ ! -x "${EXECUTABLE_PATH}" ]]; then
  echo "Could not find the built executable for ${APP_NAME}." >&2
  exit 1
fi

if [[ ! -f "${ICON_SOURCE}" ]]; then
  echo "Could not find icon source at ${ICON_SOURCE}." >&2
  exit 1
fi

ICON_WORK_DIR="$(mktemp -d "${BUILD_DIR}/appicon.XXXXXX")"
ICONSET_DIR="${ICON_WORK_DIR}/${ICON_NAME}.iconset"
mkdir -p "${ICONSET_DIR}"
trap 'rm -rf "${ICON_WORK_DIR}"' EXIT

for SIZE in 16 32 128 256 512; do
  sips -z "${SIZE}" "${SIZE}" "${ICON_SOURCE}" --out "${ICONSET_DIR}/icon_${SIZE}x${SIZE}.png" >/dev/null
done

sips -z 32 32 "${ICON_SOURCE}" --out "${ICONSET_DIR}/icon_16x16@2x.png" >/dev/null
sips -z 64 64 "${ICON_SOURCE}" --out "${ICONSET_DIR}/icon_32x32@2x.png" >/dev/null
sips -z 256 256 "${ICON_SOURCE}" --out "${ICONSET_DIR}/icon_128x128@2x.png" >/dev/null
sips -z 512 512 "${ICON_SOURCE}" --out "${ICONSET_DIR}/icon_256x256@2x.png" >/dev/null
sips -z 1024 1024 "${ICON_SOURCE}" --out "${ICONSET_DIR}/icon_512x512@2x.png" >/dev/null

iconutil -c icns "${ICONSET_DIR}" -o "${BUILD_DIR}/${ICON_NAME}.icns"

rm -rf "${APP_BUNDLE}"
mkdir -p "${MACOS_DIR}" "${RESOURCES_DIR}" "${FRAMEWORKS_DIR}"

cp "${EXECUTABLE_PATH}" "${MACOS_DIR}/${APP_NAME}"
cp "${BUILD_DIR}/${ICON_NAME}.icns" "${RESOURCES_DIR}/${ICON_NAME}.icns"

cat > "${CONTENTS_DIR}/Info.plist" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>CFBundleDevelopmentRegion</key>
    <string>en</string>
    <key>CFBundleExecutable</key>
    <string>${APP_NAME}</string>
    <key>CFBundleIdentifier</key>
    <string>${BUNDLE_ID}</string>
    <key>CFBundleInfoDictionaryVersion</key>
    <string>6.0</string>
    <key>CFBundleIconFile</key>
    <string>${ICON_NAME}.icns</string>
    <key>CFBundleName</key>
    <string>${APP_NAME}</string>
    <key>CFBundlePackageType</key>
    <string>APPL</string>
    <key>CFBundleShortVersionString</key>
    <string>${VERSION}</string>
    <key>CFBundleVersion</key>
    <string>${VERSION}</string>
    <key>LSMinimumSystemVersion</key>
    <string>14.0</string>
    <key>NSHighResolutionCapable</key>
    <true/>
    <key>NSPrincipalClass</key>
    <string>NSApplication</string>
    <key>LinkedinNotifierProjectPath</key>
    <string>${PROJECT_ROOT}</string>
</dict>
</plist>
EOF

if command -v codesign >/dev/null 2>&1; then
  codesign --force --deep --sign - "${APP_BUNDLE}" >/dev/null
fi

echo "Built app bundle:"
echo "${APP_BUNDLE}"
