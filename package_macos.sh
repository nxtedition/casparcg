#!/bin/bash
#
# CasparCG macOS Packaging Script
#
# Creates a macOS .app bundle from the build output, with optional:
#   - NDI library bundling
#   - Code signing
#   - DMG creation
#   - Notarization
#
# Usage:
#   ./package_macos.sh                              # Create unsigned app bundle
#   ./package_macos.sh --include-ndi                # Include NDI library
#   ./package_macos.sh --dmg                        # Create DMG
#   ./package_macos.sh --sign --identity "..."      # Sign the app
#   ./package_macos.sh --notarize --apple-id "..." --team-id "..." --password "..."
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Configuration
APP_NAME="CasparCG"
BUNDLE_ID="com.casparcg.server"
BUILD_DIR="build"
OUTPUT_DIR="dist"

# Parse version from CMakeLists.txt
VERSION_MAJOR=$(grep "CONFIG_VERSION_MAJOR" src/CMakeLists.txt | head -1 | sed 's/.*CONFIG_VERSION_MAJOR \([0-9]*\).*/\1/')
VERSION_MINOR=$(grep "CONFIG_VERSION_MINOR" src/CMakeLists.txt | head -1 | sed 's/.*CONFIG_VERSION_MINOR \([0-9]*\).*/\1/')
VERSION_BUG=$(grep "CONFIG_VERSION_BUG" src/CMakeLists.txt | head -1 | sed 's/.*CONFIG_VERSION_BUG \([0-9]*\).*/\1/')
VERSION="${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_BUG}"

# Get git hash
if [ -d ".git" ]; then
    GIT_HASH=$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")
else
    GIT_HASH="unknown"
fi

# Options
INCLUDE_NDI=false
CREATE_DMG=false
SIGN_APP=false
NOTARIZE=false
SIGNING_IDENTITY=""
APPLE_ID=""
TEAM_ID=""
PASSWORD=""
NDI_LIB_PATH=""

# Print usage
usage() {
    echo "Usage: $0 [options]"
    echo ""
    echo "Options:"
    echo "  --include-ndi [path]     Include NDI library (optional: specify path to libndi.dylib)"
    echo "  --dmg                    Create DMG disk image"
    echo "  --sign                   Sign the app bundle"
    echo "  --identity \"...\"         Code signing identity (Developer ID Application: ...)"
    echo "  --notarize               Notarize the app (requires --sign and Apple credentials)"
    echo "  --apple-id \"...\"         Apple ID for notarization"
    echo "  --team-id \"...\"          Team ID for notarization"
    echo "  --password \"...\"         App-specific password or @keychain:name"
    echo "  -h, --help               Show this help"
    echo ""
    echo "Examples:"
    echo "  $0                                   # Create unsigned app bundle"
    echo "  $0 --include-ndi --dmg              # Bundle NDI and create DMG"
    echo "  $0 --sign --identity \"Developer ID Application: My Name (TEAMID)\""
    echo "  $0 --sign --notarize --identity \"...\" --apple-id \"...\" --team-id \"...\" --password \"@keychain:AC_PASSWORD\""
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --include-ndi)
            INCLUDE_NDI=true
            # Check if next argument is a path (not starting with -)
            if [[ -n "$2" && ! "$2" =~ ^- ]]; then
                NDI_LIB_PATH="$2"
                shift
            fi
            shift
            ;;
        --dmg)
            CREATE_DMG=true
            shift
            ;;
        --sign)
            SIGN_APP=true
            shift
            ;;
        --identity)
            SIGNING_IDENTITY="$2"
            shift 2
            ;;
        --notarize)
            NOTARIZE=true
            shift
            ;;
        --apple-id)
            APPLE_ID="$2"
            shift 2
            ;;
        --team-id)
            TEAM_ID="$2"
            shift 2
            ;;
        --password)
            PASSWORD="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

# Validate options
if [ "$SIGN_APP" = true ] && [ -z "$SIGNING_IDENTITY" ]; then
    echo "Error: --sign requires --identity"
    exit 1
fi

if [ "$NOTARIZE" = true ]; then
    if [ "$SIGN_APP" = false ]; then
        echo "Error: --notarize requires --sign"
        exit 1
    fi
    if [ -z "$APPLE_ID" ] || [ -z "$TEAM_ID" ] || [ -z "$PASSWORD" ]; then
        echo "Error: --notarize requires --apple-id, --team-id, and --password"
        exit 1
    fi
fi

# Check build exists
if [ ! -f "$BUILD_DIR/shell/casparcg" ]; then
    echo "Error: Build not found. Run ./build_macos.sh first."
    exit 1
fi

if [ ! -d "$BUILD_DIR/Frameworks/Chromium Embedded Framework.framework" ]; then
    echo "Error: CEF framework not found in build."
    exit 1
fi

echo "========================================"
echo "CasparCG macOS Packaging"
echo "========================================"
echo "Version: $VERSION ($GIT_HASH)"
echo "Include NDI: $INCLUDE_NDI"
echo "Create DMG: $CREATE_DMG"
echo "Sign: $SIGN_APP"
echo "Notarize: $NOTARIZE"
echo ""

# Create output directory
mkdir -p "$OUTPUT_DIR"

# App bundle paths
APP_BUNDLE="$OUTPUT_DIR/$APP_NAME.app"
CONTENTS="$APP_BUNDLE/Contents"
MACOS="$CONTENTS/MacOS"
FRAMEWORKS="$CONTENTS/Frameworks"
RESOURCES="$CONTENTS/Resources"

# Remove existing bundle
if [ -d "$APP_BUNDLE" ]; then
    echo "Removing existing app bundle..."
    rm -rf "$APP_BUNDLE"
fi

echo "Creating app bundle structure..."
mkdir -p "$MACOS"
mkdir -p "$MACOS/data"
mkdir -p "$MACOS/media"
mkdir -p "$MACOS/template"
mkdir -p "$MACOS/log"
mkdir -p "$FRAMEWORKS"
mkdir -p "$RESOURCES"
mkdir -p "$RESOURCES/data"
mkdir -p "$RESOURCES/media"
mkdir -p "$RESOURCES/template"

# Copy main executable
echo "Copying executable..."
cp "$BUILD_DIR/shell/casparcg" "$MACOS/"

# Copy CEF framework
echo "Copying CEF framework..."
cp -R "$BUILD_DIR/Frameworks/Chromium Embedded Framework.framework" "$FRAMEWORKS/"

# Copy config file (to both MacOS and Resources for flexibility)
echo "Copying configuration..."
if [ -f "$BUILD_DIR/shell/casparcg.config" ]; then
    cp "$BUILD_DIR/shell/casparcg.config" "$MACOS/"
    cp "$BUILD_DIR/shell/casparcg.config" "$RESOURCES/"
fi

# Copy data files if they exist (excluding cache directories)
if [ -d "$BUILD_DIR/shell/data" ]; then
    # Use rsync to exclude cache directories
    rsync -a --exclude='cef_cache' --exclude='*.log' "$BUILD_DIR/shell/data/" "$RESOURCES/data/" 2>/dev/null || true
fi

# Copy media files if they exist (to MacOS dir where CasparCG expects them)
if [ -d "$BUILD_DIR/shell/media" ]; then
    cp -R "$BUILD_DIR/shell/media/"* "$MACOS/media/" 2>/dev/null || true
    cp -R "$BUILD_DIR/shell/media/"* "$RESOURCES/media/" 2>/dev/null || true
fi

# Copy template files if they exist (to MacOS dir where CasparCG expects them)
if [ -d "$BUILD_DIR/shell/template" ]; then
    cp -R "$BUILD_DIR/shell/template/"* "$MACOS/template/" 2>/dev/null || true
    cp -R "$BUILD_DIR/shell/template/"* "$RESOURCES/template/" 2>/dev/null || true
fi

# Copy font for OSD (if it exists) - needed in MacOS dir
if [ -f "$BUILD_DIR/shell/LiberationMono-Regular.ttf" ]; then
    cp "$BUILD_DIR/shell/LiberationMono-Regular.ttf" "$MACOS/"
    cp "$BUILD_DIR/shell/LiberationMono-Regular.ttf" "$RESOURCES/"
fi

# Include NDI if requested
if [ "$INCLUDE_NDI" = true ]; then
    echo "Including NDI library..."

    # Find NDI library
    if [ -n "$NDI_LIB_PATH" ] && [ -f "$NDI_LIB_PATH" ]; then
        NDI_SOURCE="$NDI_LIB_PATH"
    elif [ -f "/usr/local/lib/libndi.dylib" ]; then
        NDI_SOURCE="/usr/local/lib/libndi.dylib"
    elif [ -f "/Library/NDI SDK for Apple/lib/macOS/libndi.dylib" ]; then
        NDI_SOURCE="/Library/NDI SDK for Apple/lib/macOS/libndi.dylib"
    else
        echo "Warning: NDI library not found. Skipping NDI bundling."
        echo "  Install NDI SDK or specify path with --include-ndi /path/to/libndi.dylib"
        INCLUDE_NDI=false
    fi

    if [ "$INCLUDE_NDI" = true ]; then
        cp "$NDI_SOURCE" "$FRAMEWORKS/"
        echo "  Copied: $NDI_SOURCE"
    fi
fi

# Create Info.plist
echo "Creating Info.plist..."
cat > "$CONTENTS/Info.plist" << EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>CFBundleDevelopmentRegion</key>
    <string>en</string>
    <key>CFBundleExecutable</key>
    <string>casparcg</string>
    <key>CFBundleIdentifier</key>
    <string>$BUNDLE_ID</string>
    <key>CFBundleInfoDictionaryVersion</key>
    <string>6.0</string>
    <key>CFBundleName</key>
    <string>$APP_NAME</string>
    <key>CFBundleDisplayName</key>
    <string>CasparCG Server</string>
    <key>CFBundlePackageType</key>
    <string>APPL</string>
    <key>CFBundleShortVersionString</key>
    <string>$VERSION</string>
    <key>CFBundleVersion</key>
    <string>$VERSION.$GIT_HASH</string>
    <key>LSMinimumSystemVersion</key>
    <string>10.15</string>
    <key>NSHighResolutionCapable</key>
    <true/>
    <key>NSSupportsAutomaticGraphicsSwitching</key>
    <true/>
    <key>NSPrincipalClass</key>
    <string>NSApplication</string>
    <key>LSApplicationCategoryType</key>
    <string>public.app-category.video</string>
    <key>NSHumanReadableCopyright</key>
    <string>Copyright CasparCG Project. Licensed under GPLv3.</string>
</dict>
</plist>
EOF

# Create entitlements file for signing
ENTITLEMENTS_FILE="$OUTPUT_DIR/CasparCG.entitlements"
cat > "$ENTITLEMENTS_FILE" << EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>com.apple.security.cs.allow-jit</key>
    <true/>
    <key>com.apple.security.cs.allow-unsigned-executable-memory</key>
    <true/>
    <key>com.apple.security.cs.disable-library-validation</key>
    <true/>
    <key>com.apple.security.network.server</key>
    <true/>
    <key>com.apple.security.network.client</key>
    <true/>
    <key>com.apple.security.device.camera</key>
    <true/>
    <key>com.apple.security.device.audio-input</key>
    <true/>
</dict>
</plist>
EOF

# Fix library paths with install_name_tool
echo "Fixing library paths..."

# Fix CEF framework install name in main executable
install_name_tool -change \
    "@rpath/Chromium Embedded Framework.framework/Chromium Embedded Framework" \
    "@executable_path/../Frameworks/Chromium Embedded Framework.framework/Chromium Embedded Framework" \
    "$MACOS/casparcg" 2>/dev/null || true

# Also try the absolute path pattern
install_name_tool -change \
    "/Library/Frameworks/Chromium Embedded Framework.framework/Chromium Embedded Framework" \
    "@executable_path/../Frameworks/Chromium Embedded Framework.framework/Chromium Embedded Framework" \
    "$MACOS/casparcg" 2>/dev/null || true

# Fix NDI if bundled
if [ "$INCLUDE_NDI" = true ] && [ -f "$FRAMEWORKS/libndi.dylib" ]; then
    # Update the install name of the NDI library itself
    install_name_tool -id "@executable_path/../Frameworks/libndi.dylib" "$FRAMEWORKS/libndi.dylib" 2>/dev/null || true
fi

# Code signing
if [ "$SIGN_APP" = true ]; then
    echo ""
    echo "Signing app bundle..."
    echo "Identity: $SIGNING_IDENTITY"

    # Sign frameworks first (inside-out signing)
    echo "  Signing CEF framework..."
    codesign --deep --force --options runtime \
        --entitlements "$ENTITLEMENTS_FILE" \
        --sign "$SIGNING_IDENTITY" \
        "$FRAMEWORKS/Chromium Embedded Framework.framework"

    # Sign NDI if bundled
    if [ "$INCLUDE_NDI" = true ] && [ -f "$FRAMEWORKS/libndi.dylib" ]; then
        echo "  Signing NDI library..."
        codesign --force --options runtime \
            --sign "$SIGNING_IDENTITY" \
            "$FRAMEWORKS/libndi.dylib"
    fi

    # Sign main executable
    echo "  Signing main executable..."
    codesign --force --options runtime \
        --entitlements "$ENTITLEMENTS_FILE" \
        --sign "$SIGNING_IDENTITY" \
        "$MACOS/casparcg"

    # Sign the bundle
    echo "  Signing app bundle..."
    codesign --force --options runtime \
        --entitlements "$ENTITLEMENTS_FILE" \
        --sign "$SIGNING_IDENTITY" \
        "$APP_BUNDLE"

    # Verify signature
    echo ""
    echo "Verifying signature..."
    codesign --verify --deep --strict --verbose=2 "$APP_BUNDLE"

    echo ""
    echo "Checking Gatekeeper assessment..."
    spctl --assess --type execute --verbose=2 "$APP_BUNDLE" || echo "Note: Gatekeeper may require notarization for full approval"
fi

# Create DMG
DMG_PATH=""
if [ "$CREATE_DMG" = true ]; then
    echo ""
    echo "Creating DMG..."
    DMG_NAME="CasparCG-${VERSION}-macOS"
    DMG_PATH="$OUTPUT_DIR/$DMG_NAME.dmg"

    # Remove existing DMG
    rm -f "$DMG_PATH"

    # Create DMG
    hdiutil create -volname "$APP_NAME" \
        -srcfolder "$APP_BUNDLE" \
        -ov -format UDZO \
        "$DMG_PATH"

    # Sign DMG if signing is enabled
    if [ "$SIGN_APP" = true ]; then
        echo "Signing DMG..."
        codesign --force --sign "$SIGNING_IDENTITY" "$DMG_PATH"
    fi

    echo "DMG created: $DMG_PATH"
fi

# Notarization
if [ "$NOTARIZE" = true ]; then
    echo ""
    echo "Submitting for notarization..."

    if [ -z "$DMG_PATH" ]; then
        # Create a ZIP for notarization if no DMG
        ZIP_PATH="$OUTPUT_DIR/CasparCG-${VERSION}-macOS.zip"
        ditto -c -k --keepParent "$APP_BUNDLE" "$ZIP_PATH"
        NOTARIZE_PATH="$ZIP_PATH"
    else
        NOTARIZE_PATH="$DMG_PATH"
    fi

    echo "Submitting: $NOTARIZE_PATH"

    # Submit for notarization and wait
    xcrun notarytool submit "$NOTARIZE_PATH" \
        --apple-id "$APPLE_ID" \
        --team-id "$TEAM_ID" \
        --password "$PASSWORD" \
        --wait

    # Staple the ticket
    echo ""
    echo "Stapling notarization ticket..."
    if [ -n "$DMG_PATH" ]; then
        xcrun stapler staple "$DMG_PATH"
    else
        xcrun stapler staple "$APP_BUNDLE"
    fi

    echo ""
    echo "Verifying notarization..."
    if [ -n "$DMG_PATH" ]; then
        spctl --assess --type install --verbose=4 "$DMG_PATH"
    else
        spctl --assess --type execute --verbose=4 "$APP_BUNDLE"
    fi
fi

# Clean up
rm -f "$ENTITLEMENTS_FILE"

echo ""
echo "========================================"
echo "Packaging complete!"
echo "========================================"
echo ""
echo "App bundle: $APP_BUNDLE"
if [ -n "$DMG_PATH" ]; then
    echo "DMG: $DMG_PATH"
fi
echo ""
echo "To run the app:"
echo "  open $APP_BUNDLE"
echo ""
echo "Or from terminal:"
echo "  $APP_BUNDLE/Contents/MacOS/casparcg"
