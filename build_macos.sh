#!/bin/bash
# CasparCG macOS Build Script
# This script builds CasparCG for macOS with Vulkan backend

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${SCRIPT_DIR}/build"
SRC_DIR="${SCRIPT_DIR}/src"

# Parse arguments
CLEAN_BUILD=0
VERBOSE=0
ENABLE_HTML=ON
JOBS=$(sysctl -n hw.ncpu)

while [[ $# -gt 0 ]]; do
    case $1 in
        --clean|-c)
            CLEAN_BUILD=1
            shift
            ;;
        --verbose|-v)
            VERBOSE=1
            shift
            ;;
        --with-html)
            ENABLE_HTML=ON
            shift
            ;;
        --no-html)
            ENABLE_HTML=OFF
            shift
            ;;
        --jobs|-j)
            JOBS="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--clean|-c] [--verbose|-v] [--jobs|-j N] [--with-html] [--no-html]"
            exit 1
            ;;
    esac
done

echo "=== CasparCG macOS Build ==="
echo "Build directory: ${BUILD_DIR}"
echo "Source directory: ${SRC_DIR}"
echo "Parallel jobs: ${JOBS}"
echo ""

# Clean build if requested
if [[ $CLEAN_BUILD -eq 1 ]]; then
    echo "Cleaning build directory..."
    rm -rf "${BUILD_DIR}"
fi

# Create build directory
mkdir -p "${BUILD_DIR}"
cd "${BUILD_DIR}"

# Configure with CMake
echo "Configuring with CMake (HTML/CEF: ${ENABLE_HTML})..."
if [[ $VERBOSE -eq 1 ]]; then
    cmake "${SRC_DIR}" -DENABLE_HTML=${ENABLE_HTML}
else
    cmake "${SRC_DIR}" -DENABLE_HTML=${ENABLE_HTML} 2>&1 | grep -E "^--|Found|Error|Warning|===|CEF"
fi

# Build
echo ""
echo "Building with ${JOBS} parallel jobs..."
if [[ $VERBOSE -eq 1 ]]; then
    make -j${JOBS}
else
    make -j${JOBS} 2>&1 | grep -E "^\[|Error|error:|warning:"
fi

# Copy macOS run script to build directory
cp "${SRC_DIR}/shell/run_macos.sh" "${BUILD_DIR}/shell/"
chmod +x "${BUILD_DIR}/shell/run_macos.sh"

echo ""
echo "=== Build Complete ==="
echo "Binary: ${BUILD_DIR}/shell/casparcg"
echo "Run script: ${BUILD_DIR}/shell/run_macos.sh"
