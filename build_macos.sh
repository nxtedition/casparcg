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
        --jobs|-j)
            JOBS="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--clean|-c] [--verbose|-v] [--jobs|-j N]"
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
echo "Configuring with CMake..."
if [[ $VERBOSE -eq 1 ]]; then
    cmake "${SRC_DIR}"
else
    cmake "${SRC_DIR}" 2>&1 | grep -E "^--|Found|Error|Warning|==="
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
