#!/bin/bash
#
# CasparCG Self-Test Runner
#
# Usage:
#   ./run_tests.sh              # Run all tests
#   ./run_tests.sh --phase 3    # Run specific phase
#   ./run_tests.sh --test color # Run specific test
#   ./run_tests.sh --list       # List available tests
#

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Check Python
if ! command -v python3 &> /dev/null; then
    echo "Error: python3 is required"
    exit 1
fi

# Check ffprobe (needed for video analysis)
if ! command -v ffprobe &> /dev/null; then
    echo "Warning: ffprobe not found - video analysis will be limited"
fi

# Create output directory
mkdir -p test_output

# Run tests
python3 test_runner.py "$@"
