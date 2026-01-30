# CasparCG - Claude Code Guidelines

## Project Overview

CasparCG is a professional broadcast graphics and video playout server written in C++17 with CMake.

## Testing Principles

### Self-Test Framework

Automated tests in `tests/selftest/` verify CasparCG functionality via the AMCP protocol.

**Run tests after making changes:**
```bash
cd tests/selftest
./run_tests.sh               # Run all tests
./run_tests.sh --phase 3     # Test specific phase
./run_tests.sh --test <name> # Run single test
./run_tests.sh --list        # List available tests
```

### Test-Driven Development

1. **Before implementing:** Check if tests exist, add if missing
2. **After implementing:** Run relevant tests to verify
3. **When fixing bugs:** Add a test that reproduces the issue first

### Adding New Tests

1. Add test method to `tests/selftest/test_runner.py`
2. Register with `self.register_test(name, phase, method, description)`
3. Use `VideoAnalyzer` for automated verification (color sampling, frame counting)
4. Prefer automated verification over visual inspection

### Test Verification Methods

- **AMCP Response Codes:** Success = 200-202, Client error = 400-404, Server error = 500+
- **Video Analysis:** Use `ffprobe` via `VideoAnalyzer` class
- **Color Sampling:** Extract average colors from recorded frames
- **Frame Counting:** Verify duration matches expected frame count

## AMCP Protocol

CasparCG is controlled via TCP text protocol on port 5250.

```bash
# Quick test
echo "VERSION" | nc localhost 5250

# Common commands
PLAY 1-1 COLOR RED          # Play solid red on channel 1, layer 1
MIXER 1-1 OPACITY 0.5       # Set layer opacity
INFO 1                      # Get channel info
CLEAR 1                     # Clear all layers
```

## Code Conventions

- `snake_case` for functions and variables
- `PascalCase` for classes
- Keep rendering code isolated in `src/accelerator/`
- Follow patterns in adjacent code

## Key Directories

- `src/accelerator/` - GPU rendering backends
- `src/core/` - Core business logic
- `src/protocol/amcp/` - AMCP command handlers
- `src/modules/` - Producers and consumers (ffmpeg, decklink, etc.)
- `tests/selftest/` - Automated test suite

## Building on macOS

**ALWAYS use the build script for macOS builds:**
```bash
./build_macos.sh           # Normal build
./build_macos.sh --clean   # Clean build from scratch
./build_macos.sh --verbose # Verbose output
```

### Required Dependencies (install via Homebrew)
```bash
brew install boost ffmpeg tbb simde
# Vulkan SDK should be installed from https://vulkan.lunarg.com
```

### macOS Build Notes
- Uses Vulkan backend (via MoltenVK) instead of OpenGL
- Screen consumer disabled (Phase 9)
- CEF/HTML module disabled (Phase 14)
- OSD diagnostics disabled (uses stub)
