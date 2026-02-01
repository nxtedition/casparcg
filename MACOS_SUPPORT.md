# CasparCG macOS Support

This document describes the macOS support implementation using Vulkan (via MoltenVK).

## Status: Feature Complete

All core phases are complete. CasparCG runs on macOS with Vulkan rendering.

**Test Results:** 43 passed, 1 failed (ProRes encoding - depends on FFmpeg build)
- Includes HTML/CEF tests (Phase 14)

## Architecture

- **Rendering:** Vulkan via MoltenVK (translates to Metal)
- **Window:** GLFW with Vulkan surface
- **Audio:** Core Audio (AudioQueue Services)
- **Shaders:** SPIR-V compute shaders compiled from GLSL

### Directory Structure

```
src/accelerator/vk/           # Vulkan backend
├── image/
│   ├── image_kernel.cpp/h    # Rendering operations
│   ├── image_mixer.cpp/h     # Frame composition
│   └── shaders/
│       ├── blend.comp        # Blend modes, transforms, effects
│       ├── screen.vert       # Screen display vertex shader
│       └── screen.frag       # Screen display fragment shader
└── util/
    ├── buffer.cpp/h          # GPU buffer management
    ├── device.cpp/h          # Vulkan device + async dispatch
    ├── matrix.cpp/h          # Transform matrix utilities
    ├── pipeline.cpp/h        # Compute pipeline for blending
    ├── render_pipeline.cpp/h # Graphics pipeline for display
    ├── swapchain.cpp/h       # Vulkan swapchain management
    ├── texture.cpp/h         # Texture/image management
    └── vk_check.h            # Error checking macros
```

---

## Building on macOS

### Prerequisites

```bash
# Install Homebrew dependencies
brew install boost ffmpeg tbb simde glfw

# Install Vulkan SDK from https://vulkan.lunarg.com
# Ensure VULKAN_SDK environment variable is set
```

### Build

```bash
./tools/macos/build.sh             # Normal build
./tools/macos/build.sh --clean     # Clean build from scratch
./tools/macos/build.sh --verbose   # Verbose output
./tools/macos/build.sh --package   # Build and create .app bundle
```

### Run

```bash
cd build/shell
./run_macos.sh
# Or directly:
./casparcg casparcg.config
```

---

## Phase Summary

| Phase | Description | Status |
|-------|-------------|--------|
| 1 | Build System & Foundation | Complete |
| 2 | Vulkan Device & Context | Complete |
| 3 | Basic Playout + Layering | Complete |
| 4 | Blend Modes (29 modes) | Complete |
| 5 | Geometric Transforms | Complete |
| 6 | Color Processing & Effects | Complete |
| 7 | Pixel Formats & Color Spaces | Complete |
| 8 | Core Producers | Complete |
| 9 | Screen Consumer | Complete |
| 10 | File Output (FFmpeg Consumer) | Complete |
| 11 | Audio (Core Audio) | Complete |
| 12 | Hardware I/O (DeckLink) | Complete (requires hardware) |
| 13 | Network I/O (NDI) | Complete |
| 14 | HTML/CEF Templates | Complete |
| 15 | Integration & Testing | Complete |

---

## Platform-Specific Implementation Details

### Main Loop & GCD Integration

macOS GLFW requires the main thread for Cocoa operations. The screen consumer uses `dispatch_async` to marshal GLFW calls to the main queue. To ensure GCD blocks are processed, the main loop uses NSApplication event processing:

**Files:**
- [macos_main_loop.mm](src/shell/macos_main_loop.mm) - NSApplication event processing
- [main.cpp:174-195](src/shell/main.cpp#L174-L195) - macOS-specific main loop

```cpp
// main.cpp - macOS runs ASIO on background thread, main thread processes Cocoa events
#ifdef __APPLE__
    auto work_guard = boost::asio::make_work_guard(io);
    std::thread asio_thread([&io] { io.run(); });
    while (!io.stopped()) {
        macos_process_events(0.01);  // NSApplication event loop
    }
    work_guard.reset();
    asio_thread.join();
#else
    io.run();
#endif
```

### Core Audio Consumer

Uses AudioQueue Services for audio output (replacing OpenAL on Linux/Windows):

**Files:**
- [coreaudio_consumer.mm](src/modules/oal/consumer/coreaudio_consumer.mm)

### Screen Consumer (Vulkan)

GLFW window with Vulkan swapchain, separate from OpenGL implementation:

**Files:**
- [screen_consumer_vk.mm](src/modules/screen/consumer/screen_consumer_vk.mm)

---

## Module Support

| Module | Status | Notes |
|--------|--------|-------|
| color_producer | Working | Solid colors |
| image_producer | Working | PNG, JPEG, TIFF, BMP |
| ffmpeg_producer | Working | Video files |
| route_producer | Working | Channel routing |
| transition_producer | Working | CUT, MIX, PUSH, SLIDE, WIPE |
| sting_producer | Working | Overlay transitions |
| screen_consumer | Working | GLFW + Vulkan |
| ffmpeg_consumer | Working | H.264, ProRes*, MOV, streaming |
| image_consumer | Working | PNG snapshots |
| system-audio | Working | Core Audio |
| decklink | Working | Requires Desktop Video |
| ndi | Working | Requires NDI SDK |
| html | Working | CEF 131 with Metal backend |
| flash | N/A | Discontinued |
| bluefish | N/A | Windows-only |

*ProRes encoding requires FFmpeg built with prores_ks encoder

---

## HTML/CEF (Phase 14)

CEF (Chromium Embedded Framework) 131 is enabled on macOS using a non-bundle deployment:

### Implementation Details

- **CEF Version:** 131.4.1 (Chromium 131)
- **Rendering Backend:** Metal via ANGLE
- **Framework Location:** `Frameworks/Chromium Embedded Framework.framework`
- **Subprocess Handling:** Main binary handles all subprocess types via `CefExecuteProcess()`

### Key Configuration (html.cpp)

```cpp
// Framework and resources inside the bundle
settings.framework_dir_path = "<exe>/../Frameworks/Chromium Embedded Framework.framework"
settings.resources_dir_path = "<framework>/Resources"
settings.browser_subprocess_path = "<exe>"  // Main binary handles subprocesses
```

### Build Options

```bash
./tools/macos/build.sh              # Build with HTML/CEF (default)
./tools/macos/build.sh --no-html    # Build without HTML/CEF
```

### Notes

- For distribution, code signing may be required
- GPU rendering can be enabled via `configuration.html.enable-gpu`
- Remote debugging available via `configuration.html.remote-debugging-port`

---

## Testing

### Self-Test Framework

```bash
cd tests/selftest
./run_tests.sh               # Run all tests
./run_tests.sh --phase 3     # Run specific phase
./run_tests.sh --test <name> # Run single test
./run_tests.sh --list        # List available tests
```

### Test Results (Current)

```
Total: 43 passed, 1 failed
- recording_prores: FAILED (ProRes codec not available in FFmpeg)
- All other tests: PASSED
```

### Key Tests

| Test | Phase | Verifies |
|------|-------|----------|
| build_verification | 1 | Binary runs, AMCP responds |
| color_playback | 3 | Solid colors render |
| blend_modes | 4 | All 29 blend modes |
| transforms | 5 | FILL, ROTATION, CLIP, etc. |
| chroma_key | 6 | Green/blue screen keying |
| video_playback | 8 | FFmpeg producer |
| screen_output | 9 | Screen consumer displays |
| recording | 10 | FFmpeg consumer records |
| audio_consumer | 11 | Core Audio works |
| ndi_consumer | 13 | NDI output works |
| stress_layers | 15 | 20 layer stress test |
| stress_rapid | 15 | ~6000 cmd/s throughput |

---

## Runtime Dependencies

### Required
- **Vulkan SDK** (via MoltenVK) - for rendering

### Optional
- **Blackmagic Desktop Video** - for DeckLink devices
  - Download from https://www.blackmagicdesign.com/support
  - Installs `/Library/Frameworks/DeckLinkAPI.framework`

- **NDI SDK v6+** - for NDI streaming
  - Download from https://ndi.video/tools/
  - Or: http://ndi.link/NDIRedistV6Apple

---

## Known Limitations

1. **ProRes encoding** - Depends on FFmpeg build configuration
2. **Edge anti-aliasing** - Not implemented (deferred)
3. **Keyer modes** - Internal/external key deferred
4. **HTML/CEF code signing** - May be required for Gatekeeper on distributed builds

---

## Troubleshooting

### "poll() timed out waiting for main thread"

The main thread isn't processing GCD events. Ensure `macos_process_events()` is called in the main loop.

### Screen window doesn't appear

Check that GLFW initialized correctly and the main thread is processing Cocoa events.

### No audio output

Verify Core Audio consumer is registered:
```
ADD 1 AUDIO
```

### DeckLink not found

Install Blackmagic Desktop Video from https://www.blackmagicdesign.com/support

### NDI not available

Install NDI SDK from https://ndi.video/tools/

---

## Contributing

When adding new features:

1. Follow existing Vulkan patterns in `src/accelerator/vk/`
2. Add tests to `tests/selftest/test_runner.py`
3. Use compute shaders for GPU operations
4. Handle macOS-specific threading with `dispatch_async` for GLFW calls
