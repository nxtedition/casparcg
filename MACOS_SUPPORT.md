# CasparCG macOS Support Implementation Plan

This document outlines the phased approach to adding macOS support to CasparCG using the Vulkan SDK (via MoltenVK).

## Background

CasparCG currently uses **OpenGL 4.5** as its rendering backend. macOS deprecated OpenGL in 2018 and only supports up to OpenGL 4.1, making a direct port impossible. The solution is to use **Vulkan via MoltenVK**, which translates Vulkan API calls to Apple's Metal API.

### Current Architecture

- Rendering code isolated in `src/accelerator/ogl/`
- Platform contexts: SFML (Windows), EGL (Linux)
- Abstract interfaces exist (`image_mixer`, `accelerator_device`)
- 29 blend modes, multiple pixel formats, color space conversions in GLSL shaders

### Target Architecture

- New Vulkan backend in `src/accelerator/vk/`
- MoltenVK for Vulkan-to-Metal translation on macOS
- SPIR-V shaders (compiled from GLSL or written directly)
- Shared abstract interfaces with existing OpenGL backend

---

## Phase 1: Build System & Foundation ✅

**Goal:** Get CasparCG compiling on macOS with Vulkan SDK dependencies.

**Status:** COMPLETED

### Tasks

- [x] Create `src/CMakeModules/Bootstrap_macOS.cmake`
  - Configure Vulkan SDK / MoltenVK dependencies
  - Set up macOS-specific compiler flags
  - Handle framework linking (Cocoa, Metal, QuartzCore)
- [x] Add macOS platform detection in CMake
- [x] Set up dependency fetching for macOS:
  - Vulkan SDK / MoltenVK
  - FFmpeg (Homebrew or custom build)
  - Boost
  - GLFW or similar for window management (replacing SFML)
- [x] Create stub `src/accelerator/vk/` directory structure
- [x] Ensure `common`, `core`, and `shell` modules compile on macOS
- [x] Disable OpenGL-dependent code paths on macOS builds

### Files Created/Modified

- `src/CMakeModules/Bootstrap_macOS.cmake` - macOS build configuration
- `src/CMakeLists.txt` - Platform detection for macOS
- `src/accelerator/CMakeLists.txt` - Vulkan/OpenGL conditional compilation
- `src/accelerator/accelerator_vk.cpp` - Vulkan accelerator implementation
- `src/accelerator/vk/util/device.h/.cpp` - Vulkan device stub
- `src/accelerator/vk/image/image_mixer.h/.cpp` - Vulkan image mixer stub
- `src/accelerator/vk/StdAfx.h` - Precompiled header for Vulkan
- `src/common/CMakeLists.txt` - macOS platform handling
- `src/common/gl/gl_check_stub.cpp` - OpenGL stub for macOS
- `src/common/os/macos/` - macOS-specific OS functions
- `src/shell/CMakeLists.txt` - macOS linking
- `src/shell/macos_specific.cpp` - macOS platform initialization
- `src/modules/CMakeLists.txt` - Disable screen module on macOS

### Deliverable

CasparCG compiles on macOS (with rendering disabled/stubbed).

---

## Phase 2: Vulkan Device & Context ✅

**Goal:** Initialize Vulkan and create a rendering context via MoltenVK.

**Status:** COMPLETED

### Tasks

- [x] Create `src/accelerator/vk/util/device.cpp/h`
  - Vulkan instance creation
  - Physical device selection
  - Logical device and queue creation
  - Command pool and command buffer management
- [x] Create `src/accelerator/vk/util/vk_check.h`
  - Vulkan error checking macros (matching OGL pattern)
- [x] Implement async dispatch queue (matching OGL pattern with Boost.ASIO)
- [x] Add Vulkan validation layers for debug builds
- [x] Create `src/accelerator/vk/util/buffer.cpp/h`
  - GPU buffer allocation and management
  - Host-visible, coherent memory for fast CPU-GPU transfers
  - Persistent mapped pointer for zero-copy access
- [x] Create `src/accelerator/vk/util/texture.cpp/h`
  - Texture/image creation and management
  - Image layout transitions
  - Support for R, RG, BGR, BGRA formats (8-bit and 16-bit)

### Files Created/Modified

- `src/accelerator/vk/util/vk_check.h` - Vulkan error checking macros
- `src/accelerator/vk/util/device.h/.cpp` - Full Vulkan device with async dispatch
- `src/accelerator/vk/util/buffer.h/.cpp` - GPU buffer management
- `src/accelerator/vk/util/texture.h/.cpp` - Texture/image management
- `src/accelerator/vk/StdAfx.h` - Updated with Vulkan headers
- `src/accelerator/CMakeLists.txt` - Added new source files

### Deliverable

Vulkan context initializes successfully, can allocate buffers and textures.

---

## Phase 3: Basic Playout + Layering

**Goal:** Render solid color frames and composite multiple layers with basic alpha blending.

### Tasks

- [ ] Create `src/accelerator/vk/image/image_mixer.cpp/h`
  - Implement `core::image_mixer` interface
  - Basic frame composition pipeline
- [ ] Create `src/accelerator/vk/image/image_kernel.cpp/h`
  - Basic draw operations
  - Vertex buffer setup for quad rendering
- [ ] Port vertex shader to SPIR-V
  - Basic vertex transformation
  - Texture coordinate passing
- [ ] Create minimal fragment shader (SPIR-V)
  - Sample single texture
  - Apply opacity
  - Basic alpha blending (normal/over mode)
- [ ] Implement render pass and framebuffer management
- [ ] Create pipeline state objects for basic rendering
- [ ] Test with `color_producer` (solid color frames)
- [ ] Test multi-layer compositing with alpha

### Deliverable

Can render solid colors and composite 2+ layers with alpha blending.

---

## Phase 4: Blend Modes

**Goal:** Implement all 29 Photoshop-compatible blend modes.

### Tasks

Port blend modes from `src/accelerator/ogl/image/shader.frag` to SPIR-V:

- [ ] **Basic modes:** normal, add, subtract, multiply, screen
- [ ] **Lighten group:** lighten, color_dodge, linear_dodge
- [ ] **Darken group:** darken, color_burn, linear_burn
- [ ] **Contrast group:** overlay, soft_light, hard_light, vivid_light, linear_light, pin_light, hard_mix
- [ ] **Inversion group:** difference, exclusion
- [ ] **Component group:** hue, saturation, color, luminosity
- [ ] **Special modes:** divide, average, negation, phoenix, reflect, glow
- [ ] Implement blend mode selection via push constants or specialization constants
- [ ] Verify visual parity with OpenGL implementation

### Deliverable

All 29 blend modes functional and visually matching OpenGL output.

---

## Phase 5: Geometric Transforms

**Goal:** Implement all MIXER transform commands.

### Tasks

- [ ] Create `src/accelerator/vk/util/transforms.cpp/h`
- [ ] Implement geometric transforms:
  - [ ] FILL (position + scale)
  - [ ] CLIP (clipping rectangle)
  - [ ] CROP (source cropping)
  - [ ] ANCHOR (rotation anchor point)
  - [ ] ROTATION (2D rotation)
  - [ ] PERSPECTIVE (3D perspective transform)
- [ ] Implement transform matrix calculation
- [ ] Update vertex shader for perspective transforms
- [ ] Handle edge anti-aliasing
- [ ] Support tweened animations (30+ easing functions already in core)

### Deliverable

All geometric transforms work via MIXER commands.

---

## Phase 6: Color Processing & Effects

**Goal:** Implement color adjustment and keying effects.

### Tasks

- [ ] Implement color adjustments in fragment shader:
  - [ ] BRIGHTNESS
  - [ ] CONTRAST
  - [ ] SATURATION
  - [ ] LEVELS (min/max input/output, gamma)
  - [ ] INVERT
- [ ] Implement chroma key:
  - [ ] Hue/saturation/brightness thresholds
  - [ ] Softness controls
  - [ ] Spill suppression
- [ ] Implement keyer modes (internal/external key)
- [ ] Implement straight alpha vs premultiplied alpha handling

### Deliverable

All MIXER color commands functional.

---

## Phase 7: Pixel Formats & Color Spaces

**Goal:** Support all input pixel formats and color space conversions.

### Tasks

- [ ] Implement pixel format decoding in fragment shader:
  - [ ] BGRA, RGBA, ARGB, ABGR
  - [ ] BGR, RGB
  - [ ] Gray/Luma
  - [ ] YCbCr (planar 4:2:0, 4:2:2)
  - [ ] YCbCra (planar with alpha)
  - [ ] UYVY (packed 4:2:2)
  - [ ] GBRP, GBRAP (planar for ProRes)
- [ ] Implement color space conversion matrices:
  - [ ] BT.601 (SD)
  - [ ] BT.709 (HD)
  - [ ] BT.2020 (UHD)
- [ ] Support 8-bit, 10-bit, 12-bit, 16-bit depths
- [ ] Handle color range (limited vs full)

### Deliverable

All FFmpeg pixel formats render correctly with proper color.

---

## Phase 8: Core Producers

**Goal:** Get essential producers working.

### Tasks

- [ ] Verify `color_producer` works (should work from Phase 3)
- [ ] Test `route_producer` (routes frames between channels)
- [ ] Enable `image_producer`:
  - [ ] Static image loading (PNG, JPEG, TIFF, BMP, GIF)
  - [ ] Verify color accuracy
- [ ] Enable `image_scroll_producer`:
  - [ ] Scrolling animation
- [ ] Enable `ffmpeg_producer`:
  - [ ] Video file playback
  - [ ] Audio passthrough
  - [ ] Seek, loop, duration controls
  - [ ] All video filters
- [ ] Enable `transition_producer`:
  - [ ] Cut, mix, push, slide, wipe transitions
- [ ] Enable `sting_producer`:
  - [ ] Overlay/mask transitions

### Deliverable

Can play back video files, images, and perform transitions.

---

## Phase 9: Screen Consumer

**Goal:** Display output in a window on macOS.

### Tasks

- [ ] Create macOS window management (Cocoa or GLFW)
- [ ] Create Vulkan swapchain for window surface
- [ ] Implement `screen_consumer` for Vulkan:
  - [ ] Swapchain image presentation
  - [ ] VSync handling
  - [ ] Multiple monitor support
- [ ] Handle window resize and fullscreen
- [ ] Implement proper frame timing/synchronization

### Deliverable

CasparCG displays video output in a macOS window.

---

## Phase 10: File Output (FFmpeg Consumer)

**Goal:** Encode and save/stream video output.

### Tasks

- [ ] Verify `ffmpeg_consumer` works with Vulkan renderer:
  - [ ] Read back rendered frames from GPU
  - [ ] Encode to various codecs (H.264, H.265, ProRes, DNxHD)
  - [ ] Container formats (MP4, MOV, MXF)
- [ ] Test streaming outputs:
  - [ ] RTMP
  - [ ] SRT
  - [ ] HTTP
- [ ] Verify `image_consumer` for image sequence output
- [ ] Optimize GPU-to-CPU readback performance

### Deliverable

Can record and stream output.

---

## Phase 11: Audio

**Goal:** Audio playback and mixing on macOS.

### Tasks

- [ ] Option A: Port OpenAL consumer to macOS
  - [ ] OpenAL-Soft builds on macOS
- [ ] Option B: Create Core Audio consumer
  - [ ] Native macOS audio API
  - [ ] Lower latency potential
- [ ] Verify audio/video synchronization
- [ ] Test MIXER VOLUME and MASTERVOLUME commands

### Deliverable

Audio plays in sync with video.

---

## Phase 12: Hardware I/O (DeckLink)

**Goal:** Support Blackmagic DeckLink cards on macOS.

### Tasks

- [ ] Verify DeckLink SDK available for macOS
- [ ] Enable `decklink_producer`:
  - [ ] SDI input capture
  - [ ] Timecode handling
  - [ ] Ancillary data (VANC)
- [ ] Enable `decklink_consumer`:
  - [ ] SDI output
  - [ ] Keyer output modes
  - [ ] VANC output (OP47, SCTE-104)
- [ ] Test various video formats (SD, HD, UHD)
- [ ] Verify genlock/reference input

### Deliverable

SDI input/output works on macOS.

---

## Phase 13: Network I/O (NDI)

**Goal:** Support NDI streaming on macOS.

### Tasks

- [ ] Verify NDI SDK available for macOS
- [ ] Enable `ndi_producer`:
  - [ ] Discover and receive NDI sources
- [ ] Enable `ndi_consumer`:
  - [ ] Broadcast as NDI source
- [ ] Test with NDI tools

### Deliverable

NDI input/output works on macOS.

---

## Phase 14: HTML/CEF Templates

**Goal:** Support HTML5 templates via Chromium Embedded Framework.

### Tasks

- [ ] Build CEF for macOS with appropriate backend:
  - [ ] Option A: CEF with ANGLE/Metal
  - [ ] Option B: CEF with software rendering
- [ ] Enable `html_producer`:
  - [ ] HTML template loading
  - [ ] JavaScript execution
  - [ ] Dynamic content injection via CG commands
- [ ] Handle transparent backgrounds
- [ ] Verify performance with complex templates

### Deliverable

HTML templates render correctly on macOS.

---

## Phase 15: Full Integration & Testing

**Goal:** Complete system testing and polish.

### Tasks

- [ ] Run full AMCP command test suite (73+ commands)
- [ ] Performance benchmarking:
  - [ ] Frame rate stability
  - [ ] Latency measurements
  - [ ] Memory usage profiling
  - [ ] GPU utilization
- [ ] Stress testing:
  - [ ] Multiple channels
  - [ ] Many layers per channel
  - [ ] 4K/UHD formats
- [ ] Fix remaining bugs and edge cases
- [ ] Documentation:
  - [ ] macOS build instructions
  - [ ] Known limitations
  - [ ] Troubleshooting guide
- [ ] CI/CD pipeline for macOS builds

### Deliverable

Production-ready macOS release.

---

## Module Support Summary

| Module | Phase | Notes |
|--------|-------|-------|
| core | 1 | Foundation |
| color_producer | 3 | Basic rendering |
| image | 8 | After pixel formats |
| ffmpeg | 8 | After pixel formats |
| screen | 9 | Display output |
| oal (audio) | 11 | Audio playback |
| decklink | 12 | Hardware I/O |
| newtek (NDI) | 13 | Network I/O |
| html | 14 | Template rendering |
| artnet | 15 | DMX output (requires boost::variant fix) |
| flash | N/A | Discontinued, not supported |
| bluefish | N/A | Windows-only hardware |

---

## Dependencies

### Required

- **Vulkan SDK 1.3+** with MoltenVK
- **CMake 3.16+**
- **Xcode Command Line Tools**
- **FFmpeg 6.x+** (with macOS libraries)
- **Boost 1.74+**

### Optional

- **DeckLink SDK** (for hardware I/O)
- **NDI SDK** (for network I/O)
- **CEF** (for HTML templates)

---

## File Structure

```
src/accelerator/
├── ogl/                    # Existing OpenGL backend
│   ├── image/
│   │   ├── image_kernel.cpp/h
│   │   ├── image_mixer.cpp/h
│   │   ├── shader.frag
│   │   └── shader.vert
│   └── util/
│       ├── buffer.cpp/h
│       ├── context_egl.cpp
│       ├── context_sfml.cpp
│       ├── device.cpp/h
│       ├── shader.cpp/h
│       └── texture.cpp/h
│
└── vk/                     # New Vulkan backend
    ├── image/
    │   ├── image_kernel.cpp/h
    │   ├── image_mixer.cpp/h
    │   └── shaders/
    │       ├── shader.vert.spv
    │       └── shader.frag.spv
    └── util/
        ├── buffer.cpp/h
        ├── context.cpp/h
        ├── device.cpp/h
        ├── pipeline.cpp/h
        ├── shader.cpp/h
        └── texture.cpp/h
```

---

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| MoltenVK performance issues | Medium | High | Early benchmarking, consider native Metal fallback |
| Shader port complexity | Medium | Medium | Incremental porting, extensive visual testing |
| CEF macOS integration | High | Medium | Consider alternative HTML renderers |
| DeckLink SDK limitations | Low | Medium | Early SDK evaluation |
| Color accuracy differences | Medium | Medium | Comprehensive color tests, reference comparisons |

---

## Success Criteria

1. All 29 blend modes visually match OpenGL output
2. All MIXER commands functional
3. Video playback at target frame rates (25/30/50/60 fps)
4. Latency within acceptable broadcast tolerances
5. Stable 24/7 operation
6. All existing AMCP commands work unchanged

---

## Automated Self-Tests

A test suite is provided in `tests/selftest/` to verify functionality at each phase.

### Running Tests

The test runner automatically starts CasparCG, runs tests, and shuts down:

```bash
cd tests/selftest
./run_tests.sh              # Run all tests
./run_tests.sh --phase 1    # Run phase 1 tests only
./run_tests.sh --list       # List available tests
./run_tests.sh --no-server  # Don't start CasparCG (use existing instance)
```

### Test Coverage by Phase

| Phase | Test Name | What It Verifies |
|-------|-----------|------------------|
| 1 | `build_verification` | Binary runs, responds to AMCP |
| 2 | `connection` | AMCP connection works |
| 3 | `color_playback` | Solid colors render correctly |
| 3 | `multi_layer` | Layer compositing works |
| 3 | `alpha_blend` | Alpha blending between layers |
| 4 | `blend_modes` | All blend modes execute |
| 5 | `transforms` | FILL, ROTATION work |
| 6 | `color_adjust` | Brightness/contrast/saturation |
| 8 | `video_playback` | FFmpeg producer works |
| 9 | `screen_output` | Screen consumer displays |
| 10 | `recording` | FFmpeg consumer records + verifies output |

### Test Infrastructure

- `run_tests.sh` - Main test script (auto-starts/stops CasparCG)
- `amcp_client.py` - AMCP protocol client for sending commands
- `video_analyzer.py` - FFmpeg-based verification (color sampling, frame counting)
- `test_runner.py` - Main test orchestration
- `config.py` - Test configuration
- `casparcg_test.config` - Test configuration for Windows/Linux
- `casparcg_test_macos.config` - Test configuration for macOS (no screen consumer)

### Adding Tests for New Features

When implementing a new phase:
1. Add test methods to `test_runner.py`
2. Register tests with appropriate phase number
3. Use `VideoAnalyzer` for automated color/frame verification
4. Tests should be runnable without human intervention

See `tests/selftest/README.md` for detailed documentation
