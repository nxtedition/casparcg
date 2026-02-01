# CasparCG Linux Vulkan Migration Plan

This document outlines the plan to migrate Linux from OpenGL to Vulkan rendering.

## Goal

Replace the OpenGL backend on Linux with the Vulkan backend developed for macOS, achieving:
- Unified rendering codebase across macOS and Linux
- Better GPU utilization and explicit resource control
- Foundation for future Windows Vulkan migration

## Current State

### macOS (Complete)
- Vulkan backend via MoltenVK
- All 43 tests passing
- Screen consumer using GLFW + Vulkan

### Linux (Current)
- OpenGL 4.5 backend with EGL context
- Screen consumer using SFML + OpenGL
- GLSL shaders in `src/accelerator/ogl/`

### Windows (Future)
- OpenGL 4.5 backend with SFML context
- Will migrate to Vulkan after Linux

## What Can Be Reused

The macOS Vulkan implementation is designed to be cross-platform. These components require minimal changes:

| Component | Location | Linux Changes |
|-----------|----------|---------------|
| Vulkan device | `vk/util/device.cpp` | None (uses standard Vulkan) |
| Buffer management | `vk/util/buffer.cpp` | None |
| Texture management | `vk/util/texture.cpp` | None |
| Compute pipeline | `vk/util/pipeline.cpp` | None |
| Transform matrix | `vk/util/matrix.cpp` | None |
| Image kernel | `vk/image/image_kernel.cpp` | None |
| Image mixer | `vk/image/image_mixer.cpp` | None |
| Blend shader | `vk/image/shaders/blend.comp` | None |

## What Needs Linux-Specific Work

### 1. Screen Consumer

macOS uses `.mm` (Objective-C++) for GLFW/Cocoa integration. Linux needs a C++ version:

**Current:** `screen_consumer_vk.mm` (macOS-specific with dispatch_async)

**Needed:** `screen_consumer_vk.cpp` (Linux version)

Key differences:
- No GCD/dispatch_async - GLFW can be called from any thread on Linux
- No NSApplication event loop - standard GLFW event processing
- Simpler threading model

### 2. Build System

**Files to modify:**
- `src/CMakeModules/Bootstrap_Linux.cmake` - Add Vulkan SDK detection
- `src/accelerator/CMakeLists.txt` - Enable Vulkan backend on Linux
- `src/modules/screen/CMakeLists.txt` - Use Vulkan screen consumer

### 3. Main Loop

macOS requires NSApplication for GCD. Linux uses standard `io.run()`:

```cpp
// main.cpp - Linux continues to use standard ASIO
#ifdef __APPLE__
    // macOS-specific event loop (already implemented)
#else
    io.run();  // No changes needed for Linux
#endif
```

### 4. Swapchain Surface Creation

The swapchain code in `vk/util/swapchain.cpp` uses GLFW which is cross-platform, but may need verification for Linux-specific extensions:

- `VK_KHR_xcb_surface` or `VK_KHR_xlib_surface` for X11
- `VK_KHR_wayland_surface` for Wayland

GLFW handles this automatically, but extension availability should be verified.

---

## Implementation Phases

### Phase 1: Build System Setup

**Goal:** Get Vulkan backend compiling on Linux alongside OpenGL.

**Tasks:**
- [ ] Create `Bootstrap_Linux_Vulkan.cmake` or modify `Bootstrap_Linux.cmake`
- [ ] Add Vulkan SDK detection (system package or LunarG SDK)
- [ ] Enable conditional compilation: `USE_VULKAN_BACKEND` option
- [ ] Ensure GLFW is available (may already be via system packages)
- [ ] Add SPIR-V shader compilation to Linux build

**Deliverable:** CMake option to build with Vulkan backend on Linux.

### Phase 2: Core Vulkan Backend

**Goal:** Enable Vulkan rendering on Linux (headless).

**Tasks:**
- [ ] Verify `vk/util/device.cpp` works on Linux
- [ ] Verify `vk/util/buffer.cpp` and `texture.cpp` work
- [ ] Verify compute pipeline and shaders work
- [ ] Run selftest phases 1-8 (without screen consumer)
- [ ] Fix any Linux-specific Vulkan issues

**Deliverable:** Vulkan rendering works headless on Linux.

### Phase 3: Screen Consumer

**Goal:** Display output in a window on Linux.

**Tasks:**
- [ ] Create `screen_consumer_vk.cpp` (Linux version)
  - Remove macOS dispatch_async/GCD
  - Use direct GLFW calls (thread-safe on Linux)
  - Keep all Vulkan swapchain code
- [ ] Verify GLFW + Vulkan surface creation on X11
- [ ] Test on Wayland (if applicable)
- [ ] Run Phase 9 selftest

**Deliverable:** Screen consumer displays video on Linux.

### Phase 4: Integration Testing

**Goal:** Full test suite passes on Linux with Vulkan.

**Tasks:**
- [ ] Run complete selftest suite
- [ ] Test with DeckLink hardware
- [ ] Test NDI input/output
- [ ] Test ffmpeg_consumer recording
- [ ] Performance benchmarking vs OpenGL

**Deliverable:** All tests pass, performance verified.

### Phase 5: Deprecate OpenGL on Linux

**Goal:** Make Vulkan the default and only backend.

**Tasks:**
- [ ] Set Vulkan as default (remove USE_VULKAN_BACKEND option)
- [ ] Update documentation
- [ ] Remove OpenGL code or keep as fallback
- [ ] Update CI/CD pipelines

**Deliverable:** Linux uses Vulkan by default.

---

## Estimated Code Changes

### New Files

```
src/modules/screen/consumer/screen_consumer_vk.cpp  # Linux Vulkan screen consumer
```

### Modified Files

```
src/CMakeModules/Bootstrap_Linux.cmake       # Vulkan SDK detection
src/accelerator/CMakeLists.txt               # Enable Vulkan on Linux
src/modules/screen/CMakeLists.txt            # Platform consumer selection
src/modules/screen/screen.cpp                # Register Vulkan consumer
```

### Unchanged (Reused from macOS)

```
src/accelerator/vk/util/device.cpp/h
src/accelerator/vk/util/buffer.cpp/h
src/accelerator/vk/util/texture.cpp/h
src/accelerator/vk/util/pipeline.cpp/h
src/accelerator/vk/util/render_pipeline.cpp/h
src/accelerator/vk/util/swapchain.cpp/h
src/accelerator/vk/util/matrix.cpp/h
src/accelerator/vk/util/vk_check.h
src/accelerator/vk/image/image_kernel.cpp/h
src/accelerator/vk/image/image_mixer.cpp/h
src/accelerator/vk/image/shaders/blend.comp
src/accelerator/vk/image/shaders/screen.vert
src/accelerator/vk/image/shaders/screen.frag
```

---

## Dependencies

### Required

- **Vulkan SDK 1.3+** - System package or LunarG SDK
  - Ubuntu/Debian: `apt install libvulkan-dev vulkan-tools`
  - Fedora: `dnf install vulkan-loader-devel vulkan-tools`
  - Arch: `pacman -S vulkan-icd-loader vulkan-tools`

- **GLFW 3.3+** - Window management
  - Ubuntu/Debian: `apt install libglfw3-dev`
  - Fedora: `dnf install glfw-devel`
  - Arch: `pacman -S glfw`

- **glslc** (SPIR-V compiler) - Part of Vulkan SDK or shaderc
  - Ubuntu/Debian: `apt install glslang-tools` or `shaderc`
  - Or use LunarG SDK

### GPU Drivers

- **NVIDIA:** Proprietary driver 450+ recommended
- **AMD:** Mesa 21+ with RADV driver
- **Intel:** Mesa 21+ with ANV driver

---

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Driver compatibility | Medium | High | Test on multiple GPU vendors |
| Wayland issues | Medium | Medium | Focus on X11 first, Wayland later |
| Performance regression | Low | High | Benchmark against OpenGL baseline |
| Threading differences | Low | Medium | Linux is simpler (no GCD) |

---

## Testing Strategy

### Pre-Migration Baseline

Before starting, capture OpenGL performance metrics:
- Frame rate stability
- GPU utilization
- Memory usage
- Latency

### Per-Phase Testing

Each phase runs the selftest suite:
```bash
cd tests/selftest
./run_tests.sh --phase N
```

### Hardware Matrix

Test on at least:
- NVIDIA GPU (proprietary driver)
- AMD GPU (RADV)
- Intel integrated (ANV)

---

## Timeline Estimate

| Phase | Effort | Dependencies |
|-------|--------|--------------|
| Phase 1 | 1-2 days | Linux dev environment |
| Phase 2 | 2-3 days | Phase 1 |
| Phase 3 | 2-3 days | Phase 2 |
| Phase 4 | 2-3 days | Phase 3 |
| Phase 5 | 1 day | Phase 4 |

**Total:** ~2 weeks for complete Linux Vulkan migration

---

## Success Criteria

1. All 43+ selftests pass on Linux with Vulkan
2. Performance equal or better than OpenGL
3. No regressions in existing functionality
4. Works on NVIDIA, AMD, and Intel GPUs
5. Documentation updated

---

## Future: Windows Vulkan

After Linux is complete, Windows migration follows similar pattern:

1. Build system changes (MSVC + Vulkan SDK)
2. Verify core Vulkan components
3. Create Windows screen consumer (GLFW or native)
4. Full testing
5. Deprecate OpenGL

The shared Vulkan codebase from Linux/macOS makes Windows straightforward.
