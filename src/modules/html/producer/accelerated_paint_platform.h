/*
 * Copyright 2025
 *
 * This file is part of CasparCG (www.casparcg.com).
 *
 * CasparCG is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * CasparCG is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with CasparCG. If not, see <http://www.gnu.org/licenses/>.
 *
 * Author: Niklas Andersson, niklas@niklaspandersson.se
 */

#pragma once

// The platform seam for consuming CEF's OnAcceleratedPaint shared texture.
//
// CEF's "shared texture" is a different object on each platform, and so is the way you order
// your reads against the browser's writes. Everything declared here is what one platform has
// to provide; accelerated_paint_importer.cpp holds the orchestration and knows none of it.
//
// Exactly one accelerated_paint_<platform>.cpp is compiled into the module, chosen in
// CMakeLists.txt. A platform that has no implementation yet is simply not listed there, and
// the module falls back to accelerated_paint_null.cpp, which reports the path as unusable.
//
//   accelerated_paint_linux.cpp  (implemented)
//       info.planes[] of DMA-BUF fds plus a DRM format modifier. Ordering rides the kernel's
//       implicit fences: the dma-buf carries the writer's fence, which is exported as a
//       sync_file and imported into a VkSemaphore, and our completion is published back the
//       same way.
//
//   accelerated_paint_win32.cpp  (to do)
//       info.shared_texture_handle, an NT handle to a D3D11 texture. Import with
//       VK_KHR_external_memory_win32 using VK_EXTERNAL_MEMORY_HANDLE_TYPE_D3D11_TEXTURE_BIT.
//       The ordering primitive is a KEYED MUTEX, which Vulkan takes as
//       VkWin32KeyedMutexAcquireReleaseInfoKHR chained onto the submit rather than as
//       wait/signal semaphores. That does not fit acquire_wait()/publish_release() returning a
//       semaphore, so Windows adds a member to frame_sync below and command_context has to
//       pass it through to vkQueueSubmit. Cache key: the handle value.
//
//   accelerated_paint_macos.cpp  (to do)
//       info.shared_texture_io_surface, an IOSurfaceRef, imported through MoltenVK
//       (VK_EXT_metal_objects / VkImportMetalIOSurfaceInfoEXT). IOSurface carries no fence of
//       its own, so acquire_wait()/publish_release() may legitimately be no-ops there — if so,
//       say that in a comment rather than leaving them looking unfinished. Cache key: the
//       IOSurfaceRef pointer.

#include <accelerator/vulkan/util/command_context.h> // external_semaphores
#include <accelerator/vulkan/util/dmabuf.h>          // imported_image
#include <accelerator/vulkan/util/gpu_frame_factory.h>

#include <vulkan/vulkan.hpp>

#include <cstdint>
#include <memory>
#include <string>

#include <include/cef_render_handler.h>

namespace caspar { namespace html { namespace platform {

// What the platform contributes to one frame's submit. Today that is a pair of binary
// semaphores; this is a struct rather than a bare pair because Windows will need to carry
// keyed-mutex acquire/release here instead.
struct frame_sync
{
    accelerator::vulkan::external_semaphores semaphores;
};

// Empty when the device can take this path; otherwise the specific missing capability, for the
// log to name. Both halves — import and fencing — are required: importing without the fences
// is what corrupts frames under load, because the copy races the browser's writes.
std::wstring unusable_reason(accelerator::vulkan::gpu_frame_factory& gpu);

// A stable identity for the buffer CEF handed over, so the import can be cached across frames.
// Must stay valid for as long as the import is held. 0 means "cannot be used".
uint64_t source_key(const CefAcceleratedPaintInfo& info);

// A second cache key component: two buffers with the same identity must still be re-imported
// if the exporter changed how the memory is laid out. 0 when the platform has no such concept.
uint64_t source_layout(const CefAcceleratedPaintInfo& info);

// CEF's handle -> an image we can copy out of. Null on any failure; the caller treats that as
// fatal for the path, so do not throw.
std::shared_ptr<accelerator::vulkan::imported_image>
import_source(accelerator::vulkan::gpu_frame_factory& gpu,
              const CefAcceleratedPaintInfo&          info,
              int                                     width,
              int                                     height,
              vk::Format                              format);

// One line the first time an import succeeds: it proves the whole chain works and names
// whatever the platform-specific detail is that differs between GPUs.
void log_first_import(const CefAcceleratedPaintInfo& info, int width, int height);

// Order our copy behind the browser's writes. Null when there is nothing outstanding to wait
// for, which is normal and not an error. Nothing here may depend on the exporter cooperating:
// we are the one waiting.
vk::Semaphore acquire_wait(accelerator::vulkan::gpu_frame_factory& gpu, const CefAcceleratedPaintInfo& info);

// Publish our completion so the browser will not refill the buffer while the copy is still
// reading it. False when the platform refuses, in which case the caller blocks instead.
bool publish_release(accelerator::vulkan::gpu_frame_factory& gpu,
                     const CefAcceleratedPaintInfo&          info,
                     vk::Semaphore                           signal);

// What to tell the operator when publish_release() fails: which platform requirement is not
// being met. Logged once.
const wchar_t* release_failure_hint();

}}} // namespace caspar::html::platform
