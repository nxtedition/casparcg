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

// Linux: CEF hands over DMA-BUFs, and the ordering rides the kernel's implicit fences.
//
// See accelerated_paint_platform.h for the contract each function implements.

#include "accelerated_paint_platform.h"

#include "../util.h"

#include <common/log.h>

#include <sys/stat.h>
#include <unistd.h>

namespace caspar { namespace html { namespace platform {

using namespace accelerator::vulkan;

std::wstring unusable_reason(gpu_frame_factory& gpu)
{
    // Ozone's headless platform cannot allocate a native pixmap, so with no display server the
    // browser hands out nothing at all and the channel goes black. That is a property of this
    // platform's window system, not of the GPU, which is why it is checked here and not by the
    // caller: another platform has no equivalent question to ask.
    if (detect_display_server() == display_server::none)
        return L"there is no display server (X11 or Wayland)";
    if (!gpu.supports_dmabuf_import())
        return L"this device cannot import DMA-BUFs";
    if (!gpu.supports_sync_fd_semaphores())
        return L"this device cannot move a sync_file fence in and out of a semaphore";
    return {};
}

// The DMA-BUF's inode is the kernel's identity for the buffer object, and is stable across the
// fd numbers CEF hands out. Holding the import keeps a dup of that fd alive, so an inode cannot
// be recycled underneath a cache entry.
uint64_t source_key(const CefAcceleratedPaintInfo& info)
{
    if (info.plane_count <= 0 || info.plane_count > kAcceleratedPaintMaxPlanes || info.planes[0].fd < 0)
        return 0;

    struct stat st = {};
    if (::fstat(info.planes[0].fd, &st) != 0)
        return 0;
    return static_cast<uint64_t>(st.st_ino);
}

// The DRM format modifier: how the memory tiles. Getting this wrong is what produces a black or
// shredded picture on a tiling-strict driver, so it is part of the cache identity.
uint64_t source_layout(const CefAcceleratedPaintInfo& info) { return info.modifier; }

std::shared_ptr<imported_image>
import_source(gpu_frame_factory& gpu, const CefAcceleratedPaintInfo& info, int width, int height, vk::Format format)
{
    dmabuf_image desc;
    desc.width    = width;
    desc.height   = height;
    desc.format   = format;
    desc.modifier = info.modifier;
    for (int i = 0; i < info.plane_count; ++i)
        desc.planes.push_back(dmabuf_plane{info.planes[i].fd, info.planes[i].stride, info.planes[i].offset});

    return gpu.import_dmabuf(desc);
}

void log_first_import(const CefAcceleratedPaintInfo& info, int width, int height)
{
    // The modifier is the piece that differs between GPUs and the thing to quote when this
    // does not work, so it goes in the line that proves the chain works.
    CASPAR_LOG(info) << L"html: imported the browser's shared texture — " << width << L"x" << height << L", "
                     << info.plane_count << L" plane(s), DRM modifier 0x" << std::hex << info.modifier << std::dec
                     << L", stride " << info.planes[0].stride << L", offset " << info.planes[0].offset
                     << L", cef format " << static_cast<int>(info.format) << L", coded " << info.extra.coded_size.width
                     << L"x" << info.extra.coded_size.height << L", visible " << info.extra.visible_rect.x << L","
                     << info.extra.visible_rect.y << L" " << info.extra.visible_rect.width << L"x"
                     << info.extra.visible_rect.height << L".";
}

// CEF hands over no fence of its own — viz attaches a sync token to the blit's acquire side but
// never gives the consumer a release token — so the dma-buf's own write fence is the only
// ordering available against the browser's writes.
vk::Semaphore acquire_wait(gpu_frame_factory& gpu, const CefAcceleratedPaintInfo& info)
{
    const int producer_fence = export_dmabuf_read_fence(info.planes[0].fd);
    if (producer_fence < 0)
        return nullptr; // nothing outstanding on the buffer

    auto sem = gpu.import_sync_fd_semaphore(producer_fence);
    if (!sem)
        ::close(producer_fence); // the import did not take ownership
    return sem;
}

bool publish_release(gpu_frame_factory& gpu, const CefAcceleratedPaintInfo& info, vk::Semaphore signal)
{
    if (!signal)
        return false;

    const int our_fence = gpu.export_sync_fd(signal);
    if (our_fence < 0)
        return false;

    const bool ok = attach_dmabuf_read_fence(info.planes[0].fd, our_fence);
    ::close(our_fence); // the kernel took its own reference
    return ok;
}

const wchar_t* release_failure_hint()
{
    return L"the kernel would not take a read fence on the DMA-BUF, so the paint callback has to block. "
           L"Accelerated paint needs Linux 6.0 or newer.";
}

}}} // namespace caspar::html::platform
