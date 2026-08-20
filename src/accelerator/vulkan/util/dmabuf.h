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

#include <cstdint>
#include <memory>
#include <vector>

#include <vulkan/vulkan.hpp>

namespace caspar { namespace accelerator { namespace vulkan {

// DRM_FORMAT_MOD_INVALID: "the exporter did not tell us how the memory is tiled".
// Import refuses it rather than guessing linear — guessing is exactly the bug that
// shows up as a black or shredded picture on tiling-strict drivers (NVIDIA).
constexpr uint64_t drm_format_mod_invalid = 0x00ffffffffffffffULL;
constexpr uint64_t drm_format_mod_linear  = 0ULL;

// One plane of an externally allocated DMA-BUF, as handed over by a producer outside
// our device (CEF's GPU process, a V4L2 decoder, ...). `fd` is BORROWED: import_dmabuf
// dups what it keeps, so the caller still owns — and still closes — its own copy.
struct dmabuf_plane
{
    int      fd     = -1;
    uint32_t stride = 0; // row pitch in bytes
    uint64_t offset = 0; // byte offset of this plane within the buffer
};

// An externally allocated image to import. `modifier` is the DRM format modifier the
// exporter allocated with and is NOT optional — it is what tells the driver how the
// memory tiles, and it is the piece a naive import gets wrong.
struct dmabuf_image
{
    int                       width    = 0;
    int                       height   = 0;
    vk::Format                format   = vk::Format::eUndefined;
    uint64_t                  modifier = drm_format_mod_invalid;
    std::vector<dmabuf_plane> planes;
};

// A VkImage whose memory is an imported DMA-BUF. Deliberately NOT a texture: it is a
// transfer source only — no view, no sampling. The memory belongs to a foreign producer
// that recycles it on its own schedule, so its content has to be copied into a texture we
// own before it can ride a frame through the mixer.
//
// Owns the imported VkDeviceMemory, which owns the dup'd fd — freeing the memory closes it.
class imported_image final
{
  public:
    imported_image(vk::Device       device,
                   vk::Image        image,
                   vk::DeviceMemory memory,
                   int              width,
                   int              height,
                   vk::Format       format,
                   uint64_t         modifier);
    ~imported_image();

    imported_image(const imported_image&)            = delete;
    imported_image& operator=(const imported_image&) = delete;

    vk::Image  id() const { return image_; }
    int        width() const { return width_; }
    int        height() const { return height_; }
    vk::Format format() const { return format_; }
    uint64_t   modifier() const { return modifier_; }

  private:
    vk::Device       device_;
    vk::Image        image_;
    vk::DeviceMemory memory_;
    int              width_;
    int              height_;
    vk::Format       format_;
    uint64_t         modifier_;
};

// Take ownership of an imported image from the foreign (non-Vulkan) producer that wrote
// it, so `dst_family` may read it. Record this before the first use in a submit, every
// time: the foreign producer wrote the memory again since we last looked.
//
// oldLayout is eUndefined, which for a DRM-format-modifier image does not discard the
// pixels — the modifier, not the layout, defines how the memory is arranged, so an
// UNDEFINED acquire is the only thing a Vulkan consumer *can* say about memory some other
// API just wrote. This is the standard dma-buf import barrier.
void record_foreign_acquire(vk::CommandBuffer       cmd,
                            vk::Image               image,
                            uint32_t                dst_family,
                            vk::ImageLayout         new_layout,
                            vk::PipelineStageFlags2 dst_stage,
                            vk::AccessFlags2        dst_access);

// Hand the image back to the foreign producer after the last use in the same submit, so
// it can refill the buffer once our reads have completed. The mirror of the acquire.
void record_foreign_release(vk::CommandBuffer       cmd,
                            vk::Image               image,
                            uint32_t                src_family,
                            vk::ImageLayout         old_layout,
                            vk::PipelineStageFlags2 src_stage,
                            vk::AccessFlags2        src_access);

// ---------------------------------------------------------------------------------------
// Implicit synchronisation: the dma-buf <-> sync_file bridge (Linux 6.0+)
//
// A dma-buf carries the fences of whoever last touched it, in the kernel, on the buffer
// object itself. That is how two APIs that share a buffer but share no timeline (Chromium's
// GL context and our VkDevice) stay ordered without either one blocking the CPU.
//
// The barriers above describe ownership *within* a submit; these two describe ordering
// *between* processes. Both are needed, and neither substitutes for the other.
//
// Every function here degrades to "not supported" rather than failing: the ioctls landed in
// Linux 6.0, and a driver may refuse them. The caller falls back to blocking on the CPU.

// The fence a reader must wait for: every write that has been submitted to `dmabuf_fd` but
// may not have landed. Returns a sync_file fd the CALLER OWNS and must close, or -1 when
// there is nothing to wait for or the kernel/driver does not support the ioctl.
int export_dmabuf_read_fence(int dmabuf_fd);

// Publish `sync_fd` on `dmabuf_fd` as a READ fence, so the next writer — the browser's GPU
// process, refilling a pooled buffer — waits for our copy before overwriting it. This is
// what makes it safe to return from a paint callback with the copy still in flight.
//
// `sync_fd` is BORROWED; the kernel takes a reference of its own. False when unsupported,
// in which case the caller must keep blocking instead.
bool attach_dmabuf_read_fence(int dmabuf_fd, int sync_fd);

}}} // namespace caspar::accelerator::vulkan
