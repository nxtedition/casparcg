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

#include "dmabuf.h"        // dmabuf_image, imported_image
#include "queue_manager.h" // queue_type
#include "texture.h"       // texture, texture_usage, handoff_token (via handoff.h)

#include <common/array.h>
#include <common/bit_depth.h>

#include <core/frame/frame.h>
#include <core/frame/pixel_format.h>

#include <cstdint>
#include <functional>
#include <memory>
#include <vector>

#include <vulkan/vulkan.hpp>

namespace caspar { namespace accelerator { namespace vulkan {

class command_context;
class vulkan_queue;

// One image plane handed over by a GPU producer: a texture the producer already created and recorded
// on its own queue, plus the hand-off describing how it moves to the render queue (the producer fills
// `handoff.completion` after submitting). A bare shared_ptr<texture> — no future, the texture exists.
struct gpu_plane
{
    std::shared_ptr<texture> tex;
    handoff_token            handoff;
};

// The producer-side counterpart to the const_frame::texture() consumer path: lets a GPU-aware
// producer hand the mixer texture(s) that are already on the GPU (compute, hardware decode, ...)
// instead of CPU pixels the transfer service uploads. A producer obtains it the same way the screen
// consumer obtains vulkan::texture — by downcasting the core::frame_factory it already holds:
//
//     auto* gpu = dynamic_cast<vulkan::gpu_frame_factory*>(frame_factory_.get());
//     if (!gpu) { /* non-Vulkan accelerator: fall back to create_frame + CPU pixels */ }
//
// The factory hands out the building blocks; the producer OWNS its recording. Typical use: in its
// constructor a producer mints a command_context for its queue; each frame it creates a texture,
// asks the mixer for the hand-off with make_producer_handoff(*ctx->queue(), ...), records its work +
// the producer->render release on its own context, and wraps the result with import_textures. The
// producer never names the render queue or its consumer-side scope — the mixer owns those. The
// mixer keeps no per-producer state — the textures ride
// on const_frame::opaque() like uploaded ones, so they flow through visit()/renderpass unchanged and
// the existing handoff machinery acquires them on the render queue.
class gpu_frame_factory
{
  public:
    virtual ~gpu_frame_factory() = default;

    // Allocate a GPU texture for the producer to write and the renderer to sample. EXCLUSIVE, initial
    // layout eUndefined (start the producer's first barrier from eUndefined / discard). Pass
    // texture_usage::storage for compute imageStore writes.
    virtual std::shared_ptr<texture>
    create_producer_texture(int width, int height, int stride, common::bit_depth depth) = 0;

    // True when this device can import externally allocated DMA-BUFs (see
    // device::supports_dmabuf_import). A producer that has a zero-copy hardware path AND a
    // CPU fallback asks this once, up front, to decide which one to configure.
    virtual bool supports_dmabuf_import() const = 0;

    // Import an externally allocated DMA-BUF as a transfer-source image. Null on any
    // unsupported case — always a signal to take the CPU path, never fatal. The imported
    // image is a copy SOURCE only: its memory belongs to the foreign exporter, which may
    // recycle it as soon as it stops hearing from us, so copy out of it (into a
    // create_producer_texture() texture) inside the window the exporter guarantees.
    virtual std::shared_ptr<imported_image> import_dmabuf(const dmabuf_image& img) = 0;

    // True when a sync_file fd can be moved in and out of a VkSemaphore (see
    // device::supports_sync_fd_semaphores). A producer importing a DMA-BUF asks this to
    // decide between fencing the exchange on the GPU and blocking the CPU on every frame.
    virtual bool supports_sync_fd_semaphores() const = 0;

    // Wrap a sync_file fd in a binary semaphore for one submit to wait on. Takes ownership of
    // `sync_fd` on success. Null when unsupported or refused.
    virtual vk::Semaphore import_sync_fd_semaphore(int sync_fd) = 0;

    // A binary semaphore whose signal can later be exported as a sync_file. Null when
    // unsupported.
    virtual vk::Semaphore create_exportable_semaphore() = 0;

    // Export a semaphore's pending signal as a sync_file fd the caller owns and must close.
    // Only meaningful after the submit that signals it has been queued. -1 on failure.
    virtual int export_sync_fd(vk::Semaphore semaphore) = 0;

    // Destroy a semaphore from the three calls above. The caller must be certain the submit
    // that used it has completed.
    virtual void destroy_semaphore(vk::Semaphore semaphore) = 0;

    // Mint a fresh command_context on the dedicated queue for `queue` (the render queue's alias when
    // the hardware has no dedicated family; null for an unsupported video queue). The producer holds
    // it for its lifetime and simply lets it drop on destruction — it must NOT block or tear it down
    // itself: the render queue may still be waiting on completion_tokens the context's timeline
    // signalled, so the returned shared_ptr carries a deleter that hands the context back to the mixer
    // for deferred destruction once the GPU has drained it. Get its queue back via
    // command_context::queue() to build the producer->render hand-off with make_producer_handoff().
    virtual std::shared_ptr<command_context> create_command_context(queue_type queue) = 0;

    // Build the producer->render hand-off for a texture the producer is about to release. The
    // producer supplies only what it knows — its own queue and the layout/scope it leaves the texture
    // in; the mixer supplies the render queue and its sampling scope (shader-read in the fragment
    // stage). The producer fills `completion` after submitting, then records record_release() in that
    // submit.
    virtual handoff_token make_producer_handoff(const vulkan_queue&     producer_queue,
                                                vk::ImageLayout         src_layout,
                                                vk::PipelineStageFlags2 src_stage,
                                                vk::AccessFlags2        src_access) = 0;

    // Wrap already-produced, already-recorded textures into a const_frame. Stamps each plane's
    // hand-off onto its texture so the renderer acquires it. `desc` must be valid and describe one
    // plane per texture (e.g. a single bgra plane, or two planes for NV12).
    virtual core::const_frame import_textures(const void*                    tag,
                                              std::vector<gpu_plane>         planes,
                                              const core::pixel_format_desc& desc,
                                              array<const std::int32_t>      audio = {}) = 0;
};

}}} // namespace caspar::accelerator::vulkan
