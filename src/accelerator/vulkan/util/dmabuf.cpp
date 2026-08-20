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

#include "dmabuf.h"

#if defined(__linux__)
#include <atomic>
#include <cerrno>
#include <sys/ioctl.h>
#endif

namespace caspar { namespace accelerator { namespace vulkan {

imported_image::imported_image(vk::Device       device,
                               vk::Image        image,
                               vk::DeviceMemory memory,
                               int              width,
                               int              height,
                               vk::Format       format,
                               uint64_t         modifier)
    : device_(device)
    , image_(image)
    , memory_(memory)
    , width_(width)
    , height_(height)
    , format_(format)
    , modifier_(modifier)
{
}

imported_image::~imported_image()
{
    // Freeing the imported memory closes the fd Vulkan took ownership of at import.
    device_.destroyImage(image_);
    device_.freeMemory(memory_);
}

namespace {
vk::ImageMemoryBarrier2 foreign_barrier(vk::Image image, vk::ImageLayout old_layout, vk::ImageLayout new_layout)
{
    vk::ImageMemoryBarrier2 barrier{};
    barrier.image            = image;
    barrier.oldLayout        = old_layout;
    barrier.newLayout        = new_layout;
    barrier.subresourceRange = vk::ImageSubresourceRange(vk::ImageAspectFlagBits::eColor, 0, 1, 0, 1);
    return barrier;
}

void submit_barrier(vk::CommandBuffer cmd, const vk::ImageMemoryBarrier2& barrier)
{
    vk::DependencyInfo dep_info;
    dep_info.setImageMemoryBarriers(barrier);
    cmd.pipelineBarrier2(dep_info);
}
} // namespace

void record_foreign_acquire(vk::CommandBuffer       cmd,
                            vk::Image               image,
                            uint32_t                dst_family,
                            vk::ImageLayout         new_layout,
                            vk::PipelineStageFlags2 dst_stage,
                            vk::AccessFlags2        dst_access)
{
    // Acquire half of a queue-family ownership transfer: the src scope is empty (the
    // foreign producer's writes are made visible by whatever it signalled to us), only the
    // dst scope is described.
    auto barrier                = foreign_barrier(image, vk::ImageLayout::eUndefined, new_layout);
    barrier.srcQueueFamilyIndex = VK_QUEUE_FAMILY_FOREIGN_EXT;
    barrier.dstQueueFamilyIndex = dst_family;
    barrier.srcStageMask        = vk::PipelineStageFlagBits2::eTopOfPipe;
    barrier.srcAccessMask       = vk::AccessFlagBits2::eNone;
    barrier.dstStageMask        = dst_stage;
    barrier.dstAccessMask       = dst_access;
    submit_barrier(cmd, barrier);
}

void record_foreign_release(vk::CommandBuffer       cmd,
                            vk::Image               image,
                            uint32_t                src_family,
                            vk::ImageLayout         old_layout,
                            vk::PipelineStageFlags2 src_stage,
                            vk::AccessFlags2        src_access)
{
    // Release half: only the src scope is described. eGeneral is the layout convention for
    // handing a modifier image back to a non-Vulkan user, which cannot express layouts.
    auto barrier                = foreign_barrier(image, old_layout, vk::ImageLayout::eGeneral);
    barrier.srcQueueFamilyIndex = src_family;
    barrier.dstQueueFamilyIndex = VK_QUEUE_FAMILY_FOREIGN_EXT;
    barrier.srcStageMask        = src_stage;
    barrier.srcAccessMask       = src_access;
    barrier.dstStageMask        = vk::PipelineStageFlagBits2::eBottomOfPipe;
    barrier.dstAccessMask       = vk::AccessFlagBits2::eNone;
    submit_barrier(cmd, barrier);
}

#if defined(__linux__)
namespace {

// From <linux/dma-buf.h>, declared here rather than included: these landed in Linux 6.0 and
// the build host's UAPI headers may predate them. The kernel only ever sees the numbers.
struct dma_buf_sync_file
{
    uint32_t flags;
    int32_t  fd;
};

constexpr unsigned dma_buf_sync_read  = 1u << 0;
constexpr char     dma_buf_ioctl_base = 'b';

// _IOWR(b, 2, struct dma_buf_sync_file) / _IOW(b, 3, struct dma_buf_sync_file)
const unsigned long ioctl_export_sync_file = _IOWR(dma_buf_ioctl_base, 2, struct dma_buf_sync_file);
const unsigned long ioctl_import_sync_file = _IOW(dma_buf_ioctl_base, 3, struct dma_buf_sync_file);

// A kernel or driver without these ioctls answers the same way every time, so ask once and
// then stop paying for the syscall. ENOTTY/EINVAL from the first attempt is the answer for
// the process's lifetime.
std::atomic<bool> sync_file_unsupported{false};

bool is_unsupported(int err) { return err == ENOTTY || err == EINVAL || err == ENOSYS; }

} // namespace

int export_dmabuf_read_fence(int dmabuf_fd)
{
    if (dmabuf_fd < 0 || sync_file_unsupported.load(std::memory_order_relaxed))
        return -1;

    // DMA_BUF_SYNC_READ means "I intend to read", and the kernel answers with the fences
    // that must signal first — the WRITE fences. Asking for WRITE would return the readers
    // too, which would make us wait on ourselves.
    dma_buf_sync_file arg{};
    arg.flags = dma_buf_sync_read;
    arg.fd    = -1;

    if (::ioctl(dmabuf_fd, ioctl_export_sync_file, &arg) != 0) {
        if (is_unsupported(errno))
            sync_file_unsupported.store(true, std::memory_order_relaxed);
        return -1;
    }

    return arg.fd;
}

bool attach_dmabuf_read_fence(int dmabuf_fd, int sync_fd)
{
    if (dmabuf_fd < 0 || sync_fd < 0 || sync_file_unsupported.load(std::memory_order_relaxed))
        return false;

    dma_buf_sync_file arg{};
    arg.flags = dma_buf_sync_read;
    arg.fd    = sync_fd;

    if (::ioctl(dmabuf_fd, ioctl_import_sync_file, &arg) != 0) {
        if (is_unsupported(errno))
            sync_file_unsupported.store(true, std::memory_order_relaxed);
        return false;
    }

    return true;
}
#else
int  export_dmabuf_read_fence(int) { return -1; }
bool attach_dmabuf_read_fence(int, int) { return false; }
#endif

}}} // namespace caspar::accelerator::vulkan
