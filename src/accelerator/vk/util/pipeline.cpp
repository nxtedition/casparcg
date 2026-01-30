/*
 * Copyright (c) 2011 Sveriges Television AB <info@casparcg.com>
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
 * Author: CasparCG Team
 */

#include "../StdAfx.h"

#include "pipeline.h"
#include "texture.h"
#include "vk_check.h"

#include <common/log.h>

#include <vulkan/vulkan.h>

// Include the compiled SPIR-V shader
#include <vk_blend_shader.h>

namespace caspar { namespace accelerator { namespace vk {

struct blend_pipeline::impl
{
    VkDevice              device_              = VK_NULL_HANDLE;
    VkPhysicalDevice      physical_device_     = VK_NULL_HANDLE;
    VkCommandPool         command_pool_        = VK_NULL_HANDLE;
    VkQueue               queue_               = VK_NULL_HANDLE;
    VkShaderModule        shader_module_       = VK_NULL_HANDLE;
    VkDescriptorSetLayout descriptor_layout_   = VK_NULL_HANDLE;
    VkPipelineLayout      pipeline_layout_     = VK_NULL_HANDLE;
    VkPipeline            pipeline_            = VK_NULL_HANDLE;
    VkDescriptorPool      descriptor_pool_     = VK_NULL_HANDLE;

    impl(void* device, void* physical_device, void* command_pool, void* queue)
        : device_(static_cast<VkDevice>(device))
        , physical_device_(static_cast<VkPhysicalDevice>(physical_device))
        , command_pool_(static_cast<VkCommandPool>(command_pool))
        , queue_(static_cast<VkQueue>(queue))
    {
        create_shader_module();
        create_descriptor_layout();
        create_pipeline_layout();
        create_pipeline();
        create_descriptor_pool();

        CASPAR_LOG(info) << L"[vk::blend_pipeline] Vulkan blend compute pipeline initialized";
    }

    ~impl()
    {
        if (device_ != VK_NULL_HANDLE) {
            vkDeviceWaitIdle(device_);

            if (descriptor_pool_ != VK_NULL_HANDLE) {
                vkDestroyDescriptorPool(device_, descriptor_pool_, nullptr);
            }
            if (pipeline_ != VK_NULL_HANDLE) {
                vkDestroyPipeline(device_, pipeline_, nullptr);
            }
            if (pipeline_layout_ != VK_NULL_HANDLE) {
                vkDestroyPipelineLayout(device_, pipeline_layout_, nullptr);
            }
            if (descriptor_layout_ != VK_NULL_HANDLE) {
                vkDestroyDescriptorSetLayout(device_, descriptor_layout_, nullptr);
            }
            if (shader_module_ != VK_NULL_HANDLE) {
                vkDestroyShaderModule(device_, shader_module_, nullptr);
            }
        }
    }

    void create_shader_module()
    {
        VkShaderModuleCreateInfo createInfo{};
        createInfo.sType    = VK_STRUCTURE_TYPE_SHADER_MODULE_CREATE_INFO;
        createInfo.codeSize = blend_shader_spv_size * sizeof(uint32_t);
        createInfo.pCode    = blend_shader_spv;

        VK(vkCreateShaderModule(device_, &createInfo, nullptr, &shader_module_));
    }

    void create_descriptor_layout()
    {
        // Two storage images: src (read-only) and dst (read-write)
        std::array<VkDescriptorSetLayoutBinding, 2> bindings{};

        // Binding 0: source image (read-only)
        bindings[0].binding            = 0;
        bindings[0].descriptorType     = VK_DESCRIPTOR_TYPE_STORAGE_IMAGE;
        bindings[0].descriptorCount    = 1;
        bindings[0].stageFlags         = VK_SHADER_STAGE_COMPUTE_BIT;
        bindings[0].pImmutableSamplers = nullptr;

        // Binding 1: destination image (read-write)
        bindings[1].binding            = 1;
        bindings[1].descriptorType     = VK_DESCRIPTOR_TYPE_STORAGE_IMAGE;
        bindings[1].descriptorCount    = 1;
        bindings[1].stageFlags         = VK_SHADER_STAGE_COMPUTE_BIT;
        bindings[1].pImmutableSamplers = nullptr;

        VkDescriptorSetLayoutCreateInfo layoutInfo{};
        layoutInfo.sType        = VK_STRUCTURE_TYPE_DESCRIPTOR_SET_LAYOUT_CREATE_INFO;
        layoutInfo.bindingCount = static_cast<uint32_t>(bindings.size());
        layoutInfo.pBindings    = bindings.data();

        VK(vkCreateDescriptorSetLayout(device_, &layoutInfo, nullptr, &descriptor_layout_));
    }

    void create_pipeline_layout()
    {
        VkPushConstantRange pushConstantRange{};
        pushConstantRange.stageFlags = VK_SHADER_STAGE_COMPUTE_BIT;
        pushConstantRange.offset     = 0;
        pushConstantRange.size       = sizeof(blend_push_constants);

        VkPipelineLayoutCreateInfo pipelineLayoutInfo{};
        pipelineLayoutInfo.sType                  = VK_STRUCTURE_TYPE_PIPELINE_LAYOUT_CREATE_INFO;
        pipelineLayoutInfo.setLayoutCount         = 1;
        pipelineLayoutInfo.pSetLayouts            = &descriptor_layout_;
        pipelineLayoutInfo.pushConstantRangeCount = 1;
        pipelineLayoutInfo.pPushConstantRanges    = &pushConstantRange;

        VK(vkCreatePipelineLayout(device_, &pipelineLayoutInfo, nullptr, &pipeline_layout_));
    }

    void create_pipeline()
    {
        VkPipelineShaderStageCreateInfo shaderStageInfo{};
        shaderStageInfo.sType  = VK_STRUCTURE_TYPE_PIPELINE_SHADER_STAGE_CREATE_INFO;
        shaderStageInfo.stage  = VK_SHADER_STAGE_COMPUTE_BIT;
        shaderStageInfo.module = shader_module_;
        shaderStageInfo.pName  = "main";

        VkComputePipelineCreateInfo pipelineInfo{};
        pipelineInfo.sType  = VK_STRUCTURE_TYPE_COMPUTE_PIPELINE_CREATE_INFO;
        pipelineInfo.stage  = shaderStageInfo;
        pipelineInfo.layout = pipeline_layout_;

        VK(vkCreateComputePipelines(device_, VK_NULL_HANDLE, 1, &pipelineInfo, nullptr, &pipeline_));
    }

    void create_descriptor_pool()
    {
        VkDescriptorPoolSize poolSize{};
        poolSize.type            = VK_DESCRIPTOR_TYPE_STORAGE_IMAGE;
        poolSize.descriptorCount = 2;  // src + dst images

        VkDescriptorPoolCreateInfo poolInfo{};
        poolInfo.sType         = VK_STRUCTURE_TYPE_DESCRIPTOR_POOL_CREATE_INFO;
        poolInfo.poolSizeCount = 1;
        poolInfo.pPoolSizes    = &poolSize;
        poolInfo.maxSets       = 1;
        poolInfo.flags         = VK_DESCRIPTOR_POOL_CREATE_FREE_DESCRIPTOR_SET_BIT;

        VK(vkCreateDescriptorPool(device_, &poolInfo, nullptr, &descriptor_pool_));
    }

    VkCommandBuffer begin_command_buffer()
    {
        VkCommandBufferAllocateInfo allocInfo{};
        allocInfo.sType              = VK_STRUCTURE_TYPE_COMMAND_BUFFER_ALLOCATE_INFO;
        allocInfo.commandPool        = command_pool_;
        allocInfo.level              = VK_COMMAND_BUFFER_LEVEL_PRIMARY;
        allocInfo.commandBufferCount = 1;

        VkCommandBuffer cmdBuffer;
        VK(vkAllocateCommandBuffers(device_, &allocInfo, &cmdBuffer));

        VkCommandBufferBeginInfo beginInfo{};
        beginInfo.sType = VK_STRUCTURE_TYPE_COMMAND_BUFFER_BEGIN_INFO;
        beginInfo.flags = VK_COMMAND_BUFFER_USAGE_ONE_TIME_SUBMIT_BIT;

        VK(vkBeginCommandBuffer(cmdBuffer, &beginInfo));

        return cmdBuffer;
    }

    void end_command_buffer(VkCommandBuffer cmdBuffer)
    {
        VK(vkEndCommandBuffer(cmdBuffer));

        VkSubmitInfo submitInfo{};
        submitInfo.sType              = VK_STRUCTURE_TYPE_SUBMIT_INFO;
        submitInfo.commandBufferCount = 1;
        submitInfo.pCommandBuffers    = &cmdBuffer;

        VK(vkQueueSubmit(queue_, 1, &submitInfo, VK_NULL_HANDLE));
        VK(vkQueueWaitIdle(queue_));

        vkFreeCommandBuffers(device_, command_pool_, 1, &cmdBuffer);
    }

    void execute(texture& src, texture& dst, const blend_push_constants& params)
    {
        // Allocate descriptor set
        VkDescriptorSetAllocateInfo allocInfo{};
        allocInfo.sType              = VK_STRUCTURE_TYPE_DESCRIPTOR_SET_ALLOCATE_INFO;
        allocInfo.descriptorPool     = descriptor_pool_;
        allocInfo.descriptorSetCount = 1;
        allocInfo.pSetLayouts        = &descriptor_layout_;

        VkDescriptorSet descriptorSet;
        VK(vkAllocateDescriptorSets(device_, &allocInfo, &descriptorSet));

        // Transition textures to GENERAL layout for compute access
        src.transition_to_general();
        dst.transition_to_general();

        // Update descriptor set with image views
        VkDescriptorImageInfo srcImageInfo{};
        srcImageInfo.imageView   = static_cast<VkImageView>(src.image_view());
        srcImageInfo.imageLayout = VK_IMAGE_LAYOUT_GENERAL;

        VkDescriptorImageInfo dstImageInfo{};
        dstImageInfo.imageView   = static_cast<VkImageView>(dst.image_view());
        dstImageInfo.imageLayout = VK_IMAGE_LAYOUT_GENERAL;

        std::array<VkWriteDescriptorSet, 2> descriptorWrites{};

        descriptorWrites[0].sType           = VK_STRUCTURE_TYPE_WRITE_DESCRIPTOR_SET;
        descriptorWrites[0].dstSet          = descriptorSet;
        descriptorWrites[0].dstBinding      = 0;
        descriptorWrites[0].dstArrayElement = 0;
        descriptorWrites[0].descriptorType  = VK_DESCRIPTOR_TYPE_STORAGE_IMAGE;
        descriptorWrites[0].descriptorCount = 1;
        descriptorWrites[0].pImageInfo      = &srcImageInfo;

        descriptorWrites[1].sType           = VK_STRUCTURE_TYPE_WRITE_DESCRIPTOR_SET;
        descriptorWrites[1].dstSet          = descriptorSet;
        descriptorWrites[1].dstBinding      = 1;
        descriptorWrites[1].dstArrayElement = 0;
        descriptorWrites[1].descriptorType  = VK_DESCRIPTOR_TYPE_STORAGE_IMAGE;
        descriptorWrites[1].pImageInfo      = &dstImageInfo;

        vkUpdateDescriptorSets(device_, static_cast<uint32_t>(descriptorWrites.size()), descriptorWrites.data(), 0, nullptr);

        // Record command buffer
        auto cmdBuffer = begin_command_buffer();

        // Bind pipeline and descriptor set
        vkCmdBindPipeline(cmdBuffer, VK_PIPELINE_BIND_POINT_COMPUTE, pipeline_);
        vkCmdBindDescriptorSets(cmdBuffer, VK_PIPELINE_BIND_POINT_COMPUTE, pipeline_layout_, 0, 1, &descriptorSet, 0, nullptr);

        // Push constants
        vkCmdPushConstants(cmdBuffer, pipeline_layout_, VK_SHADER_STAGE_COMPUTE_BIT, 0, sizeof(blend_push_constants), &params);

        // Dispatch compute shader
        // Work group size is 16x16, so we need to dispatch enough groups to cover the destination
        uint32_t groupCountX = (params.dst_width + 15) / 16;
        uint32_t groupCountY = (params.dst_height + 15) / 16;
        vkCmdDispatch(cmdBuffer, groupCountX, groupCountY, 1);

        end_command_buffer(cmdBuffer);

        // Free descriptor set
        vkFreeDescriptorSets(device_, descriptor_pool_, 1, &descriptorSet);

        // Transition destination back to shader read optimal
        dst.transition_to_shader_read();
    }
};

blend_pipeline::blend_pipeline(void* device, void* physical_device, void* command_pool, void* queue)
    : impl_(std::make_unique<impl>(device, physical_device, command_pool, queue))
{
}

blend_pipeline::~blend_pipeline() = default;

void blend_pipeline::execute(texture& src, texture& dst, const blend_push_constants& params)
{
    impl_->execute(src, dst, params);
}

}}} // namespace caspar::accelerator::vk
