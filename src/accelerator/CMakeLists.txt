cmake_minimum_required (VERSION 3.16)
project (accelerator)

# Platform-specific source selection
if (CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    # macOS: Use Vulkan backend (Phase 5 - Geometric Transforms)
    set(SOURCES
        vk/image/image_kernel.cpp
        vk/image/image_mixer.cpp
        vk/util/buffer.cpp
        vk/util/device.cpp
        vk/util/texture.cpp
        vk/util/pipeline.cpp
        vk/util/matrix.cpp
        accelerator_vk.cpp
    )
    set(HEADERS
        vk/image/image_kernel.h
        vk/image/image_mixer.h
        vk/util/buffer.h
        vk/util/device.h
        vk/util/texture.h
        vk/util/pipeline.h
        vk/util/matrix.h
        vk/util/vk_check.h
        vk/StdAfx.h
        vk_blend_shader.h
        accelerator.h
        StdAfx.h
    )

    # Find glslangValidator for SPIR-V compilation
    find_program(GLSLANG_VALIDATOR glslangValidator HINTS $ENV{VULKAN_SDK}/bin)
    if(NOT GLSLANG_VALIDATOR)
        message(FATAL_ERROR "glslangValidator not found! Please install the Vulkan SDK.")
    endif()

    # Compile compute shader to SPIR-V
    set(BLEND_SHADER_SRC ${CMAKE_CURRENT_SOURCE_DIR}/vk/image/shaders/blend.comp)
    set(BLEND_SHADER_SPV ${CMAKE_CURRENT_BINARY_DIR}/blend.comp.spv)

    add_custom_command(
        OUTPUT ${BLEND_SHADER_SPV}
        COMMAND ${GLSLANG_VALIDATOR} -V ${BLEND_SHADER_SRC} -o ${BLEND_SHADER_SPV}
        DEPENDS ${BLEND_SHADER_SRC}
        COMMENT "Compiling blend.comp to SPIR-V"
    )

    # Convert SPIR-V to C header using bin2c
    bin2c_spv("${BLEND_SHADER_SPV}" "vk_blend_shader.h" "caspar::accelerator::vk" "blend_shader_spv")
else()
    # Windows/Linux: Use OpenGL backend
    set(SOURCES
        ogl/image/image_kernel.cpp
        ogl/image/image_mixer.cpp
        ogl/image/image_shader.cpp

        ogl/util/buffer.cpp
        ogl/util/device.cpp
        ogl/util/shader.cpp
        ogl/util/texture.cpp
        ogl/util/matrix.cpp
        ogl/util/transforms.cpp

        accelerator.cpp
    )
    set(HEADERS
        ogl/image/image_kernel.h
        ogl/image/image_mixer.h
        ogl/image/image_shader.h

        ogl/util/buffer.h
        ogl/util/context.h
        ogl/util/device.h
        ogl/util/shader.h
        ogl/util/texture.h
        ogl/util/matrix.h
        ogl/util/transforms.h

        ogl_image_vertex.h
        ogl_image_fragment.h

        accelerator.h
        StdAfx.h
    )

    if (MSVC)
        list(APPEND SOURCES
            ogl/util/context_sfml.cpp
        )
    else()
        list(APPEND SOURCES
            ogl/util/context_egl.cpp
        )
    endif()

    bin2c("ogl/image/shader.vert" "ogl_image_vertex.h" "caspar::accelerator::ogl" "vertex_shader")
    bin2c("ogl/image/shader.frag" "ogl_image_fragment.h" "caspar::accelerator::ogl" "fragment_shader")
endif()

casparcg_add_library(accelerator SOURCES ${SOURCES} ${HEADERS})
target_include_directories(accelerator PRIVATE .. ${CMAKE_CURRENT_BINARY_DIR})

# Platform-specific configuration
if (CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    # macOS: Vulkan backend
    target_precompile_headers(accelerator PRIVATE vk/StdAfx.h)
    target_link_libraries(accelerator PRIVATE common core)
    target_link_libraries(accelerator PRIVATE Vulkan::Vulkan)
    target_link_libraries(accelerator PRIVATE Apple::Frameworks)
else()
    # Windows/Linux: OpenGL backend
    target_precompile_headers(accelerator PRIVATE StdAfx.h)
    target_link_libraries(accelerator PRIVATE common core GLEW::glew)

    if (MSVC)
        target_link_libraries(accelerator PRIVATE sfml-window)
    endif()
endif()

source_group(sources ./.*)
source_group(sources\\cpu\\image cpu/image/.*)
source_group(sources\\cpu\\util cpu/util/.*)
source_group(sources\\ogl\\image ogl/image/.*)
source_group(sources\\ogl\\util ogl/util/.*)
source_group(sources\\vk\\image vk/image/.*)
source_group(sources\\vk\\util vk/util/.*)
