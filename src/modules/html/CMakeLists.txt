cmake_minimum_required (VERSION 3.28)
project (html)

set(SOURCES
		producer/html_cg_proxy.cpp
		producer/html_cg_proxy.h
		producer/accelerated_paint_importer.h
		producer/accelerated_paint_platform.h
		producer/html_producer.cpp
		producer/html_producer.h

		html.cpp
		html.h
		util.h
)

# Consuming CEF's OnAcceleratedPaint shared texture needs a GPU accelerator to import into and
# a backend for this platform's flavour of shared texture. Exactly one backend is compiled;
# adding a platform means writing accelerated_paint_<platform>.cpp against the contract in
# accelerated_paint_platform.h and listing it here. With no backend the module builds
# accelerated_paint_null.cpp instead, which reports the path as unusable at runtime.
if (ENABLE_VULKAN AND UNIX AND NOT APPLE)
	list(APPEND SOURCES
		producer/accelerated_paint_importer.cpp
		producer/accelerated_paint_linux.cpp
	)
else ()
	list(APPEND SOURCES producer/accelerated_paint_null.cpp)
endif ()

casparcg_add_module_project(html
	SOURCES ${SOURCES}
	INIT_FUNCTION "html::init"
	UNINIT_FUNCTION "html::uninit"
	CLI_INTERCEPTOR "html::intercept_command_line"
	VULKAN_REQUIREMENTS_FUNCTION "html::register_vulkan_requirements"
)
# CEF 152 requires C++20 ("Compiler with C++20 support (-std=c++20)" in its CMakeLists.txt.in).
# Raised for this module alone rather than moving the whole codebase: casparcg_add_module_project
# requests cxx_std_17 PRIVATE, and target_compile_features takes the highest requested standard,
# so this wins here and propagates nowhere.
#
# Keep C++20 constructs out of html.h -- the generated included_modules.h pulls it into the shell
# target, which is still built as C++17.
target_compile_features(html PRIVATE cxx_std_20)

# GCC 14 emits a bogus -Wstringop-overflow from inside Boost.Regex's template instantiation
# (basic_regex_parser -> vector<digraph<char>>::push_back -> std::pair). It only appears under
# C++20, where std::pair's constrained constructor changes the inlining path, and the code it
# points at is entirely in system/Boost headers.
if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
	target_compile_options(html PRIVATE -Wno-stringop-overflow)
endif ()

target_link_libraries(html PRIVATE ffmpeg CEF::CEF accelerator)

target_include_directories(html PRIVATE
    ..
	${FFMPEG_INCLUDE_PATH}
)

if (ENABLE_VULKAN)
	target_link_libraries(html PRIVATE Vulkan::Headers vk-bootstrap::vk-bootstrap)
	target_compile_definitions(html PUBLIC VULKAN_HPP_DISPATCH_LOADER_DYNAMIC=1)
endif ()

# On Mac, override problematic CEF compiler flags that conflict with CasparCG code
if(APPLE)
    # Remove CEF's restrictive flags that break CasparCG compilation
    target_compile_options(html PRIVATE 
        -fexceptions        # Override CEF's -fno-exceptions
        -frtti              # Override CEF's -fno-rtti
        -Wno-newline-eof    # Disable newline warnings
        -Wno-unknown-pragmas # Ignore MSVC-specific pragmas
    )
endif()

set_target_properties(html PROPERTIES FOLDER modules)
source_group(sources\\producer producer/*)
source_group(sources ./*)
