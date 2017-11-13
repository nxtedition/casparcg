cmake_minimum_required (VERSION 2.6)
project ("CasparCG Server")

set(NO_CMAKE_SYSTEM_PATH ON)

list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/CMakeModules)

find_package(Git)
find_package(Boost COMPONENTS system thread chrono filesystem log locale regex date_time REQUIRED)
find_package(FFmpeg REQUIRED)
find_package(OpenGL REQUIRED)
find_package(JPEG REQUIRED)
find_package(FreeImage REQUIRED)
find_package(Freetype REQUIRED)
find_package(GLEW REQUIRED)
find_package(TBB REQUIRED)
find_package(SndFile REQUIRED)
find_package(OpenAL REQUIRED)
find_package(GLFW REQUIRED)
find_package(SFML 2 COMPONENTS graphics window system REQUIRED)

set(GIT_REV "0")
set(GIT_HASH "N/A")

if (GIT_FOUND)
	exec_program("${GIT_EXECUTABLE}" "${PROJECT_SOURCE_DIR}"
			ARGS rev-list --all --count
			OUTPUT_VARIABLE GIT_REV)
	exec_program("${GIT_EXECUTABLE}" "${PROJECT_SOURCE_DIR}"
			ARGS rev-parse --verify --short HEAD
			OUTPUT_VARIABLE GIT_HASH)
endif ()

configure_file("${PROJECT_SOURCE_DIR}/version.tmpl" "${PROJECT_SOURCE_DIR}/version.h")

set(BOOST_INCLUDE_PATH			"${Boost_INCLUDE_DIRS}")
set(RXCPP_INCLUDE_PATH			"${CMAKE_CURRENT_SOURCE_DIR}/dependencies64/RxCpp/include")
set(TBB_INCLUDE_PATH			"${TBB_INCLUDE_DIRS}")
set(GLEW_INCLUDE_PATH			"${GLEW_INCLUDE_DIRS}")
set(SFML_INCLUDE_PATH			"${SFML_INCLUDE_DIR}")
set(FREETYPE_INCLUDE_PATH		"${FREETYPE_INCLUDE_DIRS}")
set(FFMPEG_INCLUDE_PATH			"${FFMPEG_INCLUDE_DIRS}")
set(ASMLIB_INCLUDE_PATH			"${EXTERNAL_INCLUDE_PATH}")
set(FREEIMAGE_INCLUDE_PATH		"${FreeImage_INCLUDE_DIRS}")

if (MSVC)
	set(PLATFORM_FOLDER_NAME	"win32")
else()
	set(PLATFORM_FOLDER_NAME	"linux")
endif ()

set_property(GLOBAL PROPERTY USE_FOLDERS ON)

add_definitions( -DSFML_STATIC )
#add_definitions( -DBOOST_THREAD_VERSION=4 )
add_definitions( -DTBB_USE_CAPTURED_EXCEPTION=0 )
add_definitions( -DUNICODE )
add_definitions( -D_UNICODE )
add_definitions( -DGLEW_NO_GLU )
#add_definitions( "-DBOOST_ASIO_ERROR_CATEGORY_NOEXCEPT=noexcept(true) " ) # Workaround macro redefinition in boost
#add_definitions( -D_GLIBCXX_USE_CXX11_ABI=0 ) # Allow compilation in GCC 5 while keeping old dependencies
add_definitions( -DCASPAR_SOURCE_PREFIX="${CMAKE_CURRENT_SOURCE_DIR}" )

if (MSVC)
	set(CMAKE_CXX_FLAGS					"${CMAKE_CXX_FLAGS}					/EHa /Zi /W4 /WX /MP /fp:fast /Zm192 /FIcommon/compiler/vs/disable_silly_warnings.h")
	set(CMAKE_CXX_FLAGS_DEBUG			"${CMAKE_CXX_FLAGS_DEBUG}			/D TBB_USE_ASSERT=1 /D TBB_USE_DEBUG /bigobj")
	set(CMAKE_CXX_FLAGS_RELEASE			"${CMAKE_CXX_FLAGS_RELEASE}			/Oi /Ot /Gy /bigobj")
	set(CMAKE_CXX_FLAGS_RELWITHDEBINFO	"${CMAKE_CXX_FLAGS_RELWITHDEBINFO}	/Oi /Ot /Gy /bigobj /Ob2")
else()
	set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O3 -g")
	set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
	add_compile_options( -msse3 )
	add_compile_options( -mssse3 )
	add_compile_options( -msse4.1 )
#	add_compile_options( -pthread )
#	add_compile_options( -fPIC )
	add_compile_options( -fnon-call-exceptions ) # Allow signal handler to throw exception
	add_compile_options( -Wno-inconsistent-missing-override -Wno-inconsistent-missing-override -Wno-macro-redefined -Wno-unsequenced -Wno-deprecated-declarations -Wno-logical-not-parentheses -Wno-switch -Wno-tautological-constant-out-of-range-compare -Wno-tautological-compare -Wno-writable-strings -Wno-dangling-else )
	add_definitions( -DBOOST_NO_SWPRINTF ) # swprintf on Linux seems to always use , as decimal point regardless of C-locale or C++-locale
	add_definitions( -DTBB_USE_CAPTURED_EXCEPTION=1 )
	add_definitions( -DBOOST_ALL_NO_LIB -DBOOST_ALL_DYN_LINK -DBOOST_LOG_DYN_LINK )
endif ()

if (POLICY CMP0045)
	cmake_policy(SET CMP0045 OLD)
endif ()

include(CMake/PrecompiledHeader.cmake)

set(CASPARCG_MODULE_INCLUDE_STATEMENTS							"" CACHE INTERNAL "")
set(CASPARCG_MODULE_INIT_STATEMENTS								"" CACHE INTERNAL "")
set(CASPARCG_MODULE_UNINIT_STATEMENTS							"" CACHE INTERNAL "")
set(CASPARCG_MODULE_COMMAND_LINE_ARG_INTERCEPTORS_STATEMENTS	"" CACHE INTERNAL "")
set(CASPARCG_MODULE_PROJECTS									"" CACHE INTERNAL "")
set(CASPARCG_RUNTIME_DEPENDENCIES								"" CACHE INTERNAL "")

function(casparcg_add_include_statement HEADER_FILE_TO_INCLUDE)
	set(CASPARCG_MODULE_INCLUDE_STATEMENTS "${CASPARCG_MODULE_INCLUDE_STATEMENTS}"
			"#include <${HEADER_FILE_TO_INCLUDE}>"
			CACHE INTERNAL "")
endfunction()

function(casparcg_add_init_statement INIT_FUNCTION_NAME NAME_TO_LOG)
	set(CASPARCG_MODULE_INIT_STATEMENTS "${CASPARCG_MODULE_INIT_STATEMENTS}"
			"	${INIT_FUNCTION_NAME}(dependencies)\;"
			"	CASPAR_LOG(info) << L\"Initialized ${NAME_TO_LOG} module.\"\;"
			""
			CACHE INTERNAL "")
endfunction()

function(casparcg_add_uninit_statement UNINIT_FUNCTION_NAME)
	set(CASPARCG_MODULE_UNINIT_STATEMENTS
			"	${UNINIT_FUNCTION_NAME}()\;"
			"${CASPARCG_MODULE_UNINIT_STATEMENTS}"
			CACHE INTERNAL "")
endfunction()

function(casparcg_add_command_line_arg_interceptor INTERCEPTOR_FUNCTION_NAME)
	set(CASPARCG_MODULE_COMMAND_LINE_ARG_INTERCEPTORS_STATEMENTS "${CASPARCG_MODULE_COMMAND_LINE_ARG_INTERCEPTORS_STATEMENTS}"
			"	if (${INTERCEPTOR_FUNCTION_NAME}(argc, argv))"
			"		return true\;"
			""
			CACHE INTERNAL "")
endfunction()

function(casparcg_add_module_project PROJECT)
	set(CASPARCG_MODULE_PROJECTS "${CASPARCG_MODULE_PROJECTS}" "${PROJECT}" CACHE INTERNAL "")
endfunction()

# http://stackoverflow.com/questions/7172670/best-shortest-way-to-join-a-list-in-cmake
function(join_list VALUES GLUE OUTPUT)
	string (REGEX REPLACE "([^\\]|^);" "\\1${GLUE}" _TMP_STR "${VALUES}")
	string (REGEX REPLACE "[\\](.)" "\\1" _TMP_STR "${_TMP_STR}") #fixes escaping
	set (${OUTPUT} "${_TMP_STR}" PARENT_SCOPE)
endfunction()

function(casparcg_add_runtime_dependency FILE_TO_COPY)
	set(CASPARCG_RUNTIME_DEPENDENCIES "${CASPARCG_RUNTIME_DEPENDENCIES}" "${FILE_TO_COPY}" CACHE INTERNAL "")
endfunction()

add_subdirectory(accelerator)
add_subdirectory(common)
add_subdirectory(core)
add_subdirectory(modules)

add_subdirectory(protocol)
add_subdirectory(shell)
#add_subdirectory(unit-test)
