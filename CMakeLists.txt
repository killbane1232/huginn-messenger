cmake_minimum_required(VERSION 3.10)

project(huginn_messenger_library VERSION 0.0.1 LANGUAGES C)

# Find Go compiler
find_program(GO_BIN go REQUIRED)

# Detect Android cross-compilation
if(CMAKE_SYSTEM_NAME STREQUAL "Android")
  set(GOOS "android")
  # Extract numeric API level from ANDROID_PLATFORM (e.g. "android-24" → "24")
  string(REGEX REPLACE "^android-" "" ANDROID_API_LEVEL "${ANDROID_PLATFORM}")
  # Map ABI to Go arch + NDK target triple
  if(ANDROID_ABI STREQUAL "arm64-v8a")
    set(GOARCH "arm64")
    set(NDK_TRIPLE "aarch64-linux-android${ANDROID_API_LEVEL}")
  elseif(ANDROID_ABI STREQUAL "armeabi-v7a")
    set(GOARCH "arm")
    set(GOARM "7")
    set(NDK_TRIPLE "armv7a-linux-androideabi${ANDROID_API_LEVEL}")
  elseif(ANDROID_ABI STREQUAL "x86_64")
    set(GOARCH "amd64")
    set(NDK_TRIPLE "x86_64-linux-android${ANDROID_API_LEVEL}")
  elseif(ANDROID_ABI STREQUAL "x86")
    set(GOARCH "386")
    set(NDK_TRIPLE "i686-linux-android${ANDROID_API_LEVEL}")
  else()
    message(FATAL_ERROR "Unsupported Android ABI: ${ANDROID_ABI}")
  endif()
  get_filename_component(NDK_BIN_DIR "${CMAKE_C_COMPILER}" DIRECTORY)
  set(GO_CC "${NDK_BIN_DIR}/${NDK_TRIPLE}-clang")
  set(GO_ENV
    "GOOS=${GOOS}"
    "GOARCH=${GOARCH}"
    "CGO_ENABLED=1"
    "CC=${GO_CC}"
    "PATH=${NDK_BIN_DIR}:$ENV{PATH}"
  )
  if(DEFINED GOARM)
    list(APPEND GO_ENV "GOARM=${GOARM}")
  endif()
  message(STATUS "Go cross-compile for Android: ${GOOS}/${GOARCH} (CC=${GO_CC})")
else()
  set(GO_ENV)
endif()

# Build Go shared library during CMake build.
# Go handles its own incremental builds via build cache.
set(GO_LIB_OUT "${CMAKE_CURRENT_BINARY_DIR}/libhuginn_messenger${CMAKE_SHARED_LIBRARY_SUFFIX}")

file(GLOB_RECURSE GO_SOURCES
  "${CMAKE_CURRENT_SOURCE_DIR}/*.go"
)

add_custom_command(OUTPUT "${GO_LIB_OUT}"
  COMMAND ${CMAKE_COMMAND} -E env ${GO_ENV}
    ${GO_BIN} build -buildmode=c-shared -o "${GO_LIB_OUT}" .
  WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
  COMMENT "Building Go shared library for Huginn Messenger FFI"
  DEPENDS
    "${CMAKE_CURRENT_SOURCE_DIR}/go.mod"
    "${CMAKE_CURRENT_SOURCE_DIR}/go.sum"
    ${GO_SOURCES}
)

add_custom_target(huginn_messenger_go_build ALL DEPENDS "${GO_LIB_OUT}")

# Import the built Go library as a CMake target
add_library(huginn_messenger_go SHARED IMPORTED GLOBAL)
set_target_properties(huginn_messenger_go PROPERTIES
  IMPORTED_LOCATION "${GO_LIB_OUT}"
  IMPORTED_NO_SONAME TRUE
)
add_dependencies(huginn_messenger_go huginn_messenger_go_build)
