cmake_minimum_required(VERSION 3.10)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
include(FetchContent)
include(ExternalProject)
include(cmake_utils/PreventInSouceBuilds.cmake.in)

project(tcp_server LANGUAGES C)

# Set a default build type if none was specified
set(default_build_type "Release")
if(EXISTS "${CMAKE_SOURCE_DIR}/.git")
  set(default_build_type "Debug")
endif()

option(
  RUN_IWYU
  "Run include-what-you-use on the source files of the key-value store library."
  OFF)

option(DISABLE_ALL_WARNINGS "Disable all warnings to parse iwyu output." OFF)

# TODO: remove this and find a clean solution for it option(USE_FORKED_PARALLAX
# "Use open sourced Parallax from github" OFF)

if(RUN_IWYU STREQUAL "ON")
  file(COPY ${CMAKE_SOURCE_DIR}/scripts/CI/check-iwyu-output.py
       DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
endif()

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  message(
    STATUS
      "Setting build type to '${default_build_type}' as none was specified.")
  set(CMAKE_BUILD_TYPE
      "${default_build_type}"
      CACHE STRING "Choose the type of build." FORCE)
  # Set the possible values of build type for cmake-gui
  set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release"
                                               "MinSizeRel" "RelWithDebInfo")
endif()

if(CMAKE_BUILD_TYPE MATCHES "Release")
  add_definitions(-DRELEASE_BUILD_TYPE)
else()
  add_definitions(-DDEBUG_BUILD_TYPE)
endif()

add_definitions(-DBUILD_TYPE=${default_build_type})
set(tcp_server_EXEC "tcp-server")

if(ENC_KEYS)
  add_compile_definitions(ENC_KEYS)
endif()

if(ENC_VALUES)
  add_compile_definitions(ENC_VALUES)
endif()

if(SGX_BUILD OR SSL)
  add_compile_definitions(SSL)
endif()

if(NOT SGX_BUILD AND SSL)
  find_package(MbedTLS REQUIRED)
  add_executable(${tcp_server_EXEC} server.c plog.c server_handle.c
                                    ../common/common_ssl/mbedtls_utility.c)
  target_link_libraries(
    ${tcp_server_EXEC}
    MbedTLS::mbedx509
    MbedTLS::mbedcrypto
    MbedTLS::mbedtls
    pthread
    parallax
    rt)
elseif(SGX_BUILD)
  find_package(OpenEnclave CONFIG REQUIRED)
  add_compile_definitions(SGX)
  add_library(
    tcp_lib server.c plog.c server_handle.c ../common/common_ssl/utility.c
            ../common/common_ssl/mbedtls_utility.c)
  target_link_libraries(tcp_lib openenclave::oecryptombedtls pthread parallax
                        rt openenclave::oe_includes)
  add_executable(${tcp_server_EXEC} enc.c)
  target_compile_definitions(${tcp_server_EXEC} PUBLIC OE_API_VERSION=2)
  target_link_libraries(
    ${tcp_server_EXEC}
    openenclave::oeenclave
    openenclave::ertdeventry
    tcp_lib
    openenclave::oehostfs
    openenclave::oehostsock
    openenclave::oehostresolver
    openenclave::oehostepoll
    openenclave::oeseal_gcmaes
    parallax
    log
    yaml
    libbloom)

  # Generate key
  add_custom_command(
    OUTPUT private.pem public.pem
    COMMAND openssl genrsa -out private.pem -3 3072
    COMMAND openssl rsa -in private.pem -pubout -out public.pem)

  add_custom_command(
    OUTPUT enclave.signed
    DEPENDS ${tcp_server_EXEC} enclave.conf private.pem
    COMMAND openenclave::oesign sign -e $<TARGET_FILE:${tcp_server_EXEC}> -c
            ${CMAKE_CURRENT_SOURCE_DIR}/enclave.conf -k private.pem)

  add_custom_target(sign_tcp_server ALL DEPENDS enclave.signed)
else()
  add_executable(${tcp_server_EXEC} server.c plog.c server_handle.c)
  target_link_libraries(${tcp_server_EXEC} pthread parallax rt)
endif()

install(
  TARGETS ${tcp_server_EXEC}
  ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
  LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
  RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
  PUBLIC_HEADER DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}")

set_target_properties(${tcp_server_EXEC} PROPERTIES OUTPUT_NAME
                                                    ${tcp_server_EXEC})

FetchContent_Declare(
  log # Recommendation: Stick close to the original name.
  GIT_REPOSITORY https://github.com/innerout/log.c.git
  GIT_PROGRESS YES)

FetchContent_GetProperties(log)

if(NOT log_POPULATED)
  FetchContent_Populate(log)
  add_subdirectory(${log_SOURCE_DIR} ${log_BINARY_DIR})
  include_directories(${log_SOURCE_DIR}/include)
  FetchContent_MakeAvailable(log)
endif()

if(SGX_BUILD
   OR ENC_KEYS
   OR ENC_VALUES)
  FetchContent_Declare(
    parallax
    GIT_REPOSITORY git@carvgit.ics.forth.gr:storage/parallax.git
    GIT_TAG sgx)
else()
  FetchContent_Declare(
    parallax
    GIT_REPOSITORY https://github.com/CARV-ICS-FORTH/parallax.git
    GIT_TAG embed_bloom_filters_in_sst)
endif()

FetchContent_GetProperties(parallax)

if(NOT parallax_POPULATED)
  FetchContent_Populate(parallax)
  add_subdirectory(${parallax_SOURCE_DIR} ${parallax_BINARY_DIR})
  include_directories(${parallax_SOURCE_DIR}/lib)
  FetchContent_MakeAvailable(parallax)
endif()
