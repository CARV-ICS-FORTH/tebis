# File: tcp_server/CMakeLists.txt Author: Orestis Chiotakis
# <orchiot@ics.forth.gr>

set(tcp_server_EXEC "tcp-server")

if(NOT SGX_BUILD)
  add_executable(${tcp_server_EXEC} server.c plog.c server_handle.c)
  target_link_libraries(${tcp_server_EXEC} pthread parallax m rt)
else()
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
    openenclave::ertdeventry # For production link against ertmeshentry
    tcp_lib
    openenclave::oehostfs
    openenclave::oehostsock
    openenclave::oehostresolver
    openenclave::oehostepoll
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
endif()

install(
  TARGETS ${tcp_server_EXEC}
  ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
  LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
  RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
  PUBLIC_HEADER DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}")

set_target_properties(${tcp_server_EXEC} PROPERTIES OUTPUT_NAME
                                                    ${tcp_server_EXEC})

# find_library(BDB_LIBRARY NAMES libdb.so)
