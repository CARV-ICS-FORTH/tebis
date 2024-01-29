if(SGX_BUILD)
  add_compile_definitions(SGX)
  find_package(OpenEnclave CONFIG REQUIRED)
  find_package(OpenSSL REQUIRED)
endif()

# Set the library name
set(tcp_client_LIB "tcp-client")

# Add the tcp-client library
add_library(${tcp_client_LIB} STATIC tcp_client.c tcp_client.h)

if(SGX_BUILD)
  target_link_libraries(${tcp_client_LIB} PRIVATE log openenclave::oe_includes
                                                  OpenSSL::SSL)
else()
  target_link_libraries(${tcp_client_LIB} log)
endif()

# Install the library
install(
  TARGETS ${tcp_client_LIB}
  ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
  LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
  RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
  PUBLIC_HEADER DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}")
