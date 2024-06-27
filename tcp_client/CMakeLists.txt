# File: tcp_client/CMakeLists.txt Author: Orestis Chiotakis
# <orchiot@ics.forth.gr>

set(tcp_client_LIB "tcp-client")

add_library(${tcp_client_LIB} STATIC tcp_client.c tcp_client.h)

target_link_libraries(${tcp_client_LIB} log)

install(
  TARGETS ${tcp_client_LIB}
  ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
  LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
  RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
  PUBLIC_HEADER DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}")

set_target_properties(${tcp_client_LIB} PROPERTIES OUTPUT_NAME
                                                   ${tcp_client_LIB})
