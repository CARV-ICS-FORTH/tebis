# File: tcp_client/CMakeLists.txt
# Author: Orestis Chiotakis <orchiot@ics.forth.gr>

cmake_minumum_required(VERSION 3.11.0)

add_library(tcp-client-lib tcp_client.c)

add_executable(tcp-client client.c tcp_client.c)
