cmake_minumum_required(VERSION 3.2)

add_executable(tcp-epoll-server server.c tcp_server.c)
