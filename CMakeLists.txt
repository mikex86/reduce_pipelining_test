cmake_minimum_required(VERSION 3.25.0)
project(reduce_pipelining_test)

set(CMAKE_CXX_STANDARD 20)

add_executable(half_duplex_test half_duplex.cpp)
add_executable(full_duplex_test full_duplex.cpp)
add_executable(pipelined_test pipelined.cpp)