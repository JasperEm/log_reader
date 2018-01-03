cmake_minimum_required(VERSION 2.8)
project(log_reader CXX)

set(CMAKE_CXX_FLAGS "-std=c++17 -Wextra -Wall -pedantic")

set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}")

find_package(CAF COMPONENTS core io REQUIRED)
include_directories(${CAF_INCLUDES})

find_package(Arrow REQUIRED)
include_directories(${ARROW_INCLUDE_DIR})

find_package(Plasma REQUIRED)
include_directories(${PLASMA_INCLUDE_DIR})

macro(add name)
    add_executable(${name} ${name}.cpp ${ARGN})
    target_link_libraries(${name} ${CAF_LIBRARIES} ${ARROW_SHARED_LIB} ${PLASMA_SHARED_LIB})
endmacro()

add(client)
add(server)
