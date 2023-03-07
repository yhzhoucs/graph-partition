cmake_minimum_required(VERSION 3.0.0)
project(TestPartition VERSION 0.1.0)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include(CTest)
enable_testing()

set(HEADERS inc)

add_executable(run main.cpp)

target_include_directories(run PRIVATE ${HEADERS})

find_package(OpenMP)
if(OpenMP_CXX_FOUND)
    target_link_libraries(run PUBLIC OpenMP::OpenMP_CXX)
endif()

set(CPACK_PROJECT_NAME ${PROJECT_NAME})
set(CPACK_PROJECT_VERSION ${PROJECT_VERSION})
include(CPack)