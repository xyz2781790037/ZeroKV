cmake_minimum_required(VERSION 3.16)

project(kvcache_cpp LANGUAGES CXX)

add_library(kvcache_client STATIC
    connector.cc
    shm_allocator.cc
    uds_client.cc
)

target_compile_features(kvcache_client PUBLIC cxx_std_17)
target_include_directories(kvcache_client PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

find_package(Threads REQUIRED)
target_link_libraries(kvcache_client PUBLIC Threads::Threads)

if(UNIX AND NOT APPLE)
    target_link_libraries(kvcache_client PUBLIC rt)
endif()

add_executable(kvcache_cli
    client/kvcache_client.cc
    client/text_kv_cpu_api.cc
)

set_target_properties(kvcache_cli PROPERTIES OUTPUT_NAME kvcache_client)
target_compile_features(kvcache_cli PRIVATE cxx_std_17)
target_link_libraries(kvcache_cli PRIVATE kvcache_client)

find_path(READLINE_INCLUDE_DIR readline/readline.h)
find_library(READLINE_LIBRARY NAMES readline)
find_library(HISTORY_LIBRARY NAMES history)

if(READLINE_INCLUDE_DIR AND READLINE_LIBRARY)
    target_compile_definitions(kvcache_cli PRIVATE KVCACHE_USE_READLINE=1)
    target_include_directories(kvcache_cli PRIVATE ${READLINE_INCLUDE_DIR})
    target_link_libraries(kvcache_cli PRIVATE ${READLINE_LIBRARY})
    if(HISTORY_LIBRARY)
        target_link_libraries(kvcache_cli PRIVATE ${HISTORY_LIBRARY})
    endif()
else()
    message(STATUS "readline not found; kvcache_client will use std::getline")
endif()
