cmake_minimum_required(VERSION 2.8)

project (CSCW)
include_directories(${PPFX}/include)

find_library(COUCHSTORE_LIB couchstore)
find_library(V8_LIB v8)

add_executable(mapple
    mapple.cc
    mapreduce.cc
    cJSON.c
    )

target_link_libraries(mapple ${COUCHSTORE_LIB} ${V8_LIB})

