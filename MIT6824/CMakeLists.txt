
set(CMAKE_CXX_STANDARD 14)
set (PACKAGE_NAME MIT6824)
set(EXECUTABLE_OUTPUT_PATH ${PROJECT_SOURCE_DIR})
add_library(master SHARED src/master.cpp)
add_library(worker SHARED src/worker.cpp)
target_include_directories(master PUBLIC  include)
#../../buttonrpc-master
target_include_directories(worker PUBLIC  include)
add_executable(WORKER src/worker_main.cpp)
add_executable(MASTER src/master_main.cpp)
target_link_libraries(WORKER PUBLIC worker)

target_link_libraries(MASTER PUBLIC master)

