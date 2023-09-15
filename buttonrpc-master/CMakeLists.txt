add_library(buttonrpc SHARED buttonrpc.hpp)
target_include_directories(buttonrpc PUBLIC include)
set_target_properties(buttonrpc PROPERTIES LINKER_LANGUAGE CXX)

add_executable(main main.cpp)