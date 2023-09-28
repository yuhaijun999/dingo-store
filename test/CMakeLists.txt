enable_testing()

FIND_PACKAGE( OpenMP REQUIRED)
if(OPENMP_FOUND)
message("OPENMP FOUND")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${OpenMP_C_FLAGS}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${OpenMP_EXE_LINKER_FLAGS}")
endif()

file(GLOB TEST_SRCS "test_*.cc")
foreach(TEST_SRC ${TEST_SRCS})
  message(STATUS "TEST_SRC: ${TEST_SRC}")
  get_filename_component(TEST_WE ${TEST_SRC} NAME_WE)
  add_executable(${TEST_WE}
                 ${TEST_SRC}
                 $<TARGET_OBJECTS:DINGODB_OBJS>
                 $<TARGET_OBJECTS:PROTO_OBJS>
                )
  add_dependencies(${TEST_WE} ${DEPEND_LIBS})
  target_link_libraries(${TEST_WE}
                        "-Xlinker \"-(\""
                        ${GTEST_MAIN_LIBRARIES}
                        ${GTEST_LIBRARIES}
                        ${DYNAMIC_LIB}
                        "-Xlinker \"-)\""
                        )
endforeach()

add_subdirectory(expr)
