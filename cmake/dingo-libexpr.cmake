# Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

INCLUDE(ExternalProject)

SET(DINGO_LIBEXPR_SOURCES_DIR ${CMAKE_SOURCE_DIR}/contrib/dingo-libexpr)
SET(DINGO_LIBEXPR_BINARY_DIR ${THIRD_PARTY_PATH}/build/dingo-libexpr)
SET(DINGO_LIBEXPR_INSTALL_DIR ${THIRD_PARTY_PATH}/install/dingo-libexpr)
SET(DINGO_LIBEXPR_INCLUDE_DIR "${DINGO_LIBEXPR_INSTALL_DIR}/include" CACHE PATH "dingo-libexpr include directory." FORCE)
SET(DINGO_LIBEXPR_REL_LIBRARIES "${DINGO_LIBEXPR_INSTALL_DIR}/lib/libexpr_rel.a" CACHE FILEPATH "dingo-libexpr libexpr_rel library." FORCE)
SET(DINGO_LIBEXPR_LIBRARIES "${DINGO_LIBEXPR_INSTALL_DIR}/lib/libexpr.a" CACHE FILEPATH "dingo-libexpr libexpr library." FORCE)


ExternalProject_Add(
    extern_dingolibexpr
    ${EXTERNAL_PROJECT_LOG_ARGS}
    SOURCE_DIR ${DINGO_LIBEXPR_SOURCES_DIR}
    BINARY_DIR ${DINGO_LIBEXPR_BINARY_DIR}
    PREFIX ${DINGO_LIBEXPR_INSTALL_DIR}
    CMAKE_ARGS -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
    -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
    -DCMAKE_INSTALL_PREFIX=${DINGO_LIBEXPR_INSTALL_DIR}
    -DCMAKE_INSTALL_LIBDIR=${DINGO_LIBEXPR_INSTALL_DIR}/lib
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON
    -DCMAKE_BUILD_TYPE=${THIRD_PARTY_BUILD_TYPE}
    -DBUILD_TESTS=OFF
    ${EXTERNAL_OPTIONAL_ARGS}
    LIST_SEPARATOR |
    CMAKE_CACHE_ARGS -DCMAKE_INSTALL_PREFIX:PATH=${DINGO_LIBEXPR_INSTALL_DIR}
    -DCMAKE_INSTALL_LIBDIR:PATH=${DINGO_LIBEXPR_INSTALL_DIR}/lib
    -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=ON
    -DCMAKE_BUILD_TYPE:STRING=${THIRD_PARTY_BUILD_TYPE}
    BUILD_COMMAND $(MAKE)
    INSTALL_COMMAND mkdir -p ${DINGO_LIBEXPR_INSTALL_DIR}/lib/ COMMAND mkdir -p ${DINGO_LIBEXPR_INSTALL_DIR}/include COMMAND cp ${DINGO_LIBEXPR_BINARY_DIR}/src/rel/libexpr_rel.a ${DINGO_LIBEXPR_REL_LIBRARIES} COMMAND cp ${DINGO_LIBEXPR_BINARY_DIR}/src/expr/libexpr.a ${DINGO_LIBEXPR_LIBRARIES} COMMAND cp -r ${DINGO_LIBEXPR_SOURCES_DIR}/src ${DINGO_LIBEXPR_INCLUDE_DIR}/dingo-libexpr COMMAND find ${DINGO_LIBEXPR_INCLUDE_DIR}  -name CMakeLists.txt -exec rm -rf {} + COMMAND find ${DINGO_LIBEXPR_INCLUDE_DIR} -name *.cc -exec rm -rf {} +
)

ADD_LIBRARY(dingolibexpr STATIC IMPORTED GLOBAL)
SET_PROPERTY(TARGET dingolibexpr PROPERTY IMPORTED_LOCATION ${DINGO_LIBEXPR_REL_LIBRARIES} ${DINGO_LIBEXPR_LIBRARIES})
ADD_DEPENDENCIES(dingolibexpr extern_dingolibexpr)
