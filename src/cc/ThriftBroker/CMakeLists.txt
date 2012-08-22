#
# Copyright (C) 2007-2012 Hypertable, Inc.
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 3
# of the License, or any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
# 02110-1301, USA.
#

HT_GLOB(ThriftGen_SRCS
        gen-cpp/Client_constants.cpp 
        gen-cpp/ClientService.cpp 
        gen-cpp/Client_types.cpp
        gen-cpp/Hql_constants.cpp 
        gen-cpp/HqlService.cpp 
        gen-cpp/Hql_types.cpp)

set(CMAKE_CXX_FLAGS -DHAVE_NETINET_IN_H)

add_library(HyperThrift ThriftHelper.cc SerializedCellsReader.cc SerializedCellsWriter.cc ${ThriftGen_SRCS})
target_link_libraries(HyperThrift ${Thrift_LIBS} ${LibEvent_LIBS})

add_library(HyperThriftConfig Config.cc)
target_link_libraries(HyperThriftConfig HyperThrift Hypertable)

set(ThriftBroker_SRCS ThriftBroker.cc)

add_custom_command(
  OUTPUT    ${ThriftGen_SRCS}
  COMMAND   thrift
  ARGS      -r --gen cpp -o ${CMAKE_CURRENT_SOURCE_DIR}
            ${CMAKE_CURRENT_SOURCE_DIR}/Hql.thrift
  DEPENDS   ${CMAKE_CURRENT_SOURCE_DIR}/Client.thrift
            ${CMAKE_CURRENT_SOURCE_DIR}/Hql.thrift
  COMMENT   "thrift2cpp"
)

# Disable tcmalloc for 32-bit systems
if (CMAKE_SYSTEM_PROCESSOR STREQUAL "i386" OR
    CMAKE_SYSTEM_PROCESSOR STREQUAL "i586" OR
    CMAKE_SYSTEM_PROCESSOR STREQUAL "i686")
  set(THRIFT_MALLOC_LIBRARY "")
else ()
  set(THRIFT_MALLOC_LIBRARY ${MALLOC_LIBRARY})
endif ()

# ThriftBroker
add_executable(ThriftBroker ${ThriftBroker_SRCS})
target_link_libraries(ThriftBroker HyperThriftConfig HyperAppHelper ${THRIFT_MALLOC_LIBRARY})

# Basic regression test
add_executable(client_test tests/client_test.cc)
target_link_libraries(client_test HyperThrift HyperCommon Hypertable)
add_test(ThriftClient-cpp client_test)

# regression test for SerializedCellsWriter/SerializedCellsReader
add_executable(serialized_test tests/serialized_test.cc)
target_link_libraries(serialized_test HyperThrift HyperCommon Hypertable)
add_test(ThriftClient-Serialized-cpp serialized_test)

if (NOT HT_COMPONENT_INSTALL OR PACKAGE_THRIFTBROKER)
  install(TARGETS HyperThrift HyperThriftConfig ThriftBroker
          RUNTIME DESTINATION bin
          LIBRARY DESTINATION lib
          ARCHIVE DESTINATION lib)
  install(FILES Client.h ThriftHelper.h SerializedCellsFlag.h SerializedCellsReader.h SerializedCellsWriter.h Client.thrift Hql.thrift
          DESTINATION include/ThriftBroker)
  install(DIRECTORY gen-cpp DESTINATION include/ThriftBroker)
endif ()

if (PACKAGE_THRIFTBROKER)
  file(GLOB SCRIPTS ${CMAKE_SOURCE_DIR}/bin/*broker.sh ${CMAKE_SOURCE_DIR}/bin/stop-servers.sh ${CMAKE_SOURCE_DIR}/bin/random-wait.sh)
  install(PROGRAMS ${SCRIPTS} DESTINATION bin)
  set(CPACK_PACKAGE_NAME "hypertable-thriftbroker-only")
  include(PackageHelper)
endif ()
