#
# Copyright (C) 2007-2012 Hypertable, Inc.
#
# This file is part of Hypertable.
#
# Hypertable is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; version 3 of
# the License.
#
# Hypertable is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
# 02110-1301, USA.
#

set(Hyperspace_SRCS
ClientKeepaliveHandler.cc
ClientConnectionHandler.cc
Config.cc
DirEntry.cc
DirEntryAttr.cc
HandleCallback.cc
Protocol.cc
Session.cc
HsCommandInterpreter.cc
HsHelpText.cc
HsClientState.cc
)

# Hyperspace library
add_library(Hyperspace ${Hyperspace_SRCS})
target_link_libraries(Hyperspace HyperTools)

set(Master_SRCS
StateDbKeys.cc
BerkeleyDbFilesystem.cc
Event.cc
Master.cc
MetricsHandler.cc
request/RequestHandlerMkdir.cc
request/RequestHandlerDelete.cc
request/RequestHandlerExpireSessions.cc
request/RequestHandlerRenewSession.cc
request/RequestHandlerOpen.cc
request/RequestHandlerClose.cc
request/RequestHandlerAttrSet.cc
request/RequestHandlerAttrGet.cc
request/RequestHandlerAttrIncr.cc
request/RequestHandlerAttrExists.cc
request/RequestHandlerAttrList.cc
request/RequestHandlerAttrDel.cc
request/RequestHandlerExists.cc
request/RequestHandlerReaddir.cc
request/RequestHandlerReaddirAttr.cc
request/RequestHandlerReadpathAttr.cc
request/RequestHandlerLock.cc
request/RequestHandlerRelease.cc
request/RequestHandlerShutdown.cc
request/RequestHandlerStatus.cc
request/RequestHandlerHandshake.cc
request/RequestHandlerDoMaintenance.cc
request/RequestHandlerDestroySession.cc
response/ResponseCallbackOpen.cc
response/ResponseCallbackExists.cc
response/ResponseCallbackAttrGet.cc
response/ResponseCallbackAttrIncr.cc
response/ResponseCallbackAttrExists.cc
response/ResponseCallbackAttrList.cc
response/ResponseCallbackLock.cc
response/ResponseCallbackReaddir.cc
response/ResponseCallbackReaddirAttr.cc
response/ResponseCallbackReadpathAttr.cc
response/ResponseCallbackStatus.cc
ServerConnectionHandler.cc
ServerKeepaliveHandler.cc
main.cc
)

# Disable tcmalloc for 32-bit systems
if (CMAKE_SYSTEM_PROCESSOR STREQUAL "i386" OR
    CMAKE_SYSTEM_PROCESSOR STREQUAL "i586" OR
    CMAKE_SYSTEM_PROCESSOR STREQUAL "i686")
  set(HYPERSPACE_MALLOC_LIBRARY "")
else ()
  set(HYPERSPACE_MALLOC_LIBRARY ${MALLOC_LIBRARY})
endif ()

# Hyperspace.Master
add_executable(Hyperspace.Master ${Master_SRCS})
target_link_libraries(Hyperspace.Master Hyperspace ${BDB_LIBRARIES} ${HYPERSPACE_MALLOC_LIBRARY})

# BerkeleyDbFilesystem test
add_executable(bdb_fs_test tests/bdb_fs_test.cc BerkeleyDbFilesystem.cc StateDbKeys.cc)
target_link_libraries(bdb_fs_test ${BDB_LIBRARIES} HyperCommon)

#
# Copy test files
#
set(SRC_DIR "${HYPERTABLE_SOURCE_DIR}/src/cc/Hyperspace/tests")
set(DST_DIR "${HYPERTABLE_BINARY_DIR}/src/cc/Hyperspace")
configure_file(${SRC_DIR}/bdb_fs_test.golden ${DST_DIR}/bdb_fs_test.golden)

add_test(BerkeleyDbFilesystem bdb_fs_test)

if (NOT HT_COMPONENT_INSTALL)
  file(GLOB HEADERS *.h)

  install(FILES ${HEADERS} DESTINATION include/Hyperspace)

  install(TARGETS Hyperspace Hyperspace.Master
          RUNTIME DESTINATION bin
          LIBRARY DESTINATION lib
          ARCHIVE DESTINATION lib)
endif ()
