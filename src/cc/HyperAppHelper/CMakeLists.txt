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

set(SRC_DIR "${HYPERTABLE_SOURCE_DIR}/src/cc/HyperAppHelper")
set(DST_DIR "${HYPERTABLE_BINARY_DIR}/src/cc/HyperAppHelper")

set(Helper_SRCS
Unique.cc
Unique.h
Error.h
)

add_library(HyperAppHelper ${Helper_SRCS})
target_link_libraries(HyperAppHelper Hypertable HyperCommon ${BOOST_LIBS} ${Log4cpp_LIBRARIES}
    ${ZLIB_LIBRARIES} ${SIGAR_LIBRARIES} ${NCURSES_LIBRARY}
    ${CMAKE_THREAD_LIBS_INIT} ${MALLOC_LIBRARY})

# tests
add_executable(unique_test tests/unique_test.cc)
target_link_libraries(unique_test HyperAppHelper Hypertable HyperComm HyperCommon)
add_test(HyperAppHelper-Unique unique_test)
add_executable(error_test tests/error_test.cc)
target_link_libraries(error_test HyperAppHelper HyperCommon)
add_test(HyperAppHelper-Error error_test)

configure_file(${HYPERTABLE_SOURCE_DIR}/conf/hypertable.cfg
               ${DST_DIR}/hypertable.cfg)

if (NOT HT_COMPONENT_INSTALL)
  file(GLOB HEADERS *.h)

  install(FILES ${HEADERS} ${VERSION_H} DESTINATION include/HyperAppHelper)

  install(TARGETS HyperAppHelper
          RUNTIME DESTINATION bin
          LIBRARY DESTINATION lib
          ARCHIVE DESTINATION lib)
endif ()
