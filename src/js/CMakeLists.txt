#
# Copyright (C) 2007-2015 Hypertable, Inc.
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

if (NODEJS_FOUND)

  HT_GLOB(GenNODEJS_SRCS node/node_modules/hypertable/lib/hypertable/*.js)

  add_custom_command(
    OUTPUT    ${GenNODEJS_SRCS}
    COMMAND   thrift
    ARGS      -r --gen js:node
              -out ${CMAKE_CURRENT_SOURCE_DIR}/node/node_modules/hypertable/lib/hypertable
              ${ThriftBroker_IDL_DIR}/Hql.thrift
    DEPENDS   ${ThriftBroker_IDL_DIR}/Client.thrift
              ${ThriftBroker_IDL_DIR}/Hql.thrift
    COMMENT   "thrift2js"
  )

  add_custom_target(ThriftGenNODEJS ALL DEPENDS ${GenNODEJS_SRCS})

  add_test(NAME ThriftClient-nodejs
           WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/node
           COMMAND ${NODEJS_EXECUTABLE} ./client_test.js)

  if (NOT HT_COMONENT_INSTALL OR PACKAGE_THRIFTBROKER)
    install(DIRECTORY node DESTINATION lib/js)
  endif ()

endif ()
