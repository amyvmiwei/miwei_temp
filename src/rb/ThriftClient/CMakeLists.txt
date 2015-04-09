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

HT_GLOB(GenRb_SRCS hypertable/gen-rb/client*.rb
                   hypertable/gen-rb/hql_constants.rb
                   hypertable/gen-rb/hql_service.rb)

add_custom_command(
  OUTPUT    ${GenRb_SRCS}
  COMMAND   thrift
  ARGS      -r --gen rb -o ${CMAKE_CURRENT_SOURCE_DIR}/hypertable
            ${ThriftBroker_IDL_DIR}/Hql.thrift
  DEPENDS   ${ThriftBroker_IDL_DIR}/Client.thrift
            ${ThriftBroker_IDL_DIR}/Hql.thrift
  COMMENT   "thrift2rb"
)

add_custom_command(
  OUTPUT    ${CMAKE_CURRENT_SOURCE_DIR}/hypertable/gen-rb/hql_types.rb
  COMMAND   sed
  ARGS      -i 's/hypertable\\/thrift_gen\\/client_types/client_types/g'
            ${CMAKE_CURRENT_SOURCE_DIR}/hypertable/gen-rb/hql_types.rb
  DEPENDS   ${GenRb_SRCS}
  COMMENT   "rbPostProcess"
)

add_custom_target(ThriftGenRb ALL DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/hypertable/gen-rb/hql_types.rb)

if (RUBYTHRIFT_FOUND)
  add_test(ThriftClient-ruby ruby -I ${INSTALL_DIR}/lib/rb
      ${CMAKE_CURRENT_SOURCE_DIR}/client_test.rb)
endif ()

if (NOT HT_COMPONENT_INSTALL OR PACKAGE_THRIFTBROKER)
  install(DIRECTORY hypertable DESTINATION lib/rb)
  install(FILES client_test.rb DESTINATION lib/rb)
endif ()
