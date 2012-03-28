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

HT_GLOB(GenPerl_SRCS gen-perl/*.pm)

add_custom_command(
  OUTPUT    ${GenPerl_SRCS}
  COMMAND   thrift
  ARGS      -r --gen perl -o ${CMAKE_CURRENT_SOURCE_DIR}
            ${ThriftBroker_IDL_DIR}/Hql.thrift
  DEPENDS   ${ThriftBroker_IDL_DIR}/Client.thrift
            ${ThriftBroker_IDL_DIR}/Hql.thrift
  COMMENT   "thrift2perl"
)

add_custom_target(ThriftGenPerl ALL DEPENDS ${GenPerl_SRCS})

if (PERLTHRIFT_FOUND)
  add_test(ThriftClient-perl perl -I ${CMAKE_CURRENT_SOURCE_DIR}
           -I ${CMAKE_CURRENT_SOURCE_DIR}/gen-perl
           -I ${THRIFT_SOURCE_DIR}/lib/perl/lib
           ${CMAKE_CURRENT_SOURCE_DIR}/client_test.pl)
endif ()

if (NOT HT_COMPONENT_INSTALL OR PACKAGE_THRIFTBROKER)
  install(DIRECTORY gen-perl Hypertable DESTINATION lib/perl)
  install(FILES client_test.pl DESTINATION lib/perl)
endif ()
