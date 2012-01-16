# Copyright (C) 2007-2012 Hypertable, Inc.
#
# This file is part of Hypertable.
#
# Hypertable is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 3
# of the License, or any later version.
#
# Hypertable is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Hypertable. If not, see <http://www.gnu.org/licenses/>
#

if (Thrift_FOUND)
  HT_GLOB(ThriftGenJava_SRCS src/gen-java/*.java)

  add_custom_command(
    OUTPUT    ${ThriftGenJava_SRCS}
    COMMAND   thrift
    ARGS      -r -gen java -o ${HYPERTABLE_SOURCE_DIR}/src
              ${ThriftBroker_IDL_DIR}/Hql.thrift
    DEPENDS   ${ThriftBroker_IDL_DIR}/Client.thrift
              ${ThriftBroker_IDL_DIR}/Hql.thrift
    COMMENT   "thrift2java"
  )
endif ()

configure_file(${HYPERTABLE_SOURCE_DIR}/build.xml.in build.xml @ONLY)
add_custom_target(HypertableJavaComponents ALL ant -f build.xml
                  -Dbuild.dir=${HYPERTABLE_BINARY_DIR}/java jar
                  DEPENDS ${ThriftGenJava_SRCS})
add_custom_target(HypertableJavaExamples ALL ant -f build.xml
                  -Dbuild.dir=${HYPERTABLE_BINARY_DIR}/java examples
                  DEPENDS ${ThriftGenJava_SRCS})

add_custom_target(java)
add_dependencies(java HypertableJavaExamples HypertableJavaComponents)

