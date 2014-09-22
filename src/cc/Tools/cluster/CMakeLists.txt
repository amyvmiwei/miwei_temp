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

set(SRC_DIR "${HYPERTABLE_SOURCE_DIR}/src/cc/Tools/cluster")
set(DST_DIR "${HYPERTABLE_BINARY_DIR}/src/cc/Tools/cluster")

set(cluster_SRCS
ht_cluster.cc
ClusterCommandInterpreter.cc
ClusterDefinition.cc
ClusterDefinitionTokenizer.cc
)

# cluster - command interpreter
add_executable(cluster ${cluster_SRCS})
target_link_libraries(cluster Hypertable)

# ClusterDefinitionTokenizer_test
add_executable(ClusterDefinitionTokenizer_test ClusterDefinitionTokenizer.cc
                                     tests/ClusterDefinitionTokenizer_test.cc)
target_link_libraries(ClusterDefinitionTokenizer_test HyperCommon)

add_test(Cluster-definition-tokenizer ClusterDefinitionTokenizer_test
                  ${SRC_DIR}/tests/ClusterDefinitionTokenizer_test.input
                  ${SRC_DIR}/tests/ClusterDefinitionTokenizer_test.golden)

if (NOT HT_COMPONENT_INSTALL)
  install(TARGETS cluster RUNTIME DESTINATION bin)
endif ()
