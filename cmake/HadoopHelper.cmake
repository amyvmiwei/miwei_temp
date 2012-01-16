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

# Legacy Hadoop helper

if (JAVA_INCLUDE_PATH)
  message(STATUS "Java headers found at: ${JAVA_INCLUDE_PATH}")
  if (HADOOP_INCLUDE_PATH)
    message(STATUS "Hadoop includes located at: ${HADOOP_INCLUDE_PATH}")
  else ()
    message(STATUS "Please set HADOOP_INCLUDE_PATH variable for Legacy Hadoop support")
  endif(HADOOP_INCLUDE_PATH)

  if (HADOOP_LIB_PATH)
    message(STATUS "Hadoop libraries located at: ${HADOOP_LIB_PATH}")
  else ()
    message(STATUS "Please set HADOOP_LIB_PATH variable for Legacy Hadoop support")
  endif ()

  if (NOT BUILD_SHARED_LIBS)
    message(STATUS "Not building shared libraries. Legacy Hadoop support will be disabled")
  endif ()
else ()
  message(STATUS "Java JNI not found. Legacy Hadoop support will be disabled.")
endif(JAVA_INCLUDE_PATH)
