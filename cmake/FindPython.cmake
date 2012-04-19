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

# - Find PYTHON
#  PYTHON_INCLUDE_DIR - where to find Python.h
#  PYTHON_LIBRARIES   - List of libraries when using python-devel
#  PYTHON_FOUND       - True if python-devel was found

find_path(PYTHON_INCLUDE_DIR Python.h NO_DEFAULT_PATH PATHS
  ${HT_DEPENDENCY_INCLUDE_DIR}
  /usr/include/python
  /usr/include/python2.7
  /usr/include/python2.6
  /usr/local/include/python
  /usr/local/include/python2.7
  /usr/local/include/python2.6
)

find_library(PYTHON_LIBRARY NAMES python2.7 python2.6 python NO_DEFAULT_PATH PATHS
    ${HT_DEPENDENCY_LIB_DIR}
    /usr/lib
    /usr/local/lib
    /opt/local/lib
    )

if (PYTHON_INCLUDE_DIR)
  set(PYTHON_FOUND TRUE)
endif ()

if (PYTHON_FOUND)
  message(STATUS "Found Python-devel: ${PYTHON_LIBRARY}")
else ()
  message(STATUS "Not Found Python-devel: ${PYTHON_LIBRARY}")
endif ()

mark_as_advanced(
  PYTHON_LIBRARY
  PYTHON_INCLUDE_DIR
)
