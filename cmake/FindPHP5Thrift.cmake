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

# - Find PHP5
# This module defines
#  PHPTHRIFT_FOUND, If false, do not try to use ant

if (PHPTHRIFT_ROOT)
  if (EXISTS ${PHPTHRIFT_ROOT}/Thrift.php)
    set(PHPTHRIFT_FOUND TRUE)
    set(PHPTHRIFT_ROOT ${PHPTHRIFT_ROOT} CACHE PATH "php thrift root dir" FORCE)
  endif ()
else ()
  set(PHPTHRIFT_FOUND FALSE)
endif ()

if (PHPTHRIFT_FOUND)
  if (NOT PHPTHRIFT_FIND_QUIETLY)
    message(STATUS "Found thrift for php: ${PHPTHRIFT_ROOT}")
  endif ()
else ()
  message(STATUS "PHPTHRIFT_ROOT not found. "
                 "ThriftBroker support for php will be disabled")
endif ()

mark_as_advanced(
  PHPTHRIFT_ROOT
)

