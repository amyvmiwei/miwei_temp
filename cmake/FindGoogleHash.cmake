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

# - Find GoogleHash
# Find the native Google Sparse Hash Etc includes
#
#  GoogleHash_INCLUDE_DIR - where to find sparse_hash_set, etc.
#  GoogleHash_FOUND       - True if GoogleHash found.


if (GoogleHash_INCLUDE_DIR)
  # Already in cache, be silent
  set(GoogleHash_FIND_QUIETLY TRUE)
endif ()

find_path(GoogleHash_INCLUDE_DIR google/sparse_hash_set
  /opt/local/include
  /usr/local/include
  /usr/include
)

if (GoogleHash_INCLUDE_DIR)
   set(GoogleHash_FOUND TRUE)
else ()
   set(GoogleHash_FOUND FALSE)
endif ()

if (GoogleHash_FOUND)
   if (NOT GoogleHash_FIND_QUIETLY)
      message(STATUS "Found GoogleHash: ${GoogleHash_INCLUDE_DIR}")
   endif ()
else ()
      message(STATUS "Not Found GoogleHash: ${GoogleHash_INCLUDE_DIR}")
   if (GoogleHash_FIND_REQUIRED)
      message(FATAL_ERROR "Could NOT find GoogleHash includes")
   endif ()
endif ()

mark_as_advanced(
  GoogleHash_INCLUDE_DIR
)
