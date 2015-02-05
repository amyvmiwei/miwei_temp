/* -*- c++ -*-
 * Copyright (C) 2007-2015 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 3
 * of the License, or any later version.
 *
 * Hypertable is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

/// @file
/// Declarations of utility functions.
/// This file contains declarations for utility functions for manipulating files
/// in the brokered filesystem.

#ifndef FsBroker_Lib_Utility_h
#define FsBroker_Lib_Utility_h

#include "Client.h"

#include <string>

namespace Hypertable {
namespace FsBroker {
namespace Lib {
  
  extern void copy(ClientPtr &client, const std::string &from,
                   const std::string &to, int64_t offset=0);

  extern void copy_from_local(ClientPtr &client, const std::string &from,
                              const std::string &to, int64_t offset=0);

  extern void copy_to_local(ClientPtr &client, const std::string &from,
                            const std::string &to, int64_t offset=0);
}}}

#endif // FsBroker_Lib_Utility_h
