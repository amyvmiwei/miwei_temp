/* -*- c++ -*-
 * Copyright (C) 2007-2014 Hypertable, Inc.
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
/// This file contains declarations for utility functions used by fsclient and
/// test programs.

#ifndef Tools_fsclient_Utility_h
#define Tools_fsclient_Utility_h

#include <FsBroker/Lib/Client.h>

#include <string>

using namespace std;

namespace Hypertable {
namespace fsclient {

  void copy_from_local(FsBroker::Lib::ClientPtr &client,
                       const string &from, const string &to, int64_t offset=0);

  void copy_to_local(FsBroker::Lib::ClientPtr &client,
                     const string &from, const string &to, int64_t offset=0);

}}

#endif // Tools_fsclient_Utility_h
