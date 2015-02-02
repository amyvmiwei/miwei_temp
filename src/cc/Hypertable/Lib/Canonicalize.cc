/*
 * Copyright (C) 2007-2015 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 3 of the
 * License, or any later version.
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
/// Definitions for Canonicalize.
/// This file contains definitions for Canonicalize, a static class for
/// canonicalizing table and namespace names.

#include <Common/Compat.h>

#include "Canonicalize.h"

#include <Common/Logger.h>

#include <boost/algorithm/string.hpp>

using namespace Hypertable;
using namespace std;

void Canonicalize::table_name(std::string &name) {
  boost::trim_right_if(name, boost::is_any_of("/ "));
  if (!name.empty() && name[0] != '/')
    name = string("/") + name;
}

void Canonicalize::namespace_path(std::string &path) {
  boost::trim_right_if(path, boost::is_any_of("/ "));
  if (path.empty() || path[0] != '/')
    path = string("/") + path;
}
