/*
 * Copyright (C) 2007-2014 Hypertable, Inc.
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
/// Definitions for NamespaceFlag.
/// This file contains definitions for NamespaceFlag, a class encapsulating
/// namespace operation bit flags.

#include <Common/Compat.h>

#include "NamespaceFlag.h"

using namespace Hypertable::Lib::Master;
using namespace std;

string NamespaceFlag::to_str(int flags) {
  bool pipe_needed {};
  string str;
  if (flags & CREATE_INTERMEDIATE) {
    if (pipe_needed)
      str += "|";
    else
      pipe_needed = true;
    str += "CREATE_INTERMEDIATE";
  }
  if (flags & IF_EXISTS) {
    if (pipe_needed)
      str += "|";
    else
      pipe_needed = true;
    str += "IF_EXISTS";
  }
  if (flags & IF_NOT_EXISTS) {
    if (pipe_needed)
      str += "|";
    else
      pipe_needed = true;
    str += "IF_NOT_EXISTS";
  }
  return str;
}
