/*
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
/// Definitions for PollEvent.
/// This file contains type definitions for PollEvent, a namespace containing
/// types and functions for polling events.

#include <Common/Compat.h>

#include "PollEvent.h"

#include <string>

using namespace Hypertable;
using namespace std;

string PollEvent::to_string(int events) {
  string str;
  string or_str;

  if (events & PollEvent::READ) {
    str.append("READ");
    or_str = string("|");
  }

  if (events & PollEvent::PRI) {
    str += or_str + "PRI";
    or_str = string("|");
  }

  if (events & PollEvent::WRITE) {
    str += or_str + "WRITE";
    or_str = string("|");
  }

  if (events & PollEvent::ERROR) {
    str += or_str + "ERROR";
    or_str = string("|");
  }

  if (events & PollEvent::HUP) {
    str += or_str + "HUP";
    or_str = string("|");
  }

  if (events & PollEvent::REMOVE) {
    str += or_str + "REMOVE";
    or_str = string("|");
  }

  if (events & PollEvent::RDHUP) {
    str += or_str + "RDHUP";
    or_str = string("|");
  }

  return str;
}
