/*
 * Copyright (C) 2007-2013 Hypertable, Inc.
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

/** @file
 * Definitions for Event.
 * This file contains method definitions for Event, a class for representing
 * a network communication event.
 */

#include "Common/Compat.h"

#include <iostream>
using namespace std;

extern "C" {
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
}

#include "Common/Error.h"
#include "Common/StringExt.h"

#include "ReactorRunner.h"
#include "Event.h"

using namespace Hypertable;

String Event::to_str() const {
  string dstr;

  dstr = "Event: type=";
  if (type == CONNECTION_ESTABLISHED)
    dstr += "CONNECTION_ESTABLISHED";
  else if (type == DISCONNECT)
    dstr += "DISCONNECT";
  else if (type == MESSAGE) {
    dstr += "MESSAGE";
    dstr += (String)" version=" + (int)header.version;
    dstr += (String)" total_len=" + (int)header.total_len;
    dstr += (String)" header_len=" + (int)header.header_len;
    dstr += (String)" header_checksum=" + (int)header.header_checksum;
    dstr += (String)" flags=" + (int)header.flags;
    dstr += (String)" id=" + (int)header.id;
    dstr += (String)" gid=" + (int)header.gid;
    dstr += (String)" timeout_ms=" + (int)header.timeout_ms;
    dstr += (String)" payload_checksum=" + (int)header.payload_checksum;
    dstr += (String)" command=" + (int)header.command;
  }
  else if (type == TIMER)
    dstr += "TIMER";
  else if (type == ERROR)
    dstr += "ERROR";
  else
    dstr += (int)type;

  if (error != Error::OK)
    dstr += (String)" \"" + Error::get_text(error) + "\"";

  if (type != TIMER) {
    dstr += " from=";
    dstr += addr.format();
  }

  return dstr;
}



