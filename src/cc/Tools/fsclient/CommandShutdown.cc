/**
 * Copyright (C) 2007-2012 Hypertable, Inc.
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

#include "Common/Compat.h"
#include "Common/Error.h"
#include "Common/Usage.h"

#include "FsBroker/Lib/Protocol.h"

#include "CommandShutdown.h"

using namespace Hypertable;

const char *CommandShutdown::ms_usage[] = {
  "shutdown [now]",
  "",
  "  This command sends a shutdown request to the FsBroker",
  "  server.  If the 'now' argument is given, the FsBroker",
  "  will do an unclean shutdown by exiting immediately.  Otherwise",
  "  it will wait for all pending requests to complete before",
  "  shutting down.",
  (const char *)0
};


void CommandShutdown::run() {
  uint16_t flags = 0;
  EventPtr event_ptr;

  if (!m_connected)
    return;

  if (m_args.size() > 0) {
    if (m_args[0].first == "now")
      flags |= FsBroker::Protocol::SHUTDOWN_FLAG_IMMEDIATE;
    else
      HT_THROWF(Error::COMMAND_PARSE_ERROR, "Invalid argument - %s",
                m_args[0].first.c_str());
  }

  if (m_nowait)
    m_client->shutdown(flags, 0);
  else {
    DispatchHandlerSynchronizer sync_handler;
    m_client->shutdown(flags, &sync_handler);
    sync_handler.wait_for_reply(event_ptr);
  }

}


