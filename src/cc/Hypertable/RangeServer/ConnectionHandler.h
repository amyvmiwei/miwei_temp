/** -*- c++ -*-
 * Copyright (C) 2007-2012 Hypertable, Inc.
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

#ifndef HYPERTABLE_RANGESERVER_CONNECTIONHANDLER_H
#define HYPERTABLE_RANGESERVER_CONNECTIONHANDLER_H

#include "AsyncComm/ApplicationQueue.h"
#include "AsyncComm/DispatchHandler.h"

#include "RangeServer.h"

namespace Hypertable {

  class Comm;

  /**
   */
  class ConnectionHandler : public DispatchHandler {
  public:

    ConnectionHandler(Comm *, ApplicationQueuePtr &, RangeServerPtr);

    virtual void handle(EventPtr &event);

  private:
    Comm                *m_comm;
    ApplicationQueuePtr  m_app_queue;
    RangeServerPtr       m_range_server;
    bool                 m_shutdown;
  };

}

#endif // HYPERTABLE_RANGESERVER_CONNECTIONHANDLER_H

