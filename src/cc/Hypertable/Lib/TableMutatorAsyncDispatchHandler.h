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

#ifndef HYPERTABLE_TABLEMUTATORASYNCDISPATCHHANDLER_H
#define HYPERTABLE_TABLEMUTATORASYNCDISPATCHHANDLER_H

#include "AsyncComm/DispatchHandler.h"
#include "AsyncComm/Event.h"

#include "TableMutatorAsync.h"
#include "TableMutatorAsyncSendBuffer.h"

namespace Hypertable {

  /**
   * This class is a DispatchHandler
   *
   */
  class TableMutatorAsyncDispatchHandler : public DispatchHandler {

  public:
    /**
     * Constructor.  Initializes state.
     */
    TableMutatorAsyncDispatchHandler(ApplicationQueueInterfacePtr &app_queue,
                                     TableMutatorAsync *mutator,
                                     uint32_t scatter_buffer,
                                     TableMutatorAsyncSendBuffer *send_buffer,
                                     bool auto_refresh);

    /**
     * Dispatch method.  This gets called by the AsyncComm layer
     * when an event occurs in response to a previously sent
     * request that was supplied with this dispatch handler.
     *
     * @param event_ptr shared pointer to event object
     */
    virtual void handle(EventPtr &event_ptr);

  private:
    ApplicationQueueInterfacePtr     m_app_queue;
    TableMutatorAsync *m_mutator;
    uint32_t m_scatter_buffer;
    TableMutatorAsyncSendBuffer *m_send_buffer;
    bool m_auto_refresh;
  };
}
#endif // HYPERTABLE_TABLEMUTATORDISPATCHHANDLERASYNC_H
