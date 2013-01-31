/** -*- C++ -*-
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
 * along with Hypertable. If not, see <http://www.gnu.org/licenses/>
 */

#include "Common/Compat.h"

#include "TableMutatorShared.h"
#include "TableMutatorIntervalHandler.h"

using namespace Hypertable;


TableMutatorShared::TableMutatorShared(PropertiesPtr &props, Comm *comm,
    Table *table, RangeLocatorPtr &range_locator,
    ApplicationQueueInterfacePtr &app_queue, uint32_t timeout_ms,
    uint32_t flush_interval_ms, uint32_t flags)
  : Parent(props, comm, table, range_locator, timeout_ms, flags),
    m_flush_interval(flush_interval_ms) {

  if (m_flush_interval)
    m_tick_handler = new TableMutatorIntervalHandler(comm, app_queue.get(), this);
}


TableMutatorShared::~TableMutatorShared() {
  if (m_tick_handler)
    m_tick_handler->stop();
}


void TableMutatorShared::interval_flush() {
  try {
    ScopedRecLock lock(m_mutex);
    HiResTime now;

    if (xtime_diff_millis(m_last_flush_ts, now) >= m_flush_interval) {
      HT_DEBUG_OUT <<"need to flush"<< HT_END;
      Parent::flush();
      m_last_flush_ts.reset();
    }
    else
      HT_DEBUG_OUT <<"no need to flush"<< HT_END;
  }
  HT_RETHROW("interval flush")
}
