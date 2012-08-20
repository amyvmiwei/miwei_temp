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

#ifndef HYPERTABLE_QUERYTHREAD_H
#define HYPERTABLE_QUERYTHREAD_H

#include <vector>
#include <boost/progress.hpp>

#include "Hypertable/Lib/Table.h"
#include "Hypertable/Lib/TableMutator.h"

#include "LoadClient.h"
#include "ParallelLoad.h"

namespace Hypertable {

  class QueryThread {

  public:
  QueryThread(PropertiesPtr &props, TablePtr &table,
          boost::progress_display *progress, ParallelStateRec &state)
    : m_props(props), m_table(table), m_progress(progress), m_state(state) {
    }

    void operator()();

  private:
    PropertiesPtr m_props;
    TablePtr m_table;
    boost::progress_display *m_progress;
    ParallelStateRec &m_state;
  };

}

#endif // HYPERTABLE_QUERYTHREAD_H
