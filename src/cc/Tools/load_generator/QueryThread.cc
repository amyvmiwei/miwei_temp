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

#include <ctime>
#include <cmath>

#include "Hypertable/Lib/Client.h"
#include "Hypertable/Lib/DataGenerator.h"
#include "Hypertable/Lib/Config.h"
#include "Hypertable/Lib/Cells.h"

#include "QueryThread.h"

using namespace Hypertable;

void QueryThread::operator()() {
  double clocks_per_usec = (double)CLOCKS_PER_SEC / 1000000.0;

  ScopedLock lock(m_state.mutex);

  m_state.total_cells = 0;
  m_state.total_bytes = 0;
  m_state.cum_latency = 0;
  m_state.cum_sq_latency = 0;
  m_state.min_latency = 0;
  m_state.max_latency = 0;

  Stopwatch stopwatch;

  try {
    ScanSpecBuilder scan_spec;
    Cell cell;
    clock_t start_clocks, stop_clocks;
    int32_t delay = 0;
    if (has("query-delay"))
      delay = get_i32("query-delay");

    DataGenerator dg(m_props, true);

    for (DataGenerator::iterator iter = dg.begin(); iter != dg.end(); iter++) {
      if (delay)
        poll(0, 0, delay);

      scan_spec.clear();
      scan_spec.add_column((*iter).column_family);
      scan_spec.add_row((*iter).row_key);

      start_clocks = clock();
      
      TableScanner *scanner = m_table->create_scanner(scan_spec.get());
      Cell cell;
      while (scanner->next(cell)) {
        (*m_progress) += 1;
        m_state.total_cells++;
        m_state.total_bytes += strlen(cell.row_key)
            + strlen(cell.column_family) + cell.value_len + 16;
      }
      delete scanner;

      int64_t latency = 0;
      stop_clocks = clock();
      if (stop_clocks < start_clocks)
        latency += (int64_t)((double)((std::numeric_limits<clock_t>::max() - start_clocks) + stop_clocks) / clocks_per_usec);
      else
        latency += (int64_t)((double)(stop_clocks - start_clocks) / clocks_per_usec);
      m_state.cum_latency += latency;
      m_state.cum_sq_latency += ::pow(latency, 2);
      if (latency < m_state.min_latency)
        m_state.min_latency = latency;
      if (latency > m_state.max_latency)
        m_state.max_latency = latency;
    }
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    exit(1);
  }

  m_state.elapsed_time = stopwatch.elapsed();
}
