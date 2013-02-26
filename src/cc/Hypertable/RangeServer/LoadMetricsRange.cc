/** -*- c++ -*-
 * Copyright (C) 2007-2012 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 3 of the
 * License.
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
#include "Common/String.h"

#include <ctime>

#include "Global.h"
#include "LoadMetricsRange.h"

using namespace Hypertable;

LoadMetricsRange::LoadMetricsRange(const String &table_id, const String &start_row, const String &end_row)
  : m_new_rows(false), m_timestamp(time(0)) {
  initialize(table_id, start_row, end_row);
}


/**
 *  Value format for version 1:
 *
 * @verbatim
 * v2:<ts>,<disk>,<memory>,<disk-bytes-read-rate>,<byte-write-rate>,<byte-read-rate>,<update-rate>,<scan-rate>,<cell-write-rate>,<cell-read-rate>
 * @endverbatim
 */

void LoadMetricsRange::compute_and_store(TableMutator *mutator, time_t now,
                                         LoadFactors &load_factors,
                                         uint64_t disk_used, uint64_t memory_used) {
  bool update_start_row = false;
  String old_start_row, old_end_row;

  if (m_new_rows) {
    ScopedLock lock(m_mutex);
    uint8_t *oldbuf = m_buffer.release();
    update_start_row = true;
    old_start_row = m_start_row;
    old_end_row = m_end_row;
    initialize(m_table_id, m_new_start_row, m_new_end_row);
    m_new_rows = false;
    delete [] oldbuf;
  }

  if ((now - m_timestamp) <= 0)
    return;

  time_t rounded_time = (now+(Global::metrics_interval/2)) - ((now+(Global::metrics_interval/2))%Global::metrics_interval);
  double time_interval = (double)(now - m_timestamp);
  double scan_rate = (double)(load_factors.scans-m_load_factors.scans) / time_interval;
  double update_rate = (double)(load_factors.updates-m_load_factors.updates) / time_interval;
  double cell_read_rate = (double)(load_factors.cells_scanned-m_load_factors.cells_scanned) / time_interval;
  double cell_write_rate = (double)(load_factors.cells_written-m_load_factors.cells_written) / time_interval;
  double byte_read_rate = (double)(load_factors.bytes_scanned-m_load_factors.bytes_scanned) / time_interval;
  double byte_write_rate = (double)(load_factors.bytes_written-m_load_factors.bytes_written) / time_interval;
  double disk_byte_read_rate = (double)(load_factors.disk_bytes_read-m_load_factors.disk_bytes_read) / time_interval;

  String value = format("2:%ld,%llu,%llu,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f",
                        rounded_time, (Llu)disk_used, (Llu)memory_used,
                        disk_byte_read_rate, byte_write_rate, byte_read_rate,
                        update_rate, scan_rate, cell_write_rate, cell_read_rate);

  KeySpec key;
  String row = Global::location_initializer->get() + ":" + m_table_id;

  key.row = row.c_str();
  key.row_len = row.length();

  if (update_start_row) {
    try {
      // delete old entries
      key.flag = FLAG_DELETE_CELL;
      key.column_qualifier = old_end_row.c_str();
      key.column_qualifier_len = old_end_row.size();
      // delete old start row
      key.column_family = "range_start_row";
      mutator->set_delete(key);
      // delete old range metrics
      key.column_family = "range";
      mutator->set_delete(key);
      // delete old move
      key.column_family = "range_move";
      mutator->set_delete(key);

      // set new qualifier
      key.column_qualifier = m_end_row;
      key.column_qualifier_len = strlen(m_end_row);
      // insert new start row
      key.column_family = "range_start_row";
      mutator->set(key, (uint8_t *)m_start_row, strlen(m_start_row));
    }
    catch (Exception &e) {
      HT_ERROR_OUT << "Problem updating sys/RS_METRICS - " << e << HT_END;
    }
  }
  else {
    key.column_qualifier = m_end_row;
    key.column_qualifier_len = strlen(m_end_row);
  }


  key.column_family = "range";
  try {
    mutator->set(key, (uint8_t *)value.c_str(), value.length());
  }
  catch (Exception &e) {
    HT_ERROR_OUT << "Problem updating sys/RS_METRICS - " << e << HT_END;
  }

  m_timestamp = now;
  m_load_factors = load_factors;
}


void LoadMetricsRange::initialize(const String &table_id, const String &start_row,
    const String &end_row) {

  m_buffer.reserve(table_id.length() + 1 + start_row.length() + 1 + end_row.length() + 1);

  // save table ID
  m_table_id = (const char *)m_buffer.ptr;
  m_buffer.add(table_id.c_str(), table_id.length()+1);

  // save start row
  m_start_row = (const char *)m_buffer.ptr;
  m_buffer.add(start_row.c_str(), start_row.length()+1);

  // save end row
  m_end_row = (const char *)m_buffer.ptr;
  m_buffer.add(end_row.c_str(), end_row.length()+1);
}
