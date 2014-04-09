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
/// Definitions for MergeScannerRange.
/// This file contains type definitions for MergeScannerRange, a class used for
/// performing a scan over a range.

#include <Common/Compat.h>
#include "MergeScannerRange.h"

#include <Hypertable/Lib/Key.h>

#include <Common/Logger.h>

#include <cassert>

using namespace Hypertable;
using namespace std;

MergeScannerRange::MergeScannerRange(const string &table_id, ScanContextPtr &scan_ctx) 
  : MergeScanner(scan_ctx), m_cell_offset(0), m_cell_skipped(0), 
    m_cell_count(0), m_cell_limit(0), m_row_offset(0), m_row_skipped(0),
    m_row_count(0), m_row_limit(0), m_cell_count_per_family(0), 
    m_cell_limit_per_family(0), m_prev_key(0), m_prev_timestamp(TIMESTAMP_NULL),
    m_prev_cf(-1), m_skip_this_row(false) {
  if (scan_ctx->spec != 0) {
    m_cell_limit = scan_ctx->spec->cell_limit;
    m_row_limit = scan_ctx->spec->row_limit;
    m_cell_limit_per_family = scan_ctx->spec->cell_limit_per_family;
    m_row_offset = scan_ctx->spec->row_offset;
    m_cell_offset = scan_ctx->spec->cell_offset;

    if (scan_ctx->spec->rebuild_indices) {
      bool has_index = false;
      bool has_qualifier_index = false;

      for (auto cf : scan_ctx->schema->get_column_families()){
        if (!cf || cf->deleted)
          continue;
        if (scan_ctx->spec->rebuild_indices.value_index() && cf->has_index) {
          has_index = true;
          if (has_qualifier_index)
            break;
        }
        if (scan_ctx->spec->rebuild_indices.qualifier_index() && cf->has_qualifier_index) {
          has_qualifier_index = true;
          if (has_index)
            break;
        }
      }
      if (has_index || has_qualifier_index)
        m_index_updater =
          IndexUpdaterFactory::create(table_id, scan_ctx->schema,
                                      has_index, has_qualifier_index);
    }
  }
}

bool 
MergeScannerRange::do_get(Key &key, ByteString &value) 
{
  if (!m_queue.empty()) {
    const ScannerState &sstate = m_queue.top();
    key = sstate.key;
    value = sstate.value;
    return true;
  }

  return false;
}

void
MergeScannerRange::do_initialize()
{
  int64_t cur_bytes;
  ScannerState sstate;

  if (m_queue.empty())
    return;

  sstate = m_queue.top();

  // update I/O tracking
  cur_bytes = sstate.key.length + sstate.value.length();
  io_add_input_cell(cur_bytes);

  // if a new cell was inserted then store the column family and the
  // key; this is needed in do_forward() to figure out if the next row
  // has a new column family or column qualifier
  if (sstate.key.flag == FLAG_INSERT) {
    assert(m_prev_key.fill()==0);

    m_cell_count_per_family = 1;
    m_prev_key.set(sstate.key.row, (sstate.key.flag_ptr+1)
                   - (const uint8_t *)sstate.key.row);
    m_prev_cf = sstate.key.column_family_code;
    m_prev_timestamp = sstate.key.timestamp;

    if (m_row_offset) {
      m_skip_this_row = true;
      m_row_skipped++;
    }

    if (m_cell_offset)
      m_cell_skipped = 1;
    else if (!m_row_offset && m_cell_limit)
      m_cell_count = 1;

    if (m_row_limit && !m_row_offset && !m_cell_offset)
      m_row_count = 1;
  }

  io_add_output_cell(cur_bytes);

  // was OFFSET or CELL_OFFSET or index rebuild specified? then move forward and
  // skip
  if (m_cell_offset || m_row_offset || m_index_updater)
    do_forward();
}

void 
MergeScannerRange::do_forward() 
{
  int64_t cur_bytes;
  ScannerState sstate;
  Key key;

forward:
  // empty queue? return to caller
  if (m_queue.empty())
    return;
  sstate = m_queue.top();

  // while the queue is not empty: pop the top element, forward it
  // and then re-insert it back into the queue
  while (true) {
    bool new_row = false;
    bool new_cf = false;
    bool new_cq = false;

    m_queue.pop();

    sstate.scanner->forward();
    if (sstate.scanner->get(sstate.key, sstate.value))
      m_queue.push(sstate);

    // empty queue? return to caller
    if (m_queue.empty())
      return;

    sstate = m_queue.top();

    // update the I/O tracking
    cur_bytes = sstate.key.length + sstate.value.length();
    io_add_input_cell(cur_bytes);

    // If this scan is for rebuilding indexes, update indexes and continue
    if (m_index_updater) {
      assert(sstate.key.flag == FLAG_INSERT);
      m_index_updater->add(sstate.key, sstate.value);
      continue;
    }

    // check if this insert starts a new row, a new column family
    // or a new column qualifier, and make sure that the limits
    // are respected
    //
    // if the MergeScannerAccessGroup returns deleted keys then they will 
    // be processed below.
    if (sstate.key.flag == FLAG_INSERT) {
      const uint8_t *latest_key = (const uint8_t *)sstate.key.row;
      size_t latest_key_len = (sstate.key.flag_ptr+1) - 
        (const uint8_t *)sstate.key.row;

      if (m_prev_key.fill()==0) {
        new_row = new_cf = new_cq = true;
        m_cell_count_per_family = 1;
        if (m_row_offset && m_row_skipped < m_row_offset) {
          m_skip_this_row = true;
          m_row_skipped++;
        }
        else
          m_skip_this_row = false;
        m_prev_key.set(latest_key, latest_key_len);
        m_prev_cf = sstate.key.column_family_code;
        m_prev_timestamp = sstate.key.timestamp;
      }
      else if (m_prev_key.fill() != latest_key_len ||
               memcmp(latest_key, m_prev_key.base, latest_key_len)) {
        new_cq = true;

        if (strcmp(sstate.key.row, (const char *)m_prev_key.base)) {
          new_row = true;
          new_cf = true;
          m_cell_count_per_family = 1;
          if (m_row_offset && m_row_skipped < m_row_offset) {
            m_skip_this_row = true;
            m_row_skipped++;
          }
          else
            m_skip_this_row = false;
        }
        else if (sstate.key.column_family_code != m_prev_cf) {
          new_cf = true;
          m_cell_count_per_family = 1;
        }

        m_prev_key.set(latest_key, latest_key_len);
        m_prev_cf = sstate.key.column_family_code;
        m_prev_timestamp = sstate.key.timestamp;
      }
      else if (m_prev_timestamp != sstate.key.timestamp)
        m_prev_timestamp = sstate.key.timestamp;
      else
        continue;
    }

    bool incr_cf_count = false;

    // check ROW_OFFSET
    if (m_skip_this_row)
      continue;

    if ((!m_cell_offset && m_row_limit) || m_cell_limit_per_family) {
      if (new_row) {
        m_row_count++;
        if (m_row_limit && m_row_count > m_row_limit)
          m_done = true;
        break;
      }
      else if (m_cell_limit_per_family) {
        if (!new_cf)
          incr_cf_count = true;
      }
    }

    if (incr_cf_count) {
      m_cell_count_per_family++;
      if (m_cell_count_per_family > m_cell_limit_per_family)
        continue;
    }

    break;
  }

  if (!m_done) {
    // check CELL_OFFSET and call function recursively if we need to skip
    // more cells
    if (m_cell_offset != 0 && m_cell_skipped < m_cell_offset) {
      m_cell_skipped++;
      goto forward;
    }

    if (m_cell_limit)
      m_cell_count++;
  }

  io_add_output_cell(cur_bytes);

  if (m_cell_limit != 0 && m_cell_count > m_cell_limit)
    m_done = true;
}
