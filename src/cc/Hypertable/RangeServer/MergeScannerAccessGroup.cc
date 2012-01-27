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

#include "Common/Compat.h"
#include <cassert>

#include "Common/Logger.h"

#include "Hypertable/Lib/Key.h"

#include "MergeScannerAccessGroup.h"

using namespace Hypertable;


MergeScannerAccessGroup::MergeScannerAccessGroup(ScanContextPtr &scan_ctx, 
    bool return_deletes)
  : MergeScanner(scan_ctx), m_return_deletes(return_deletes),
    m_revs_count(0), m_revs_limit(0), m_prev_key(0), m_prev_cf(-1),
    m_no_forward(false), m_count_present(false), 
    m_skip_remaining_counter(false), m_counted_value(12),
    m_delete_present(false), m_deleted_row(0),
    m_deleted_column_family(0), m_deleted_cell(0), m_deleted_cell_version(0),
    m_scan_context(scan_ctx.get())
{ 
  m_start_timestamp = m_scan_context->time_interval.first;
  m_end_timestamp = m_scan_context->time_interval.second;
  m_revision = m_scan_context->revision;
}

bool 
MergeScannerAccessGroup::do_get(Key &key, ByteString &value) 
{
  // check if we have a counter result ready
  if (m_no_forward) {
    key = m_counted_key;
    value.ptr = m_counted_value.base;
    return true;
  }

  // otherwise pick the next key/value from the queue
  if (!m_queue.empty()) {
    const ScannerState &sstate = m_queue.top();
    key = sstate.key;
    value = sstate.value;
    return true;
  }

  return false;
}

void
MergeScannerAccessGroup::do_initialize()
{
  ScannerState sstate;
  int64_t cur_bytes=0;

  while (!m_queue.empty()) {
    sstate = m_queue.top();

    CellFilterInfo& cfi = m_scan_context->family_info[sstate.key.column_family_code];

    // update I/O tracking
    cur_bytes = sstate.key.length + sstate.value.length();
    io_add_input_cell(cur_bytes);

    // Only need to worry about counters if this scanner scans over a 
    // single access group since no counter will span multiple access grps

    if (sstate.key.timestamp < cfi.cutoff_time
        || sstate.key.timestamp < m_start_timestamp) {
      m_queue.pop();
      sstate.scanner->forward();
      if (sstate.scanner->get(sstate.key, sstate.value))
        m_queue.push(sstate);
      continue;
    }
    else if (sstate.key.flag == FLAG_DELETE_ROW) {
      update_deleted_row(sstate.key);
      if (!m_return_deletes) {
        forward();
        m_initialized = true;
        return;
      }
    }
    else if (sstate.key.flag == FLAG_DELETE_COLUMN_FAMILY) {
      update_deleted_column_family(sstate.key);
      if (!m_return_deletes) {
        forward();
        m_initialized = true;
        return;
      }
    }
    else if (sstate.key.flag == FLAG_DELETE_CELL) {
      update_deleted_cell(sstate.key);
      if (!m_return_deletes) {
        forward();
        m_initialized = true;
        return;
      }
    }
    else if (sstate.key.flag == FLAG_DELETE_CELL_VERSION) {
      update_deleted_cell_version(sstate.key);
      if (!m_return_deletes) {
        forward();
        m_initialized = true;
        return;
      }
    }
    else if (sstate.key.flag == FLAG_INSERT) {
      if (sstate.key.revision > m_revision
          || (sstate.key.timestamp >= m_end_timestamp 
            && !m_return_deletes)) {
        m_queue.pop();
        sstate.scanner->forward();
        if (sstate.scanner->get(sstate.key, sstate.value))
          m_queue.push(sstate);
        continue;
      }

      // keep track of revisions
      const uint8_t *latest_key = (const uint8_t *)sstate.key.row;
      size_t latest_key_len = sstate.key.flag_ptr - 
                (const uint8_t *)sstate.key.row + 1;

      if (m_prev_key.fill()==0) {
        m_prev_key.set(latest_key, latest_key_len);
        m_prev_cf = sstate.key.column_family_code;
        m_revs_count=0;
        m_revs_limit = cfi.max_versions;
      }
      else if (m_prev_key.fill() != latest_key_len ||
          memcmp(latest_key, m_prev_key.base, latest_key_len)) {
        m_prev_key.set(latest_key, latest_key_len);
        m_prev_cf = sstate.key.column_family_code;
        m_revs_count=0;
        m_revs_limit = cfi.max_versions;
      }
      m_revs_count++;
      if (m_revs_limit && m_revs_count > m_revs_limit && !cfi.counter) {
        m_queue.pop();
        sstate.scanner->forward();
        if (sstate.scanner->get(sstate.key, sstate.value))
          m_queue.push(sstate);
        continue;
      }

      // row set 
      if (!m_scan_context->rowset.empty()) {
        int cmp = 1;
        while (!m_scan_context->rowset.empty()
            && (cmp = strcmp(*m_scan_context->rowset.begin(), sstate.key.row)) < 0)
          m_scan_context->rowset.erase(m_scan_context->rowset.begin());
        if (cmp > 0) {
          m_queue.pop();
          sstate.scanner->forward();
          if (sstate.scanner->get(sstate.key, sstate.value))
            m_queue.push(sstate);
          continue;
        }
      }
      // row regexp
      if (m_scan_context->row_regexp)
        if (!RE2::PartialMatch(sstate.key.row, 
            *(m_scan_context->row_regexp))) {
          m_queue.pop();
          sstate.scanner->forward();
          if (sstate.scanner->get(sstate.key, sstate.value))
            m_queue.push(sstate);
          continue;
        }
      // column qualifier doesn't match
      if (!cfi.qualifier_matches(sstate.key.column_qualifier)) {
        m_queue.pop();
        sstate.scanner->forward();
        if (sstate.scanner->get(sstate.key, sstate.value))
          m_queue.push(sstate);
        continue;
      }
      // filter by value regexp last since its probly the most expensive
      if (m_scan_context->value_regexp && !cfi.counter) {
        const uint8_t *dptr;
        if (!RE2::PartialMatch(re2::StringPiece((const char *)sstate.value.str(),
                            sstate.value.decode_length(&dptr)), 
                            *(m_scan_context->value_regexp))) {
          m_queue.pop();
          sstate.scanner->forward();
          if (sstate.scanner->get(sstate.key, sstate.value))
            m_queue.push(sstate);
          continue;
        }
      }

      m_delete_present = false;
      m_prev_key.set(sstate.key.row, sstate.key.flag_ptr
                     - (const uint8_t *)sstate.key.row + 1);
      m_prev_cf = sstate.key.column_family_code;
      m_revs_limit = cfi.max_versions;

      // if counter then keep incrementing till we are ready with 1st kv pair
      if (cfi.counter) {
        start_count(sstate.key, sstate.value);
        forward();
        m_initialized = true;
        return;
      }
    }

    break;
  }

  io_add_output_cell(cur_bytes);
}

void 
MergeScannerAccessGroup::do_forward()
{
  ScannerState sstate;
  Key key;
  bool counter;
  int64_t cur_bytes;

  if (m_queue.empty()) {
    if (m_count_present)
      finish_count();
    else
      m_no_forward = false;
    return;
  }

  sstate = m_queue.top();

  // while the queue is not empty: pop the top element, forward it, 
  // re-insert it back into the queue
  while (true) {
    while (true) {
      m_queue.pop();

      // In some cases the forward might already be done and so the 
      // scanner shdn't be forwarded again. For example you know a counter 
      // is done only after forwarding to the 1st post counter cell or 
      // reaching the end of the scan.
      if (m_no_forward)
        m_no_forward = false;
      else
        sstate.scanner->forward();

      if (sstate.scanner->get(sstate.key, sstate.value))
        m_queue.push(sstate);

      if (m_queue.empty()) {
        // scan ended on a counter
        if (m_count_present)
          finish_count();
        return;
      }

      sstate = m_queue.top();

      // update I/O tracking
      cur_bytes = sstate.key.length + sstate.value.length();
      io_add_input_cell(cur_bytes);

      CellFilterInfo& cfi = m_scan_context->family_info[sstate.key.column_family_code];

      // we only need to care about counters for a MergeScanner which is 
      // merging over a single access group since no counter will span 
      // multiple access groups
      counter = cfi.counter;

      // apply the various filters...
      if(sstate.key.timestamp < cfi.cutoff_time
         || sstate.key.timestamp < m_start_timestamp) {
        continue;
      }
      else if (sstate.key.revision > m_revision
          || (sstate.key.timestamp >= m_end_timestamp 
            && (!m_return_deletes || sstate.key.flag == FLAG_INSERT))) {
        continue;
      }
      else if (sstate.key.flag == FLAG_DELETE_ROW) {
        if (matches_deleted_row(sstate.key)) {
          if (m_deleted_row_timestamp < sstate.key.timestamp)
            m_deleted_row_timestamp = sstate.key.timestamp;
        }
        else
          update_deleted_row(sstate.key);
        if (m_return_deletes)
          break;
      }
      else if (sstate.key.flag == FLAG_DELETE_COLUMN_FAMILY) {
        if (matches_deleted_column_family(sstate.key)) {
          if (m_deleted_column_family_timestamp < sstate.key.timestamp)
            m_deleted_column_family_timestamp = sstate.key.timestamp;
        }
        else
          update_deleted_column_family(sstate.key);
        if (m_return_deletes)
          break;
      }
      else if (sstate.key.flag == FLAG_DELETE_CELL) {
        if (matches_deleted_cell(sstate.key)) {
          if (m_deleted_cell_timestamp < sstate.key.timestamp)
            m_deleted_cell_timestamp = sstate.key.timestamp;
        }
        else
          update_deleted_cell(sstate.key);
        if (m_return_deletes)
          break;
      }
      else if (sstate.key.flag == FLAG_DELETE_CELL_VERSION) {
        if (matches_deleted_cell_version(sstate.key))
          m_deleted_cell_version_set.insert(sstate.key.timestamp);
        else
          update_deleted_cell_version(sstate.key);
        if (m_return_deletes)
          break;
      }
      else if (sstate.key.flag == FLAG_INSERT) {
        // this cell is not a delete and it is within the requested 
        // time interval.
        if (m_delete_present) {
          if (m_deleted_cell_version.fill() > 0) {
            if (!matches_deleted_cell_version(sstate.key)) {
              // we wont see the previously seen deleted cell version again
              m_deleted_cell_version.clear();
              m_deleted_cell_version_set.clear();
            }
            else if (m_deleted_cell_version_set.find(sstate.key.timestamp) !=
                     m_deleted_cell_version_set.end())
              // apply previously seen delete cell version to this cell
              continue;
          }
          if (m_deleted_cell.fill() > 0) {
            if (!matches_deleted_cell(sstate.key))
              // we wont see the previously seen deleted cell again
              m_deleted_cell.clear();
            else if (sstate.key.timestamp <= m_deleted_cell_timestamp)
              // apply previously seen delete cell to this cell
              continue;
          }
          if (m_deleted_column_family.fill() > 0) {
            if (!matches_deleted_column_family(sstate.key))
              // we wont see the previously seen deleted column family again
              m_deleted_column_family.clear();
            else if (sstate.key.timestamp <= m_deleted_column_family_timestamp)
              // apply previously seen delete column family to this cell
              continue;
          }
          if (m_deleted_row.fill() > 0) {
            if (!matches_deleted_row(sstate.key))
              // we wont see the previously seen deleted row family again
              m_deleted_row.clear();
            else if (sstate.key.timestamp <= m_deleted_row_timestamp)
              // apply previously seen delete row family to this cell
              continue;
          }
          if (m_deleted_cell_version.fill() == 0 
              && m_deleted_cell.fill() == 0 
              && m_deleted_column_family.fill() == 0 
              && m_deleted_row.fill() == 0)
            m_delete_present = false;
        }

        // keep track of revisions
        const uint8_t *latest_key = (const uint8_t *)sstate.key.row;
        size_t latest_key_len = sstate.key.flag_ptr -
                (const uint8_t *)sstate.key.row + 1;

        if (m_prev_key.fill()==0) {
          m_prev_key.set(latest_key, latest_key_len);
          m_prev_cf = sstate.key.column_family_code;
          m_revs_count=0;
          m_revs_limit = cfi.max_versions;
        }
        else if (m_prev_key.fill() != latest_key_len ||
            memcmp(latest_key, m_prev_key.base, latest_key_len)) {

          m_prev_key.set(latest_key, latest_key_len);
          m_prev_cf = sstate.key.column_family_code;
          m_revs_count=0;
          m_revs_limit = cfi.max_versions;
        }
        m_revs_count++;
        if (m_revs_limit && m_revs_count > m_revs_limit && !counter)
          continue;

        // row set
        if (!m_scan_context->rowset.empty()) {
          int cmp = 1;
          while (!m_scan_context->rowset.empty()
              && (cmp = strcmp(*m_scan_context->rowset.begin(),
                                sstate.key.row)) < 0)
            m_scan_context->rowset.erase(m_scan_context->rowset.begin());
          if (cmp > 0)
            continue;
        }
        // row regexp
        if (m_scan_context->row_regexp) {
          bool cached, match;
          m_regexp_cache.check_rowkey(sstate.key.row, &cached, &match);
          if (!cached) {
            match = RE2::PartialMatch(sstate.key.row, 
                        *(m_scan_context->row_regexp));
            m_regexp_cache.set_rowkey(sstate.key.row, match);
          }
          if (!match)
            continue;
        }
        // column qualifier match
        if(cfi.has_qualifier_regexp_filter()) {
          bool cached, match;
          m_regexp_cache.check_column(sstate.key.column_family_code,
              sstate.key.column_qualifier, &cached, &match);
          if (!cached) {
            match = cfi.qualifier_matches(sstate.key.column_qualifier);
            m_regexp_cache.set_column(sstate.key.column_family_code,
                sstate.key.column_qualifier, match);
          }
          if (!match)
            continue;
        }
        else if (!cfi.qualifier_matches(sstate.key.column_qualifier)) {
          continue;
        }

        // filter but value regexp last since its probly the most expensive
        if (m_scan_context->value_regexp && !counter) {
          const uint8_t *dptr;
          if (!RE2::PartialMatch(re2::StringPiece((const char *)sstate.value.str(),
                            sstate.value.decode_length(&dptr)), 
                            *(m_scan_context->value_regexp)))
            continue;
        }
        break;
      }
      if (m_delete_present && m_return_deletes)
        break;
    }

    // deal with counters. apply row_limit but not revs/cell_limit_per_family
    if (m_count_present) {
      if(counter && matches_counted_key(sstate.key)) {
        if (sstate.key.flag == FLAG_INSERT) {
          // keep incrementing
          increment_count(sstate.key, sstate.value);
          continue;
        }
      }
      else {
        // count done, new count seen but not started
        finish_count();
        break;
      }
    }
    else if (counter && sstate.key.flag == FLAG_INSERT) {
      // start new count and loop
      start_count(sstate.key, sstate.value);
      continue;
    }

    break;
  }

  io_add_output_cell(cur_bytes);
}

