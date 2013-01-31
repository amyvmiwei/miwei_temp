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
#include <vector>

#include "Common/Error.h"
#include "Common/String.h"

#include "Table.h"
#include "TableScannerAsync.h"
#include "IndexScannerCallback.h"
#include "LoadDataEscape.h"

extern "C" {
#include <poll.h>
}

using namespace Hypertable;


/**
 *
 */
TableScannerAsync::TableScannerAsync(Comm *comm, 
      ApplicationQueueInterfacePtr &app_queue, Table *table,
      RangeLocatorPtr &range_locator, const ScanSpec &scan_spec, 
      uint32_t timeout_ms, ResultCallback *cb, int flags)
  : m_bytes_scanned(0), m_current_scanner(0), m_outstanding(0), 
    m_error(Error::OK), m_cancelled(false), m_use_index(false)
{
  ScopedLock lock(m_mutex);
  ScanSpecBuilder index_spec;
  bool use_qualifier = false;
  const ScanSpec *pspec = &scan_spec;

  HT_ASSERT(timeout_ms);

  // can we optimize this query with an index?
  if (!(flags&Table::SCANNER_FLAG_IGNORE_INDEX)
      && (table->has_index_table() || table->has_qualifier_index_table())
      && use_index(table, scan_spec, index_spec, &use_qualifier)) {
    pspec = &index_spec.get();
    m_use_index = true;

    // create a ResultCallback object which will load the keys from
    // the index, then sort and verify them
    cb = new IndexScannerCallback(table, scan_spec, cb, timeout_ms, 
                        use_qualifier);

    // get the index table
    if (use_qualifier)
      table = table->get_qualifier_index_table().get();
    else
      table = table->get_index_table().get();

    // fall through and retrieve values from the primary table. the 
    // IndexScannerCallback object will then transform the results and forward 
    // them to the original callback
  }

  m_cb = cb;
  m_table = table;
  m_scan_spec_builder = *pspec;

  init(comm, app_queue, table, range_locator, *pspec, timeout_ms, cb);
}

bool TableScannerAsync::use_index(TablePtr table, const ScanSpec &primary_spec, 
                ScanSpecBuilder &index_spec, bool *use_qualifier)
{
  HT_ASSERT(!table->schema()->need_id_assignment());

  // never use an index if the row key is specified
  if (primary_spec.row_intervals.size())
    return false;

  index_spec.set_keys_only(true);
  index_spec.add_column("v1");

  // if a column qualifier is specified then make sure that all columns have
  // a qualifier index. only support prefix column qualifiers or exact
  // qualifiers, but not the regexp qualifier
  if (primary_spec.columns.size()) {
    for (size_t i = 0; i < primary_spec.columns.size(); i++) {
      Schema::ColumnFamily *cf = 0;
      const char *qualifier_start = strchr(primary_spec.columns[i], ':');
      if (!qualifier_start) {
        *use_qualifier = false;
        break;
      }

      String column(primary_spec.columns[i], qualifier_start);
      cf = table->schema()->get_column_family(column);
      if (!cf || !cf->has_qualifier_index) {
        *use_qualifier = false;
        break;
      }

      qualifier_start++;
      String qualifier(qualifier_start);
      boost::algorithm::trim_if(qualifier, boost::algorithm::is_any_of("'\""));

      if (qualifier[0] == '/') {
        *use_qualifier = false;
        break;
      }

      // prefix match: create row interval ["%d,qualifier", ..)
      if (qualifier[0] == '^') {
        String q(qualifier.c_str() + 1);
        trim_if(q, boost::algorithm::is_any_of("'\""));
        String s = format("%d,%s", (int)cf->id, q.c_str());
        add_index_row(index_spec, s.c_str());
      }
      // exact match:  create row interval ["%d,qualifier\t", ..)
      else {
        String s = format("%d,%s\t", (int)cf->id, qualifier.c_str());
        add_index_row(index_spec, s.c_str());
      }

      *use_qualifier = true;
    }
  }

  if (*use_qualifier)
    return true;

  index_spec.get().row_regexp = 0;
  index_spec.get().columns.resize(0);

  // for value prefix queries we require normal indicies for ALL scanned columns
  if (primary_spec.column_predicates.size() == 1 && primary_spec.columns.size()) {

    foreach_ht (const ColumnPredicate &cp, primary_spec.column_predicates) {
      Schema::ColumnFamily *cf=table->schema()->get_column_family(
                                cp.column_family);
      if (!cf || !cf->has_index)
        return false;

      if (cp.operation != ColumnPredicate::EXACT_MATCH &&
          cp.operation != ColumnPredicate::PREFIX_MATCH)
        return false;

      // every \t in the original value gets escaped
      const char *value;
      size_t valuelen;
      LoadDataEscape lde;
      lde.escape(cp.value, cp.value_len, &value, &valuelen);

      // exact match:  create row interval ["%d,value\t", ..)
      // prefix match: create row interval ["%d,value", ..)
      StaticBuffer sb(valuelen + 5);
      char *p = (char *)sb.base;
      sprintf(p, "%d,", (int)cf->id);
      p     += strlen(p);
      memcpy(p, value, valuelen);
      p     += valuelen;
      if (cp.operation == ColumnPredicate::EXACT_MATCH)
        *p++  = '\t';
      *p++  = '\0';

      add_index_row(index_spec, (const char *)sb.base);
    }

    *use_qualifier = false;
    return true;
  }

  return false;
}

void TableScannerAsync::add_index_row(ScanSpecBuilder &ssb, const char *row)
{
  // this code is identical to the RELOP_SW handler in HqlParser.h
  String tmp;
  RowInterval ri;
  ri.start = row;
  ri.start_inclusive = true;
  const char *str = row;
  const char *end = str + strlen(row);
  const char *ptr;
  for (ptr = end - 1; ptr > str; --ptr) {
    if (::uint8_t(*ptr) < 0xffu) {
      tmp = String(str, ptr - str);
      tmp.append(1, (*ptr)+1);
      ri.end = tmp.c_str();
      ri.end_inclusive = false;
      break;
    }
  }
  if (ptr == str) {
    tmp = row;
    tmp.append(4, (char)0xff);
    ri.end = tmp.c_str();
    ri.end_inclusive = false;
  }
  ssb.add_row_interval(ri.start, ri.start_inclusive,
                    ri.end, ri.end_inclusive);
}

void TableScannerAsync::init(Comm *comm, ApplicationQueueInterfacePtr &app_queue, 
        Table *table, RangeLocatorPtr &range_locator, 
        const ScanSpec &scan_spec, uint32_t timeout_ms, ResultCallback *cb)
{
  int scanner_id = 0;
  IntervalScannerAsyncPtr ri_scanner;
  ScanSpec interval_scan_spec;
  Timer timer(timeout_ms);
  bool current_set = false;

  m_cb->increment_outstanding();
  m_cb->register_scanner(this);

  try {
    if (scan_spec.row_intervals.empty()) {
      if (scan_spec.cell_intervals.empty()) {
        ri_scanner = 0;
        ri_scanner = new IntervalScannerAsync(comm, app_queue, table, 
                        range_locator, scan_spec, timeout_ms, 
                        !current_set, this, scanner_id++);

        current_set = true;
        m_interval_scanners.push_back(ri_scanner);
        m_outstanding++;
      }
      else {
        for (size_t i=0; i<scan_spec.cell_intervals.size(); i++) {
          scan_spec.base_copy(interval_scan_spec);
          interval_scan_spec.cell_intervals.push_back(
              scan_spec.cell_intervals[i]);
          ri_scanner = 0;
          ri_scanner = new IntervalScannerAsync(comm, app_queue, table, 
                        range_locator, interval_scan_spec, timeout_ms,
                        !current_set, this, scanner_id++);
          current_set = true;
          m_interval_scanners.push_back(ri_scanner);
          m_outstanding++;
        }
      }
    }
    else if (scan_spec.scan_and_filter_rows) {
      ScanSpec rowset_scan_spec;
      scan_spec.base_copy(rowset_scan_spec);
      rowset_scan_spec.row_intervals.reserve(scan_spec.row_intervals.size());
      foreach_ht (const RowInterval& ri, scan_spec.row_intervals) {
        if (ri.start != ri.end && strcmp(ri.start, ri.end) != 0) {
          scan_spec.base_copy(interval_scan_spec);
          interval_scan_spec.row_intervals.push_back(ri);
          ri_scanner = 0;
          ri_scanner = new IntervalScannerAsync(comm, app_queue, table,
                        range_locator, interval_scan_spec, timeout_ms,
                        !current_set, this, scanner_id++);
          current_set = true;
          m_interval_scanners.push_back(ri_scanner);
          m_outstanding++;
        }
        else
          rowset_scan_spec.row_intervals.push_back(ri);
      }
      if (rowset_scan_spec.row_intervals.size()) {
        ri_scanner = 0;
        ri_scanner = new IntervalScannerAsync(comm, app_queue, table, 
                        range_locator, rowset_scan_spec, timeout_ms, 
                        !current_set, this, scanner_id++);
        current_set = true;
        m_interval_scanners.push_back(ri_scanner);
        m_outstanding++;
      }
    }
    else {
      for (size_t i=0; i<scan_spec.row_intervals.size(); i++) {
        scan_spec.base_copy(interval_scan_spec);
        interval_scan_spec.row_intervals.push_back(scan_spec.row_intervals[i]);
        ri_scanner = 0;
        ri_scanner = new IntervalScannerAsync(comm, app_queue, table, 
                        range_locator, interval_scan_spec, timeout_ms, 
                        !current_set, this, scanner_id++);
        current_set = true;
        m_interval_scanners.push_back(ri_scanner);
        m_outstanding++;
      }
    }
  }
  catch (Exception &e) {
    m_error = e.code();
    m_error_msg = e.what();
    if (ri_scanner && ri_scanner->has_outstanding_requests()) {
      m_interval_scanners.push_back(ri_scanner);
      m_outstanding++;
    }
    if (m_outstanding == 0)
      maybe_callback_error(0, false);
  }
}

TableScannerAsync::~TableScannerAsync() {
  try {
    cancel();
    wait_for_completion();
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
  }
  if (m_use_index) {
    ((IndexScannerCallback *)m_cb)->shutdown();
    delete m_cb;
    m_cb = 0;
  }
}

void TableScannerAsync::cancel() {
  ScopedLock lock(m_cancel_mutex);
  m_cancelled = true;
}

bool TableScannerAsync::is_cancelled() {
  ScopedLock lock(m_cancel_mutex);
  return m_cancelled;
}

void TableScannerAsync::handle_error(int scanner_id, int error, const String &error_msg,
                                     bool is_create) {
  bool cancelled = is_cancelled();
  ScopedLock lock(m_mutex);
  bool abort = false;
  bool next = false;

  // if we've already seen an error or the scanner has been cacncelled
  if (m_error != Error::OK || cancelled) {
    abort=true;
    next = m_interval_scanners[scanner_id]->abort(is_create);
  }
  else {
    switch(error) {
      case (Error::TABLE_NOT_FOUND):
      case (Error::RANGESERVER_TABLE_NOT_FOUND):
        if (m_table->auto_refresh() && is_create)
        abort = !(m_interval_scanners[scanner_id]->retry_or_abort(true, true, 
                    is_create, &next, error));
        else {
          next = m_interval_scanners[scanner_id]->abort(is_create);
          abort = true;
        }
        break;
      case (Error::RANGESERVER_GENERATION_MISMATCH):
        if (m_table->auto_refresh() && is_create)
        abort = !(m_interval_scanners[scanner_id]->retry_or_abort(true, false, 
                    is_create, &next, error));
        else {
          next = m_interval_scanners[scanner_id]->abort(is_create);
          abort = true;
        }
        break;
      case(Error::RANGESERVER_INVALID_SCANNER_ID):
        abort = !(m_interval_scanners[scanner_id]->is_destroyed_scanner(is_create));
        next = !m_interval_scanners[scanner_id]->has_outstanding_requests();
        break;
      case(Error::RANGESERVER_RANGE_NOT_FOUND):
      case(Error::COMM_NOT_CONNECTED):
      case(Error::COMM_BROKEN_CONNECTION):
        abort = !(m_interval_scanners[scanner_id]->retry_or_abort(false, true, 
                    is_create, &next, error));
        break;

      default:
        Exception e(error, error_msg);
        HT_ERROR_OUT <<  "Received error: is_create=" << is_create << " - "<< e << HT_END;
        next = m_interval_scanners[scanner_id]->abort(is_create);
        abort = true;
    }
  }

  // if we've seen an error before then don't bother with callback
  if (m_error != Error::OK || cancelled) {
    maybe_callback_error(scanner_id, next);
    if (next && scanner_id == m_current_scanner)
      move_to_next_interval_scanner(scanner_id);
    return;
  }
  else if (abort) {
    Exception e(error, error_msg);
    m_error = error;
    m_error_msg = error_msg;
    HT_ERROR_OUT << e << HT_END;
    maybe_callback_error(scanner_id, next);
    if (next && scanner_id == m_current_scanner)
      move_to_next_interval_scanner(scanner_id);
  }
  else if (next && scanner_id == m_current_scanner) {
    move_to_next_interval_scanner(scanner_id);
  }
}

void TableScannerAsync::handle_timeout(int scanner_id, const String &error_msg, bool is_create) {
  bool cancelled = is_cancelled();
  ScopedLock lock(m_mutex);
  bool next;

  next = m_interval_scanners[scanner_id]->abort(is_create);
  // if we've seen an error before or scanner has been cancelled then don't bother with callback
  if (m_error != Error::OK || cancelled) {
   maybe_callback_error(scanner_id, next);
   return;
  }

  HT_ERROR_OUT << "Unable to complete scan request within " << m_timeout_ms
               << " - " << error_msg << HT_END;
  m_error = Error::REQUEST_TIMEOUT;
  maybe_callback_error(scanner_id, next);
  if (next && scanner_id == m_current_scanner)
    move_to_next_interval_scanner(scanner_id);

}

void TableScannerAsync::handle_result(int scanner_id, EventPtr &event, bool is_create) {

  bool cancelled = is_cancelled();
  ScopedLock lock(m_mutex);
  ScanCellsPtr cells;

  // abort interval scanners if we've seen an error previously or scanned has been cancelled
  bool abort = (m_error != Error::OK || cancelled);

  bool next;
  bool do_callback = false;
  int current_scanner = scanner_id;

  try {
    // If the scan already encountered an error/cancelled
    // don't bother calling into callback anymore
    if (abort) {
      next = m_interval_scanners[scanner_id]->abort(is_create);
      if (cancelled && m_error == Error::OK) {
        // scanner was cancelled and is over
        if (next && m_outstanding==1) {
          do_callback = true;
          cells = new ScanCells;
        }
        else
          do_callback = false;
        maybe_callback_ok(scanner_id, next, do_callback, cells);
      }
      else
        maybe_callback_error(scanner_id, next);
    }
    else {
      // send results to interval scanner
      next = m_interval_scanners[scanner_id]->handle_result(&do_callback, cells, event, is_create);
      maybe_callback_ok(scanner_id, next, do_callback, cells);
    }

    if (next)
      move_to_next_interval_scanner(current_scanner);
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    m_error = e.code();
    m_error_msg = e.what();
    next = !m_interval_scanners[current_scanner]->has_outstanding_requests();
    maybe_callback_error(current_scanner, next);
    throw;
  }
}

void TableScannerAsync::maybe_callback_error(int scanner_id, bool next) {
  bool eos = false;
  // ok to update m_outstanding since caller has locked mutex
  if (next) {
    HT_ASSERT(m_outstanding>0 && m_interval_scanners[scanner_id] != 0);
    m_outstanding--;
    m_interval_scanners[scanner_id] = 0;
  }

  if (m_outstanding == 0) {
    eos = true;
  }

  m_cb->scan_error(this, m_error, m_error_msg, eos);

  if (eos) {
    m_cb->deregister_scanner(this);
    m_cb->decrement_outstanding();
    m_cond.notify_all();
  }
}

void TableScannerAsync::maybe_callback_ok(int scanner_id, bool next, bool do_callback, ScanCellsPtr &cells) {
  bool eos = false;
  // ok to update m_outstanding since caller has locked mutex
  if (next) {
    HT_ASSERT(m_outstanding>0 && m_interval_scanners[scanner_id] != 0);
    m_outstanding--;
    m_interval_scanners[scanner_id] = 0;
  }

  if (m_outstanding == 0) {
    eos = true;
  }

  if (do_callback) {
    if (eos)
      cells->set_eos();
    HT_ASSERT(cells != 0);
    m_cb->scan_ok(this, cells);
  }

  if (m_outstanding==0) {
    m_cb->deregister_scanner(this);
    m_cb->decrement_outstanding();
    m_cond.notify_all();
  }
}

void TableScannerAsync::wait_for_completion() {
  ScopedLock lock(m_mutex);
  while (m_outstanding != 0)
    m_cond.wait(lock);
}

String TableScannerAsync::get_table_name() const {
  return m_table->get_name();
}

void TableScannerAsync::move_to_next_interval_scanner(int current_scanner) {
  bool next = true;
  bool cancelled = is_cancelled();
  bool do_callback;
  ScanCellsPtr cells;
  bool abort = cancelled || (m_error != Error::OK);

  while (next && m_outstanding && current_scanner < ((int)m_interval_scanners.size())-1) {
    current_scanner++;
    // unless the scan has been aborted we should be going through scanners in order
    if (current_scanner != m_current_scanner+1) {
      HT_ASSERT(abort);
      break;
    }
    m_current_scanner = current_scanner;

    if (m_interval_scanners[current_scanner] !=0) {
      next = m_interval_scanners[current_scanner]->set_current(&do_callback, cells, abort);
      HT_ASSERT(do_callback || !next || abort);

      // this is the last outstanding scanner and the scan was cancelled
      // or failed with an error
      if (next 
          && m_outstanding==1 
          && ((cancelled && m_error == Error::OK)
            || m_error != Error::OK)) {
        do_callback = true;
        cells = new ScanCells;
      }
      maybe_callback_ok(m_current_scanner, next, do_callback, cells);
    }
  }

  // if we skipped ALL outstanding scanners then make sure the "eos" marker
  // is sent to the caller, and m_outstanding is decremented
  if (next 
      && m_outstanding == 1
      && current_scanner == ((int)m_interval_scanners.size() - 1) 
      && !cells) {
    cells = new ScanCells;
    maybe_callback_ok(m_current_scanner, true, true, cells);
    m_current_scanner = (int)m_interval_scanners.size() - 1;
  }
}

