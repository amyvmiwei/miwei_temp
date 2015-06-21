/*
 * Copyright (C) 2007-2015 Hypertable, Inc.
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

#include <Common/Compat.h>

#include "Future.h"
#include "TableScannerAsync.h"
#include "TableMutatorAsync.h"

#include <Common/Time.h>

#include <chrono>

using namespace Hypertable;
using namespace std;

bool Future::get(ResultPtr &result) {
  unique_lock<mutex> lock(m_outstanding_mutex);
  size_t mem_result=0;

  while (true) {

    // wait till we have results to serve
    m_outstanding_cond.wait(lock, [this](){
        return !_is_empty() || _is_done() || _is_cancelled(); });

    if (_is_cancelled())
      return false;
    if (_is_empty() && _is_done())
      return false;
    result = m_queue.front();
    mem_result = result->memory_used();
    m_queue.pop_front();
    m_memory_used -= mem_result;
    HT_ASSERT(m_memory_used >= 0);
    // wake a thread blocked on queue space
    m_outstanding_cond.notify_one();

    if (result->is_error())
      break;
    else if (result->is_scan()) {
      TableScannerAsync *scanner = result->get_scanner();
      // ignore result if scanner has been cancelled
      if (!scanner || !scanner->is_cancelled()) // scanner must be alive
        break;
    }
    else if (result->is_update()) {
      TableMutatorAsync *mutator = result->get_mutator();
      // check if alive mutator has been cancelled
      if (!mutator || m_mutator_map.find((uint64_t)mutator) == m_mutator_map.end() ||
        !mutator->is_cancelled())
        break;
    }
  }
  return true;
}

bool Future::get(ResultPtr &result, uint32_t timeout_ms, bool &timed_out) {

  if (timeout_ms == 0)
    return get(result);

  {
    unique_lock<mutex> lock(m_outstanding_mutex);

    timed_out = false;

    size_t mem_result=0;

    auto wait_time = chrono::system_clock::now() + chrono::milliseconds(timeout_ms);

    while (true) {
      // wait till we have results to serve
      while(_is_empty() && !_is_done() && !_is_cancelled()) {
        timed_out = m_outstanding_cond.wait_until(lock, wait_time) == std::cv_status::timeout;
	if (timed_out)
	  return _is_done();
      }
      if (_is_cancelled())
	return false;
      if (_is_empty() && _is_done())
	return false;
      result = m_queue.front();
      mem_result = result->memory_used();
      m_queue.pop_front();
      m_memory_used -= mem_result;
      HT_ASSERT(m_memory_used >= 0);
      // wake a thread blocked on queue space
      m_outstanding_cond.notify_one();

      if (result->is_error())
	break;
      if (result->is_scan()) {
	TableScannerAsync *scanner = result->get_scanner();
	// ignore result if scanner has been cancelled
	if (!scanner || !scanner->is_cancelled()) // scanner must be alive
	  break;
      }
      else if (result->is_update()) {
	TableMutatorAsync *mutator = result->get_mutator();
	// check if alive mutator has been cancelled
	if (!mutator || m_mutator_map.find((uint64_t)mutator) == m_mutator_map.end() ||
	    !mutator->is_cancelled())
	  break;
      }
    }
    // wake a thread blocked on queue space
    m_outstanding_cond.notify_one();
    return true;
  }
}

void Future::scan_ok(TableScannerAsync *scanner, ScanCellsPtr &cells) {
  ResultPtr result = make_shared<Result>(scanner, cells);
  enqueue(result);
}

void Future::enqueue(ResultPtr &result) {
  unique_lock<mutex> lock(m_outstanding_mutex);
  size_t mem_result = result->memory_used();

  m_outstanding_cond.wait(lock, [this](){
      return has_remaining_capacity() || _is_cancelled(); });

  if (!_is_cancelled()) {
    m_queue.push_back(result);
    m_memory_used += mem_result;
  }
  m_outstanding_cond.notify_one();
}

void Future::scan_error(TableScannerAsync *scanner, int error, const string &error_msg,
                        bool eos) {
  ResultPtr result = make_shared<Result>(scanner, error, error_msg);
  enqueue(result);
}

void Future::update_ok(TableMutatorAsync *mutator) {
  ResultPtr result = make_shared<Result>(mutator);
  enqueue(result);
}

void Future::update_error(TableMutatorAsync *mutator, int error, FailedMutations &failures) {
  ResultPtr result = make_shared<Result>(mutator, error, failures);
  enqueue(result);
}

void Future::cancel() {
  lock_guard<mutex> lock(m_outstanding_mutex);
  m_cancelled = true;
  ScannerMap::iterator s_it = m_scanner_map.begin();
  while (s_it != m_scanner_map.end()) {
    s_it->second->cancel();
    s_it++;
  }
  MutatorMap::iterator m_it = m_mutator_map.begin();
  while (m_it != m_mutator_map.end()) {
    m_it->second->cancel();
    m_it++;
  }
  m_queue.clear();
  m_memory_used = 0;

  m_outstanding_cond.notify_all();
}

void Future::register_mutator(TableMutatorAsync *mutator) {
  lock_guard<mutex> lock(m_outstanding_mutex);
  uint64_t addr = (uint64_t) mutator;
  MutatorMap::iterator it = m_mutator_map.find(addr);
  if (m_cancelled)
    HT_THROWF(Error::CANCELLED,
              "Attempt to register mutator with cancelled future %lld",
              (Lld)reinterpret_cast<int64_t>(this));
  HT_ASSERT(it == m_mutator_map.end());
  m_mutator_map[addr] = mutator;
}

void Future::deregister_mutator(TableMutatorAsync *mutator) {
  lock_guard<mutex> lock(m_outstanding_mutex);
  uint64_t addr = (uint64_t) mutator;
  MutatorMap::iterator it = m_mutator_map.find(addr);
  HT_ASSERT(it != m_mutator_map.end());
  m_mutator_map.erase(it);
}
void Future::register_scanner(TableScannerAsync *scanner) {
  lock_guard<mutex> lock(m_outstanding_mutex);
  uint64_t addr = (uint64_t) scanner;
  ScannerMap::iterator it = m_scanner_map.find(addr);
  if (m_cancelled)
    HT_THROWF(Error::CANCELLED,
              "Attempt to register scanner with cancelled future %lld",
              (Lld)reinterpret_cast<int64_t>(this));
  HT_ASSERT(it == m_scanner_map.end());
  m_scanner_map[addr] = scanner;
}

void Future::deregister_scanner(TableScannerAsync *scanner) {
  lock_guard<mutex> lock(m_outstanding_mutex);
  uint64_t addr = (uint64_t) scanner;
  ScannerMap::iterator it = m_scanner_map.find(addr);
  HT_ASSERT(it != m_scanner_map.end());
  m_scanner_map.erase(it);
}
