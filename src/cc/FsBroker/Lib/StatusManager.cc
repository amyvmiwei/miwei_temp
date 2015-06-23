/* -*- c++ -*-
 * Copyright (C) 2007-2015 Hypertable, Inc.
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

/// @file
/// Definitions for StatusManager.
/// This file contains definitions for StatusManager, a class for managing the
/// status of a file system broker.

#include <Common/Compat.h>

#include "StatusManager.h"

#include <Common/String.h>

#include <cstring>

using namespace Hypertable;
using namespace Hypertable::FsBroker::Lib;
using namespace std;

namespace {
  // Minimum elapsed time before transitioning back to OK status
  const chrono::steady_clock::duration CLEAR_CHANGE_INTERVAL = chrono::milliseconds(60000);
}

StatusManager::StatusManager() {
  m_current_status = m_status.get();
}

void StatusManager::set_read_status(Status::Code code, const std::string &text) {
  if (code == Status::Code::OK)
    clear_status();
  else {
    set_status(code, text);
    m_last_read_error = chrono::steady_clock::now();
  }
}

void StatusManager::set_write_status(Status::Code code, const std::string &text) {
  if (code == Status::Code::OK)
    clear_status();
  else {
    set_status(code, text);
    m_last_write_error = chrono::steady_clock::now();
  }
}

void StatusManager::set_read_error(int error) {
  if (error == 0)
    clear_status();
  else {
    set_error(error);
    m_last_read_error = chrono::steady_clock::now();
  }
}

void StatusManager::set_write_error(int error) {
  if (error == 0)
    clear_status();
  else {
    set_error(error);
    m_last_write_error = chrono::steady_clock::now();
  }
}

void StatusManager::clear_status() {
  if (m_current_status == Status::Code::OK)
    return;
  auto now = chrono::steady_clock::now();
  lock_guard<mutex> lock(m_mutex);
  if (now - m_last_read_error > CLEAR_CHANGE_INTERVAL &&
      now - m_last_write_error > CLEAR_CHANGE_INTERVAL) {
    m_status.set(Status::Code::OK, "");
    m_current_status = Status::Code::OK;
  }
}

void StatusManager::set_status(Status::Code code, const std::string &text) {
  lock_guard<mutex> lock(m_mutex);
  m_current_status = code;
  m_status.set(m_current_status, text);
}

void StatusManager::set_error(int error) {
  char errtext[128];
  errtext[0] = 0;
  strerror_r(errno, errtext, 128);
  lock_guard<mutex> lock(m_mutex);
  m_current_status = Status::Code::CRITICAL;
  m_status.set(m_current_status, errtext);
}
