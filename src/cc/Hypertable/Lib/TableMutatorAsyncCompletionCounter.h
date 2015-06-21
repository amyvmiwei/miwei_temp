/* -*- c++ -*-
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

#ifndef Hypertable_Lib_TableMutatorAsyncCompletionCounter_h
#define Hypertable_Lib_TableMutatorAsyncCompletionCounter_h

#include <Common/Time.h>

#include <mutex>

namespace Hypertable {

  /**
   * Tracks outstanding RangeServer update requests.  This class is used to
   * track the state of outstanding RangeServer update requests for a scatter
   * send.  It is initialized with the number of updates issued.  When each
   * update returns or times out the counter is decremented and when all
   * updates have completed or timed out, an object of this class will signal
   * completion.
   */
  class TableMutatorAsyncCompletionCounter {
  public:
    TableMutatorAsyncCompletionCounter() {}

    void set(size_t count) {
      std::lock_guard<std::mutex> lock(m_mutex);
      m_outstanding = count;
      m_done = (m_outstanding == 0) ? true : false;
      m_errors = m_retries = false;
    }

    bool decrement() {
      std::lock_guard<std::mutex> lock(m_mutex);
      HT_ASSERT(m_outstanding > 0);
      m_outstanding--;
      if (m_outstanding == 0) {
        m_done = true;
      }
      return m_done;
    }
    void set_retries() { std::lock_guard<std::mutex> lock(m_mutex); m_retries = true; }

    void set_errors() { std::lock_guard<std::mutex> lock(m_mutex); m_errors = true; }

    bool has_retries() { std::lock_guard<std::mutex> lock(m_mutex); return m_retries; }

    bool has_errors() { std::lock_guard<std::mutex> lock(m_mutex); return m_errors; }

    void clear_errors() { std::lock_guard<std::mutex> lock(m_mutex); m_errors = false; }

  private:
    std::mutex m_mutex;
    size_t m_outstanding {};
    bool m_done {};
    bool m_retries {};
    bool m_errors {};
  };

}

#endif // Hypertable_Lib_TableMutatorAsyncCompletionCounter_h
