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

#ifndef HYPERTABLE_INDEXMUTATORCALLBACK_H
#define HYPERTABLE_INDEXMUTATORCALLBACK_H

#include <vector>
#include <deque>
#include <map>
#include <boost/thread/condition.hpp>

#include "Common/Mutex.h"
#include "ResultCallback.h"
#include "Key.h"
#include "KeySpec.h"
#include "TableMutatorAsync.h"

namespace Hypertable {

  struct ltStringPtr {
    bool operator()(const char *lhs, const char *rhs) const {
      return strcmp(lhs, rhs) < 0;
    }
  };

  /** ResultCallback for secondary indices; used by TableMutatorAsync
   */
  class IndexMutatorCallback: public ResultCallback {

  public:

    typedef std::multimap<const char *, Cell, ltStringPtr> KeyMap;
    typedef std::pair<String, int> FailedRow;

    IndexMutatorCallback(TableMutatorAsync *primary_mutator,
            ResultCallback *original_cb, uint64_t max_memory)
      : ResultCallback(), m_primary_mutator(primary_mutator),
      m_original_cb(original_cb), m_max_memory(max_memory) {
    }

    virtual ~IndexMutatorCallback() {
    }

    /** buffers a cell in the cellbuffer and the keymap */
    void buffer_key(Key &key, const void *value, uint32_t value_len) {
      ScopedLock lock(m_mutex);

      // to avoid Schema lookups: use the numeric ID as the column family
      char tmp[32];
      sprintf(tmp, "%d", key.column_family_code);

      // create a cell and add it to the cell-buffer
      Cell cell(key.row, &tmp[0], key.column_qualifier,
              key.timestamp, key.revision, (uint8_t *)value, 
              value_len, key.flag);
      m_cellbuffer.add(cell, true);
      m_used_memory += strlen(key.row) + strlen(&tmp[0])
          + 8 + 8 + 2 + value_len;
      if (key.column_qualifier)
        m_used_memory += strlen(key.column_qualifier);

      // retrieve the inserted cell, then store it in the keymap as well
      m_cellbuffer.get_cell(cell, m_cellbuffer.size() - 1);
      m_keymap.insert(KeyMap::value_type(cell.row_key, cell));
    }

    /**
     * Callback method for successful scan
     *
     * @param scanner Pointer to scanner
     * @param scancells returned cells
     */
    virtual void scan_ok(TableScannerAsync *scanner, ScanCellsPtr &scancells) {
      if (m_original_cb)
        m_original_cb->scan_ok(scanner, scancells);
    }

    /**
     * Callback method for scan errors
     *
     * @param scanner Pointer to scanner
     * @param error Error code
     * @param error_msg Error message
     * @param eos end of scan indicator
     */
    virtual void scan_error(TableScannerAsync *scanner, int error, 
                            const String &error_msg, bool eos) {
      if (m_original_cb)
        m_original_cb->scan_error(scanner, error, error_msg, eos);
    }

    virtual void update_ok(TableMutatorAsync *mutator) {
      // only propagate if event is from the primary table
      if (mutator == m_primary_mutator && m_original_cb)
        m_original_cb->update_ok(mutator);
    }

    virtual void update_error(TableMutatorAsync *mutator, int error, 
                              FailedMutations &failures) {
      // check mutator; if the failures are from the primary table then 
      // propagate them directly to the caller; if failures come from 
      // updating the index table(s) then continue below
      if (mutator == m_primary_mutator && m_original_cb) {
        m_original_cb->update_error(mutator, error, failures);
        return;
      }

      ScopedLock lock(m_mutex);

      m_error = error;

      // remove failed updates from the keymap
      foreach_ht (const FailedMutation &fm, failures) {
        const Cell &cell = fm.first;
        
        // get the original row key
        const char *row = cell.row_key + strlen(cell.row_key);
        while (*row != '\t' && row > cell.row_key)
          --row;
        if (*row != '\t') {
          m_failed_rows.push_back(FailedRow(cell.row_key, error));
          continue;
        }

        // and store it for later
        m_failed_rows.push_back(FailedRow(row + 1, error));
      }
    }

    void propagate_failures() {
      FailedMutations propagate;

      ScopedLock lock(m_mutex);

      foreach_ht (const FailedRow &fail, m_failed_rows) {
        // add all cells with this row key to the failed mutations and
        // purge them from the keymap
        std::pair<KeyMap::iterator, KeyMap::iterator> it =
                m_keymap.equal_range(fail.first.c_str());
        for (KeyMap::iterator i = it.first; i != it.second; ++i) {
          Cell &cell = i->second;
          propagate.push_back(std::pair<Cell, int>(cell, fail.second));
        }
        m_keymap.erase(it.first, it.second);
      }

      if (m_original_cb && !propagate.empty())
        m_original_cb->update_error(m_primary_mutator, m_error, propagate);
    }

    bool needs_flush() {
      ScopedLock lock(m_mutex);
      return m_used_memory > m_max_memory;
    }

    void consume_keybuffer(TableMutatorAsync *mutator) {
      ScopedLock lock(m_mutex);

      IndexMutatorCallback::KeyMap::iterator it;
      for (it = m_keymap.begin(); it != m_keymap.end(); ++it) {
        mutator->update_without_index(it->second);
      }

      // then clear the internal buffers in the callback
      clear();
    }
    
    void clear() {
      m_keymap.clear();
      m_cellbuffer.clear();
      m_used_memory = 0;
    }

  private:
    // the original mutator for the primary table
    TableMutatorAsync *m_primary_mutator;

    // the original callback object specified by the user
    ResultCallback *m_original_cb;

    // a flat memory buffer for storing the original keys
    CellsBuilder m_cellbuffer;

    // a map for storing the original keys
    KeyMap m_keymap;

    // a list of failed keys and their error code
    std::vector<FailedRow> m_failed_rows;

    // a mutex to protect the members
    Mutex m_mutex;

    // maximum size of buffered keys
    uint64_t m_max_memory;

    // currently used memory
    uint64_t m_used_memory;

    // last error code returned from the RangeServer
    int m_error;
  };

  typedef intrusive_ptr<IndexMutatorCallback> IndexMutatorCallbackPtr;

} // namespace Hypertable

#endif // HYPERTABLE_INDEXMUTATORCALLBACK_H
