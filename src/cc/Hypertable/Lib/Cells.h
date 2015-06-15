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

#ifndef Hypertable_Lib_Cells_h
#define Hypertable_Lib_Cells_h

#include "Cell.h"

#include <Common/PageArenaAllocator.h>
#include <Common/StringExt.h>

#include <memory>
#include <vector>
#include <set>

namespace Hypertable {

  typedef PageArenaAllocator<Cell> CellAlloc;
  typedef std::vector<Cell, CellAlloc> Cells;
  typedef std::pair<Cell, int> FailedMutation;
  typedef std::vector<FailedMutation> FailedMutations;

  class CellsBuilder {
  public:
    CellsBuilder(size_t size_hint = 128)
      : m_size_hint(size_hint) {
      // initialization in the initializer list it will fail on dtor
      m_cells = Cells(CellAlloc(m_arena));
      m_str_set = CstrSet(CstrSetAlloc(m_arena));
      m_cells.reserve(size_hint);
    }

    ~CellsBuilder() {
      m_str_set.clear();
      m_cells.clear();
      m_arena.free();
    }

    size_t size() const {
      return m_cells.size();
    }

    size_t memory_used() const {
      return m_arena.used();
    }

    void get_cell(Cell &cc, size_t ii) {
      cc = m_cells[ii];
    }

    void add(const Cell &cell, bool own = true) {
      if (!own) {
        m_cells.push_back(cell);
        return;
      }

      Cell copy;

      if (cell.row_key)
        copy.row_key = m_arena.dup(cell.row_key);

      if (cell.column_family)
        copy.column_family = get_or_add(cell.column_family);

      if (cell.column_qualifier)
        copy.column_qualifier = get_or_add(cell.column_qualifier);

      copy.timestamp = cell.timestamp;
      copy.revision = cell.revision;

      if (cell.value) {
        copy.value = (uint8_t *)m_arena.dup(cell.value, cell.value_len);
        copy.value_len = cell.value_len;
      }
      copy.flag = cell.flag;
      m_cells.push_back(copy);
    }

    void get(Cells &cells) { cells = m_cells; }
    Cells &get() { return m_cells; }
    const Cells &get() const { return m_cells; }

    void clear() {
      m_str_set.clear();
      m_cells.clear();
      m_arena.free();
      m_cells.reserve(m_size_hint);
    }

    void copy_failed_mutations(const FailedMutations &src, FailedMutations &dst) {
      clear();
      dst.clear();
      size_t ii=0;
      for (const auto &v : src) {
        add(v.first);
        dst.push_back(std::make_pair(m_cells[ii], v.second));
        ++ii;
      }
    }
  protected:
    typedef PageArenaAllocator<const char*> CstrSetAlloc;
    typedef std::set<const char*, LtCstr, CstrSetAlloc> CstrSet;

    CharArena m_arena;
    Cells m_cells;
    CstrSet m_str_set;
    size_t m_size_hint;

    inline const char* get_or_add(const char* s) {
      CstrSet::iterator it = m_str_set.find(s);
      if (it == m_str_set.end()) {
        it = m_str_set.insert(m_arena.dup(s)).first;
      }
      return *it;
    }
  };

  /// Smart pointer to CellsBuilder
  typedef std::shared_ptr<CellsBuilder> CellsBuilderPtr;

}

#endif // Hypertable_Lib_Cells_h
