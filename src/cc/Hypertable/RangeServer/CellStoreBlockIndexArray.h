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

#ifndef HYPERTABLE_CELLSTOREBLOCKINDEXARRAY_H
#define HYPERTABLE_CELLSTOREBLOCKINDEXARRAY_H

#include <cassert>
#include <iostream>
#include <map>

#include "Common/StaticBuffer.h"

#include "Hypertable/Lib/SerializedKey.h"


namespace Hypertable {

  template <typename OffsetT>
  class CellStoreBlockIndexElementArray {
  public:
    CellStoreBlockIndexElementArray() { }
    CellStoreBlockIndexElementArray(const SerializedKey &key_) : key(key_), offset(0) { }

    SerializedKey key;
    OffsetT offset;
  };

  template <typename OffsetT>
  struct LtCellStoreBlockIndexElementArray {
    bool operator()(const CellStoreBlockIndexElementArray<OffsetT> &x,
        const CellStoreBlockIndexElementArray<OffsetT> &y) const {
      return x.key < y.key;
    }
  };

  /**
   * Provides an STL-style iterator on CellStoreBlockIndex objects.
   */
  template <typename OffsetT>
  class CellStoreBlockIndexIteratorArray {
  public:
    typedef typename std::vector<CellStoreBlockIndexElementArray<OffsetT> >::iterator ArrayIteratorT;

    CellStoreBlockIndexIteratorArray() { }
    CellStoreBlockIndexIteratorArray(ArrayIteratorT iter) : m_iter(iter) { }
    SerializedKey key() { return (*m_iter).key; }
    int64_t value() { return (int64_t)(*m_iter).offset; }
    CellStoreBlockIndexIteratorArray &operator++() { ++m_iter; return *this; }
    CellStoreBlockIndexIteratorArray operator++(int) {
      CellStoreBlockIndexIteratorArray<OffsetT> copy(*this);
      ++(*this);
      return copy;
    }
    bool operator==(const CellStoreBlockIndexIteratorArray &other) {
      return m_iter == other.m_iter;
    }
    bool operator!=(const CellStoreBlockIndexIteratorArray &other) {
      return m_iter != other.m_iter;
    }
  protected:
    ArrayIteratorT m_iter;
  };

  /**
   *
   */
  template <typename OffsetT>
  class CellStoreBlockIndexArray {
  public:
    typedef typename Hypertable::CellStoreBlockIndexIteratorArray<OffsetT> iterator;
    typedef typename Hypertable::CellStoreBlockIndexElementArray<OffsetT> ElementT;
    typedef typename Hypertable::LtCellStoreBlockIndexElementArray<OffsetT> LtT;
    typedef typename std::vector<ElementT> ArrayT;
    typedef typename std::vector<ElementT>::iterator ArrayIteratorT;

    CellStoreBlockIndexArray() : m_disk_used(0) { }

    void load(DynamicBuffer &fixed, DynamicBuffer &variable,int64_t end_of_data,
              const String &start_row="", const String &end_row="") {
      size_t total_entries = fixed.fill() / sizeof(OffsetT);
      SerializedKey key;
      OffsetT offset;
      ElementT ee;
      const uint8_t *key_ptr;
      bool in_scope = (start_row == "") ? true : false;
      bool check_for_end_row = end_row != "";

      m_index_entries = (int64_t)total_entries;

      assert(variable.own);

      m_end_of_last_block = end_of_data;

      m_keydata = variable;
      fixed.ptr = fixed.base;
      key_ptr   = m_keydata.base;

      for (int64_t i=0; i<m_index_entries; ++i) {

        // variable portion
        key.ptr = key_ptr;
        key_ptr += key.length();

        // fixed portion (e.g. offset)
        memcpy(&offset, fixed.ptr, sizeof(offset));
        fixed.ptr += sizeof(offset);

        if (!in_scope) {
          if (strcmp(key.row(), start_row.c_str()) < 0)
            continue;
          in_scope = true;
        }
        else if (check_for_end_row &&
                 strcmp(key.row(), end_row.c_str()) > 0) {
          ee.key = key;
          ee.offset = offset;
          m_array.push_back(ee);
          if (i+1 < m_index_entries) {
            key.ptr = key_ptr;
            key_ptr += key.length();
            memcpy(&m_end_of_last_block, fixed.ptr, sizeof(offset));
          }
          break;
        }
        ee.key = key;
        ee.offset = offset;
        m_array.push_back(ee);
      }

      HT_ASSERT(key_ptr <= (m_keydata.base + m_keydata.size));

      if (!m_array.empty()) {

        std::sort(m_array.begin(), m_array.end(), LtT());
        /** compute space covered by this index scope **/
        m_disk_used = m_end_of_last_block - (*m_array.begin()).offset;

        /** determine split key **/
        size_t mid_point = (m_array.size()==2) ? 0 : m_array.size()/2;
        m_middle_key = m_array[mid_point].key;
      }

    }

    void display() {
      SerializedKey last_key;
      int64_t last_offset = 0;
      int64_t block_size;
      size_t i=0;
      for (ArrayIteratorT iter = m_array.begin(); iter != m_array.end(); ++iter) {
        if (last_key) {
          block_size = (*iter).offset - last_offset;
          std::cout << i << ": offset=" << last_offset << " size=" << block_size
                    << " row=" << last_key.row() << "\n";
          i++;
        }
        last_offset = (*iter).offset;
        last_key = (*iter).key;
      }
      if (last_key) {
        block_size = m_end_of_last_block - last_offset;
        std::cout << i << ": offset=" << last_offset << " size=" << block_size
                  << " row=" << last_key.row() << std::endl;
      }
      std::cout << "sizeof(OffsetT) = " << sizeof(OffsetT) << std::endl;
    }

    const SerializedKey middle_key() { return m_middle_key; }

    size_t memory_used() {
      return m_keydata.size + (m_array.size() * (sizeof(ElementT)));
    }

    int64_t disk_used() { return m_disk_used; }

    int64_t end_of_last_block() { return m_end_of_last_block; }

    int64_t index_entries() { return m_index_entries; }

    iterator begin() {
      return iterator(m_array.begin());
    }

    iterator end() {
      return iterator(m_array.end());
    }

    iterator lower_bound(const SerializedKey& k) {
      ElementT ee(k);
      return iterator(std::lower_bound(m_array.begin(), m_array.end(), ee, LtT()));
    }

    iterator upper_bound(const SerializedKey& k) {
      ElementT ee(k);
      return iterator(std::upper_bound(m_array.begin(), m_array.end(), ee, LtT()));
    }

    void clear() {
      m_array.clear();
      m_keydata.free();
      m_middle_key.ptr = 0;
      m_index_entries = 0;
    }

  private:
    ArrayT m_array;
    StaticBuffer m_keydata;
    SerializedKey m_middle_key;
    int64_t m_end_of_last_block;
    int64_t m_disk_used;
    int64_t m_index_entries;
  };


} // namespace Hypertable

#endif // HYPERTABLE_CELLSTOREBLOCKINDEXARRAY_H
