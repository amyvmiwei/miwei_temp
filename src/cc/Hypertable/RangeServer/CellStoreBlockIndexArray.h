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

/** @file
 * Declarations for CellStoreBlockIndexArray.
 * This file contains the type declarations for CellStoreBlockIndexArray, a
 * template class used to load and provide access to a CellStore block
 * index.
 */

#ifndef Hypertable_RangeServer_CellStoreBlockIndexArray_h
#define Hypertable_RangeServer_CellStoreBlockIndexArray_h

#include "CellList.h"
#include "CellListScannerBuffer.h"

#include <Hypertable/Lib/Key.h>
#include <Hypertable/Lib/PseudoTables.h>
#include <Hypertable/Lib/SerializedKey.h>

#include <Common/StaticBuffer.h>

#include <cassert>
#include <iostream>
#include <map>

namespace Hypertable {

  /** @addtogroup RangeServer
   * @{
   */

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

  CellStoreBlockIndexArray() : m_disk_used(0), m_maximum_entries((OffsetT)-1) { }

    void load(DynamicBuffer &fixed, DynamicBuffer &variable,int64_t end_of_data,
              const String &start_row="", const String &end_row="") {
      size_t total_entries = fixed.fill() / sizeof(OffsetT);
      SerializedKey key;
      OffsetT offset;
      ElementT ee;
      const uint8_t *key_ptr;
      bool in_scope = (start_row == "") ? true : false;
      bool check_for_end_row = end_row != "";
      const uint8_t *variable_start = variable.base;
      const uint8_t *variable_end = variable.ptr;

      assert(variable.own);

      m_end_of_last_block = end_of_data;

      fixed.ptr = fixed.base;
      key_ptr   = variable.base;

      for (size_t i=0; i<total_entries; ++i) {

        // variable portion
        key.ptr = key_ptr;
        key_ptr += key.length();

        // fixed portion (e.g. offset)
        memcpy(&offset, fixed.ptr, sizeof(offset));
        fixed.ptr += sizeof(offset);

        if (!in_scope) {
          if (strcmp(key.row(), start_row.c_str()) <= 0)
            continue;
          variable_start = key.ptr;
          in_scope = true;
        }

        if (check_for_end_row && strcmp(key.row(), end_row.c_str()) > 0) {
          ee.key = key;
          ee.offset = offset;
          m_array.push_back(ee);
          if (i+1 < total_entries) {
            key.ptr = key_ptr;
            key_ptr += key.length();
            variable_end = key_ptr;
            memcpy(&m_end_of_last_block, fixed.ptr, sizeof(offset));
          }
          break;
        }
        ee.key = key;
        ee.offset = offset;
        m_array.push_back(ee);
      }

      HT_ASSERT(key_ptr <= variable.ptr);

      if (!m_array.empty()) {
        HT_ASSERT(variable_start < variable_end);

        // Copy portion of variable buffer used to m_keydata and fixup the
        // array pointers to point into this new buffer
        StaticBuffer keydata(variable_end-variable_start);
        memcpy(keydata.base, variable_start, variable_end-variable_start);
        for (auto &element : m_array) {
          HT_ASSERT(element.key.ptr < variable_end);
          uint64_t offset = element.key.ptr - variable_start;
          element.key.ptr = keydata.base + offset;
        }
        m_keydata.free();
        m_keydata = keydata;

        // compute space covered by this index scope
        m_disk_used = m_end_of_last_block - (*m_array.begin()).offset;

        // determine split key
        size_t mid_point = (m_array.size()==2) ? 0 : m_array.size()/2;
        m_middle_key = m_array[mid_point].key;
      }

      // Free variable buf here to maintain original semantics
      variable.free();

      // The first time this method is called, it is called with the entire
      // index, so save the size of the entire index for "fraction covered"
      // calcuations
      if (m_maximum_entries == (OffsetT)-1)
        m_maximum_entries = (OffsetT)total_entries;
    }

    void rescope(const String &start_row="", const String &end_row="") {
      DynamicBuffer fixed(m_array.size() * sizeof(OffsetT));
      DynamicBuffer variable;

      // Transfer ownership of m_keydata buffer to variable
      m_keydata.own = false;
      variable.base = variable.mark = m_keydata.base;
      variable.size = m_keydata.size;
      variable.ptr = variable.base + variable.size;

      // Populate fixed array
      for (auto &element : m_array)
        fixed.add_unchecked(&element.offset, sizeof(OffsetT));
      m_array.clear();

      // Perform normal load
      load(fixed, variable, m_end_of_last_block, start_row, end_row);
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

    /** Accumulates unique row estimates from block index entries.
     * @param split_row_data Reference to accumulator map holding unique
     * row and count estimates
     * @param keys_per_block Key count to add for each index entry
     */
    void unique_row_count_estimate(CellList::SplitRowDataMapT &split_row_data,
                                   int32_t keys_per_block) {
      const char *row, *last_row = 0;
      int64_t last_count = 0;
      for (auto &e : m_array) {
        row = e.key.row();
        if (last_row == 0)
          last_row = row;
        if (strcmp(row, last_row) != 0) {
          CstrToInt64MapT::iterator iter = split_row_data.find(last_row);
          if (iter == split_row_data.end())
            split_row_data[last_row] = last_count;
          else
            iter->second += last_count;
          last_row = row;
          last_count = 0;
        }
        last_count += keys_per_block;
      }
      // Deliberately skipping last entry in m_array because it is
      // larger than end_row
    }

    /** Populates <code>scanner</code> with data for <i>.cellstore.index</i>
     * pseudo table.  This method synthesizes the following
     * <i>.cellstore.index</i> pseudo-table cells, for each in-scope block
     * index entry:
     * 
     *   - <b>Size</b> - Estimated uncompressed size of the block
     *   - <b>CompressedSize</b> - Actual compressed size of the block
     *   - <b>KeyCount</b> - Estimated key count for the block
     *     (<code>keys_per_block</code>)
     *
     * Each key is formed by taking the <i>row</i>, <i>timestamp</i>, and
     * <i>revision</i> from the block index key, using FLAG_INSERT as
     * the <i>flag</i>, and constructing the column <i>qualifier</i> as
     * follows:
     * <pre>
     * <cellstore-filename> ':' <hex-offset>
     * </pre>
     * 
     * As an example, a single block index entry might generate the following
     * cells in TSV format:
     * <pre>
     * help@acme.com	Size:2/2/default/ZwmE_ShYJKgim-IL/cs103:00028A61	171728
     * help@acme.com	CompressedSize:2/2/default/ZwmE_ShYJKgim-IL/cs103:00028A61	65231
     * help@acme.com	KeyCount:2/2/default/ZwmE_ShYJKgim-IL/cs103:00028A61	281
     * </pre>
     * @param scanner Pointer to CellListScannerBuffer to hold data
     * @param filename Name of associated CellStore file
     * @param keys_per_block Estimate of number of keys per block
     * @param compression_ratio Block compression ratio for associated CellStore
     * file
     */
    void populate_pseudo_table_scanner(CellListScannerBuffer *scanner,
                                       const String &filename,
                                       int32_t keys_per_block,
                                       float compression_ratio) {
      Key key;
      DynamicBuffer qualifier(filename.length() + 32);
      DynamicBuffer serial_key_buf;
      DynamicBuffer value_buf(32);
      char buf[32];
      char *offset_ptr;
      const char *offset_format = (sizeof(OffsetT) == 4) ? "%08llX" : "%016llX";
      double size;
      OffsetT next_offset;

      qualifier.add_unchecked(filename.c_str(), filename.length());
      qualifier.add_unchecked(":", 1);
      offset_ptr = (char *)qualifier.ptr;

      for (size_t i=0; i<m_array.size(); i++) {

        if (i < (m_array.size()-1))
          next_offset = m_array[i+1].offset;
        else
          next_offset = m_end_of_last_block;

        key.load(m_array[i].key);
        sprintf(offset_ptr, offset_format, (long long)m_array[i].offset);

        // Size key
        serial_key_buf.clear();
        create_key_and_append(serial_key_buf, FLAG_INSERT, key.row,
                              PseudoTables::CELLSTORE_INDEX_SIZE,
                              (const char *)qualifier.base, key.timestamp,
                              key.revision);
        // Size value
        value_buf.clear();
        size = (double)(next_offset - m_array[i].offset) / (double)compression_ratio;
        sprintf(buf, "%lu", (unsigned long)size);
        Serialization::encode_vi32(&value_buf.ptr, strlen(buf));
        strcpy((char *)value_buf.ptr, buf);

        scanner->add( SerializedKey(serial_key_buf.base), ByteString(value_buf.base) );

        // CompressedSize key
        serial_key_buf.clear();
        create_key_and_append(serial_key_buf, FLAG_INSERT, key.row,
                              PseudoTables::CELLSTORE_INDEX_COMPRESSED_SIZE,
                              (const char *)qualifier.base, key.timestamp,
                              key.revision);
        // CompressedSize value
        value_buf.clear();
        sprintf(buf, "%lu", (unsigned long)(next_offset - m_array[i].offset));
        Serialization::encode_vi32(&value_buf.ptr, strlen(buf));
        strcpy((char *)value_buf.ptr, buf);

        scanner->add( SerializedKey(serial_key_buf.base), ByteString(value_buf.base) );

        // KeyCount key
        serial_key_buf.clear();
        create_key_and_append(serial_key_buf, FLAG_INSERT, key.row,
                              PseudoTables::CELLSTORE_INDEX_KEY_COUNT,
                              (const char *)qualifier.base, key.timestamp,
                              key.revision);
        // KeyCount value
        value_buf.clear();
        sprintf(buf, "%lu", (unsigned long)keys_per_block);
        Serialization::encode_vi32(&value_buf.ptr, strlen(buf));
        strcpy((char *)value_buf.ptr, buf);

        scanner->add( SerializedKey(serial_key_buf.base), ByteString(value_buf.base) );

      }
    }

    size_t memory_used() {
      return m_keydata.size + (m_array.size() * (sizeof(ElementT)));
    }

    int64_t disk_used() { return m_disk_used; }

    double fraction_covered() {
      HT_ASSERT(m_maximum_entries != (OffsetT)-1);
      return (double)m_array.size() / (double)m_maximum_entries;
    }

    int64_t end_of_last_block() { return m_end_of_last_block; }

    int64_t index_entries() { return m_array.size(); }

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
      m_maximum_entries = (OffsetT)-1;
    }

  private:
    ArrayT m_array;
    StaticBuffer m_keydata;
    SerializedKey m_middle_key;
    OffsetT m_end_of_last_block;
    OffsetT m_disk_used;
    OffsetT m_maximum_entries;
  };

  /** @}*/

} // namespace Hypertable

#endif // Hypertable_RangeServer_CellStoreBlockIndexArray_h
