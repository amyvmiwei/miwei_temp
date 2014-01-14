/*
 * Copyright (C) 2007-2013 Hypertable, Inc.
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
/// Declarations for CellStore.
/// This file contains the type declarations for CellStore, an abstract base
/// class from which is derived concrete classes for creating and loading
/// cell store files.

#ifndef HYPERTABLE_CELLSTORE_H
#define HYPERTABLE_CELLSTORE_H

#include <Hypertable/RangeServer/CellList.h>
#include <Hypertable/RangeServer/CellListScannerBuffer.h>
#include <Hypertable/RangeServer/CellStoreBlockIndexArray.h>
#include <Hypertable/RangeServer/CellStoreTrailer.h>
#include <Hypertable/RangeServer/KeyDecompressor.h>

#include <Hypertable/Lib/BlockCompressionCodec.h>
#include <Hypertable/Lib/Types.h>

#include <Common/Mutex.h>
#include <Common/String.h>
#include <Common/ByteString.h>
#include <Common/Filesystem.h>

#include <vector>

namespace Hypertable {

  /// @addtogroup RangeServer
  /// @{

  /**
   * Abstract base class for persistent cell lists (ones that are stored on
   * disk).
   */
  class CellStore : public CellList {
  public:

    class IndexMemoryStats {
    public:
      IndexMemoryStats() :
	bloom_filter_memory(0), bloom_filter_access_counter(0),
	block_index_memory(0), block_index_access_counter(0) { }
      int64_t  bloom_filter_memory;
      uint64_t bloom_filter_access_counter;
      int64_t  block_index_memory;
      uint64_t block_index_access_counter;
    };

    CellStore() : m_bytes_read(0), m_block_count(0), m_index_refcount(0) { }

    virtual ~CellStore() { return; }

    virtual void add(const Key &key, const ByteString value) = 0;

    /** Populates <code>split_row_data</code> with unique row and count
     * estimates from block index.
     * @param split_row_data Reference to accumulator map holding unique
     * rows and counts taken from block index.
     * @note <code>split_row_data</code> should not be cleared
     */
    virtual void split_row_estimate_data(SplitRowDataMapT &split_row_data) {
      HT_FATAL("Not implemented!");
    }

    /** Populates <code>scanner</code> with key/value pairs generated from
     * CellStore index.
     * @param scanner Pointer to CellListScannerBuffer to receive key/value
     * pairs
     */
    virtual void populate_index_pseudo_table_scanner(CellListScannerBuffer *scanner) { }

    /// Returns the number of key/value pairs in the cell store.
    /// @return Number of key/value pairs in list
    virtual int64_t get_total_entries() = 0;

    virtual CellListScanner *
    create_scanner(ScanContextPtr &scan_ctx) { return 0; }

    /**
     * Creates a new cell store.
     *
     * @param fname name of file to contain the cell store
     * @param max_entries maximum number of entries the cell store is
     *        expected to have
     * @param props cellstore specific properties
     * @param table_id Table identifier
     */
    virtual void create(const char *fname, size_t max_entries,
                        PropertiesPtr &props, const TableIdentifier *table_id=0) = 0;

    /**
     * Finalizes the creation of a cell store, by writing block index and
     * metadata trailer.
     *
     * @param table_identifier table identifier
     */
    virtual void finalize(TableIdentifier *table_identifier) = 0;

    /**
     * Opens a cell store with possibly a restricted view.  When a range
     * splits, the cell stores that comprise the range get shared between the
     * two newly created ranges.  This method allows each of the two ranges to
     * open the same cell store but view different portions of it.
     *
     * @param fname pathname of file containing cell store
     * @param start_row restricts view of this store to key/value pairs that
     *        are greater than this value
     * @param end_row restricts view of this store to key/value pairs that are
     *        less than or equal to this value
     * @param fd cell store file descriptor
     * @param file_length length of cell store file
     * @param trailer pointer to trailer object
     */
    virtual void open(const String &fname, const String &start_row,
                      const String &end_row, int32_t fd, int64_t file_length,
                      CellStoreTrailer *trailer) = 0;

    virtual void rescope(const String &start_row, const String &end_row) {
      HT_FATAL("Not implemented!");
    }

    /**
     * Returns the block size used for this cell store.  The block size is the
     * amount of uncompressed key/value pairs to collect before compressing and
     * storing as a compressed block in the cell store.
     *
     * @return block size
     */
    virtual int64_t get_blocksize() = 0;

    /**
     * Bloom filter lookup.
     * @param scan_ctx Scan context
     * @return true if cell store may contain row referenced by
     * <code>scan_ctx</code>
     */
    virtual bool may_contain(ScanContextPtr &scan_ctx) = 0;

    /**
     * Returns the disk used by this cell store.  If the cell store is opened
     * with a restricted range, then it returns an estimate of the disk used by
     * that range.
     *
     * @return disk used by this cell store or portion thereof
     */
    virtual uint64_t disk_usage() = 0;

    /**
     * Returns the number of CellStore blocks covered by this object.
     * @return block count
     */
    virtual size_t block_count() {
      ScopedLock lock(m_mutex);
      return m_block_count;
    }

    /**
     * Returns block compression ratio of this cell store.
     *
     * @return compression ratio
     */
    virtual float compression_ratio() = 0;

    /**
     * Pathname of cell store file
     *
     * @return string reference to the cell store path name
     */
    virtual std::string &get_filename() = 0;

    /**
     * Returns a unique identifier which identifies the underlying file
     * for caching purposes
     *
     * @return unique file id
     */
    virtual int get_file_id() = 0;

    /**
     * Return a pointer to the trailer object for this cell store
     *
     * @return pointer to the internal trailer of this cell store
     */
    virtual CellStoreTrailer *get_trailer() = 0;

    /**
     * Creates a block compression codec suitable for decompressing
     * the cell store's blocks
     *
     * @return pointer to compression codec
     */
    virtual BlockCompressionCodec *create_block_compression_codec() = 0;

    /**
     * Creates a key decompressor suitable for decompressing the
     * keys stored in this cell store
     *
     * @return pointer to key decompressor
     */
    virtual KeyDecompressor *create_key_decompressor();

    /**
     * Sets the cell store files replaced by this CellStore
     */
    virtual void set_replaced_files(const std::vector<String> &old_files);

    /**
     * Returns all the cell store files replaced by this CellStore
     *
     * @return vector of strings with names of the files that this cell store replaces
     */
    virtual const std::vector<String> &get_replaced_files();

    /**
     * Displays block information to stdout
     */
    virtual void display_block_info() = 0;

    /**
     * Return Bloom filter size
     *
     * @return size of bloom filter
     */
    virtual size_t bloom_filter_size() = 0;

    /**
     * Returns the open file descriptor for the CellStore file
     *
     * @return open file descriptor
     */
    virtual int32_t get_fd() = 0;

    /**
     * Closes and reopens the underlying CellStore file
     *
     * @return new file descriptor
     */
    virtual int32_t reopen_fd() = 0;

    /**
     * Returns the amount of memory consumed by the bloom filter
     *
     * @return memory used by bloom filter
     */
    virtual int64_t bloom_filter_memory_used() = 0;

    /**
     * Returns the amount of memory consumed by the block index
     *
     * @return memory used by block index
     */
    virtual int64_t block_index_memory_used() = 0;

    /**
     * Returns the offset of the end of the last block in the cell store
     *
     * @return offset of end of last block
     */
    virtual int64_t end_of_last_block() = 0;

    /**
     * Purges bloom filter and block indexes
     *
     * @return amount of memory purged
     */
    virtual uint64_t purge_indexes() = 0;

    /**
     * Returns amount of purgeable index memory available
     */
    virtual void get_index_memory_stats(IndexMemoryStats *statsp) {
      ScopedLock lock(m_mutex);
      memcpy(statsp, &m_index_stats, sizeof(IndexMemoryStats));
    }

    /**
     * Returns true if the cellstore was opened with a restricted range
     *
     * @return true if cellstore opened with restricted range
     */
    virtual bool restricted_range() = 0;

    /**
     * Returns the number of "uncompressed" bytes read from the underlying
     * filesystem.
     *
     * @return number of uncompressed bytes read from filesystem
     */
    uint64_t bytes_read() {
      ScopedLock lock(m_mutex);
      return m_bytes_read;
    }

    /** Decrement index reference count.
     */
    void decrement_index_refcount() {
      ScopedLock lock(m_mutex);
      m_index_refcount--;
    }

    virtual uint16_t block_header_format() = 0;

    static const char DATA_BLOCK_MAGIC[10];
    static const char INDEX_FIXED_BLOCK_MAGIC[10];
    static const char INDEX_VARIABLE_BLOCK_MAGIC[10];

  protected:

    Mutex m_mutex;
    IndexMemoryStats m_index_stats;
    std::vector <String> m_replaced_files;
    uint64_t m_bytes_read;
    size_t m_block_count;
    uint32_t m_index_refcount;
  };

  typedef intrusive_ptr<CellStore> CellStorePtr;

  /// @}

} // namespace Hypertable

#endif // HYPERTABLE_CELLSTORE_H
