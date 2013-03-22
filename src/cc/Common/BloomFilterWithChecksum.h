/*
 * Copyright (C) 2007-2012 Hypertable, Inc.
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

/** @file
 * A Bloom Filter with Checksums.
 * A bloom filter is a probabilistic datastructure (see
 * http://en.wikipedia.org/wiki/Bloom_filter). It's used in CellStores to speed
 * up database queries. This bloom filter stores additional checksums.
 */

#ifndef HYPERTABLE_BLOOM_FILTER_WITH_CHECKSUM_H
#define HYPERTABLE_BLOOM_FILTER_WITH_CHECKSUM_H

#include <cmath>
#include <limits.h>
#include "Common/Checksum.h"
#include "Common/Filesystem.h"
#include "Common/Logger.h"
#include "Common/MurmurHash.h"
#include "Common/Serialization.h"
#include "Common/StaticBuffer.h"
#include "Common/StringExt.h"
#include "Common/System.h"

namespace Hypertable {

/** @addtogroup Common
 *  @{
 */

/**
 * A space-efficent probabilistic set for membership test, false postives
 * are possible, but false negatives are not.
 */
template <class HasherT = MurmurHash2>
class BasicBloomFilterWithChecksum {
public:
  /**
   * Constructor
   *
   * @param items_estimate An estimated number of items that will be inserted
   * @param false_positive_prob The probability for false positives
   */
  BasicBloomFilterWithChecksum(size_t items_estimate,
          float false_positive_prob) {
    m_items_actual = 0;
    m_items_estimate = items_estimate;
    m_false_positive_prob = false_positive_prob;
    double num_hashes = -std::log(m_false_positive_prob) / std::log(2);
    m_num_hash_functions = (size_t)num_hashes;
    m_num_bits = (size_t)(m_items_estimate * num_hashes / std::log(2));
    if (m_num_bits == 0) {
      HT_THROWF(Error::EMPTY_BLOOMFILTER,
              "Num elements=%lu false_positive_prob=%.3f",
              (Lu)items_estimate, false_positive_prob);
    }
    m_num_bytes = (m_num_bits / CHAR_BIT) + (m_num_bits % CHAR_BIT ? 1 : 0);
    m_bloom_base = new uint8_t[total_size()];
    m_bloom_bits = m_bloom_base + 4;
    memset(m_bloom_base, 0, total_size());

    HT_DEBUG_OUT << "num funcs=" << m_num_hash_functions << " num bits="
        << m_num_bits << " num bytes= " << m_num_bytes << " bits per element="
        << double(m_num_bits) / items_estimate << HT_END;
  }

  /**
   * Alternative constructor
   *
   * @param items_estimate An estimated number of items that will be inserted
   * @param bits_per_item Average bits per item
   * @param num_hashes Number of hash functions for the filter
   */
  BasicBloomFilterWithChecksum(size_t items_estimate, float bits_per_item,
          size_t num_hashes) {
    m_items_actual = 0;
    m_items_estimate = items_estimate;
    m_false_positive_prob = 0.0;
    m_num_hash_functions = num_hashes;
    m_num_bits = (size_t)((double)items_estimate * (double)bits_per_item);
    if (m_num_bits == 0) {
      HT_THROWF(Error::EMPTY_BLOOMFILTER, "Num elements=%lu bits_per_item=%.3f",
              (Lu)items_estimate, bits_per_item);
    }
    m_num_bytes = (m_num_bits / CHAR_BIT) + (m_num_bits % CHAR_BIT ? 1 : 0);
    m_bloom_base = new uint8_t[total_size()];
    m_bloom_bits = m_bloom_base + 4;
    memset(m_bloom_base, 0, total_size());

    HT_DEBUG_OUT << "num funcs=" << m_num_hash_functions << " num bits="
        << m_num_bits << " num bytes=" << m_num_bytes << " bits per element="
        << double(m_num_bits) / items_estimate << HT_END;
  }

  /**
   * Alternative constructor
   *
   * @param items_estimate An estimated number of items that will be inserted
   * @param items_actual Actual number of items
   * @param length Number of bits
   * @param num_hashes Number of hash functions for the filter
   */
  BasicBloomFilterWithChecksum(size_t items_estimate, size_t items_actual,
          int64_t length, size_t num_hashes) {
    m_items_actual = items_actual;
    m_items_estimate = items_estimate;
    m_false_positive_prob = 0.0;
    m_num_hash_functions = num_hashes;
    m_num_bits = (size_t)length;
    if (m_num_bits == 0) {
      HT_THROWF(Error::EMPTY_BLOOMFILTER,
              "Estimated items=%lu actual items=%lu length=%lld num hashes=%lu",
              (Lu)items_estimate, (Lu)items_actual, (Lld)length, (Lu)num_hashes);
    }
    m_num_bytes = (m_num_bits / CHAR_BIT) + (m_num_bits % CHAR_BIT ? 1 : 0);
    m_bloom_base = new uint8_t[total_size()];
    m_bloom_bits = m_bloom_base + 4;
    memset(m_bloom_base, 0, total_size());

    HT_DEBUG_OUT << "num funcs=" << m_num_hash_functions << " num bits="
        << m_num_bits << " num bytes=" << m_num_bytes << " bits per element="
        << double(m_num_bits) / items_estimate << HT_END;
  }

  /** Destructor; releases resources */
  ~BasicBloomFilterWithChecksum() {
    delete[] m_bloom_base;
  }

  /* XXX/review static functions to expose the bloom filter parameters, given
   1) probablility and # keys
   2) #keys and fix the total size (m), to calculate the optimal
      probability - # hash functions to use
  */

  /** Inserts a new blob into the hash.
   *
   * Runs through each hash function and sets the appropriate bit for each
   * hash.
   *
   * @param key Pointer to the key's data
   * @param len Size of the data (in bytes)
   */
  void insert(const void *key, size_t len) {
    uint32_t hash = len;

    for (size_t i = 0; i < m_num_hash_functions; ++i) {
      hash = m_hasher(key, len, hash) % m_num_bits;
      m_bloom_bits[hash / CHAR_BIT] |= (1 << (hash % CHAR_BIT));
    }
    m_items_actual++;
  }

  /** Overloaded insert function for Strings.
   *
   * @param key Reference to the string.
   */
  void insert(const String &key) {
    insert(key.c_str(), key.length());
  }

  /** Checks if the data set "may" contain the key. This can return false
   * positives.
   *
   * This function runs through all hash tables and checks if the hashed bit 
   * is set. If any of them is not set then the key is definitely not part of
   * the data set. If all bits are set then the key "may" be in the data set.
   *
   * @param key Pointer to the key's data
   * @param len Size of the data (in bytes)
   * @return true if the key "may" be contained, otherwise false
   */
  bool may_contain(const void *key, size_t len) const {
    uint32_t hash = len;
    uint8_t byte_mask;
    uint8_t byte;

    for (size_t i = 0; i < m_num_hash_functions; ++i) {
      hash = m_hasher(key, len, hash) % m_num_bits;
      byte = m_bloom_bits[hash / CHAR_BIT];
      byte_mask = (1 << (hash % CHAR_BIT));

      if ((byte & byte_mask) == 0) {
        return false;
      }
    }
    return true;
  }

  /** Overloaded may_contain function for Strings
   *
   * @param key The String to look for
   * @return true if the key "may" be contained, otherwise false
   */
  bool may_contain(const String &key) const {
    return may_contain(key.c_str(), key.length());
  }

  /** Serializes the BloomFilter into a static memory buffer
   *
   * @param buf The static memory buffer
   */
  void serialize(StaticBuffer &buf) {
    buf.set(m_bloom_base, total_size(), false);
    uint8_t *ptr = buf.base;
    Serialization::encode_i32(&ptr, fletcher32(m_bloom_bits, m_num_bytes));
  }

  /** Getter for the serialized bloom filter data, including metadata and
   * checksums
   *
   * @return pointer to the serialized bloom filter data
   */
  uint8_t *base() { return m_bloom_base; }
  
  /** Validates the checksum of the BloomFilter
   *
   * @param filename The filename of this BloomFilter; required to calculate
   *        the checksum
   * @throws Error::BLOOMFILTER_CHECKSUM_MISMATCH If the checksum does not
   *        match
   */
  void validate(String &filename) {
    const uint8_t *ptr = m_bloom_base;
    size_t remain = 4;
    uint32_t stored_checksum = Serialization::decode_i32(&ptr, &remain);
    uint32_t computed_checksum = fletcher32(m_bloom_bits, m_num_bytes);
    if (stored_checksum != computed_checksum)
      HT_THROW(Error::BLOOMFILTER_CHECKSUM_MISMATCH, filename.c_str());
  }

  /** Getter for the bloom filter size
   *
   * @return The size of the bloom filter data (in bytes)
   */
  size_t size() { return m_num_bytes; }

  /** Getter for the total size (including checksum and metadata)
   *
   * @return The total size of the bloom filter data (including
   *        checksum and metadata, in bytes)
   */
  size_t total_size() {
    return 4 + m_num_bytes + HT_IO_ALIGNMENT_PADDING(4 + m_num_bytes);
  }

  /** Getter for the number of hash functions
   *
   * @return The number of hash functions
   */
  size_t get_num_hashes() { return m_num_hash_functions; }

  /** Getter for the number of bits
   *
   * @return The number of bits
   */
  size_t get_length_bits() { return m_num_bits; }

  /** Getter for the estimated number of items
   *
   * @return The estimated number of items
   */
  size_t get_items_estimate() { return m_items_estimate; }

  /** Getter for the actual number of items
   *
   * @return The actual number of items
   */
  size_t get_items_actual() { return m_items_actual; }

private:
  /** The hash function implementation */
  HasherT    m_hasher;

  /** Estimated number of items */
  size_t     m_items_estimate;

  /** Actual number of items */
  size_t     m_items_actual;

  /** Probability of returning a false positive */
  float      m_false_positive_prob;

  /** Number of hash functions */
  size_t     m_num_hash_functions;

  /** Number of bits */
  size_t     m_num_bits;

  /** Number of bytes (approx. m_num_bits / 8) */
  size_t     m_num_bytes;

  /** The actual bloom filter bit-array */
  uint8_t   *m_bloom_bits;

  /** The serialized bloom filter data, including metadata and checksums */
  uint8_t   *m_bloom_base;
};

typedef BasicBloomFilterWithChecksum<> BloomFilterWithChecksum;

} // namespace Hypertable

#endif // HYPERTABLE_BLOOM_FILTER_WITH_CHECKSUM_H
