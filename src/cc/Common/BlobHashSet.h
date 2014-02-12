/* -*- c++ -*-
 * Copyright (C) 2007-2014 Hypertable, Inc.
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
 * along with Hypertable. If not, see <http://www.gnu.org/licenses/>
 */

/** @file
 * A HashSet optimized for blobs.
 * This file implements a HashSet for storing and looking up blobs efficiently.
 * It is used i.e. for Bloom Filters.
 */

#ifndef HYPERTABLE_CHARSTR_HASHMAP_H
#define HYPERTABLE_CHARSTR_HASHMAP_H

#include <Common/BlobHashTraits.h>

#include <unordered_set>

namespace Hypertable {

/** @addtogroup Common
 *  @{
 */

/**
 * A HashSet for storing and looking up blobs efficiently. A Blob is a
 * simple structure with { void *, size_t } (see BlobHashTraits.h).
 */
template <class TraitsT = BlobHashTraits<> >
class BlobHashSet : public std::unordered_set<Blob, typename TraitsT::hasher,
                                              typename TraitsT::key_equal> {
private:
  typedef std::unordered_set<Blob, typename TraitsT::hasher,
                             typename TraitsT::key_equal> Base;

public:
  typedef typename Base::iterator iterator;
  typedef typename Base::key_type key_type;
  typedef typename Base::value_type value_type;
  typedef typename TraitsT::key_allocator key_allocator;
  typedef std::pair<iterator, bool> InsRet;

  /** Constructor creates an empty set */
  BlobHashSet() { }

  /** Overloaded Constructor creates an empty set with a specified number of
   * buckets
   *
   * @param n_buckets Number of buckets in the hash table
   */
  BlobHashSet(size_t n_buckets) : Base(n_buckets) { }

  /** Inserts a new Blob into the set. Hides all insert methods in the
   * base class.
   *
   * @param buf Pointer to the blob's data
   * @param len Size of the blob
   * @return An InsRet pair with an iterator to the new blob and a flag whether
   *        the blob already existed in the set
   */
  InsRet insert(const void *buf, size_t len) {
    return insert_blob(len, Base::insert(value_type(buf, len)));
  }

  /** Insert function for a String object
   *
   * @param s Reference to the String which is inserted
   * @return An InsRet pair with an iterator to the new blob and a flag whether
   *        the blob already existed in the set
   */
  InsRet insert(const String &s) {
    return insert_blob(s.size(), Base::insert(value_type(s.data(), s.size())));
  }

  /** Insert function for a Blob object
   *
   * @param blob Reference to the blob which is inserted
   * @return An InsRet pair with an iterator to the new blob and a flag whether
   *        the blob already existed in the set
   */
  InsRet insert(const Blob &blob) {
    return insert_blob(blob.size, Base::insert(blob));
  }

  /** Looks up the blob and returns an iterator to it. Hides all find methods
   * in the base class.
   *
   * @param blob Reference to the blob to find
   * @return Iterator to the found blob, or end() if the blob was not found
   */
  iterator find(const Blob &blob) { return Base::find(blob); }

  /** Find function for String objects
   *
   * @param s Reference to the string to find
   * @return Iterator to the found blob, or end() if the blob was not found
   */
  iterator find(const String &s) {
    return Base::find(Blob(s.data(), s.size()));
  }

  /** Returns the allocator; used for testing.
   *
   * @return The key_allocator object
   */
  key_allocator &key_alloc() { return m_alloc; }

private:
  /** The key_allocator - i.e. a CharArena instance */
  key_allocator m_alloc;

  /** Helper function to insert a blob; if the blob already exists then a
   * duplicate blob is created and inserted.
   *
   * @param len The length of the blob which was inserted
   * @param rv The InsRet pair, which is the result of the actual insert
   *        operation.
   * @return A copy of the @a rv parameter is returned.
   */
  InsRet insert_blob(size_t len, InsRet rv) {
    if (rv.second)
      const_cast<const void *&>(rv.first->start) =
          m_alloc.dup(rv.first->start, len);

    return rv;
  }
};

/** @}*/

} // namespace Hypertable

#endif //! HYPERTABLE_CHARSTR_HASHMAP_H
