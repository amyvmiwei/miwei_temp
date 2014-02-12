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
 * HashMap optimized for char * strings.
 * This file implements a HashMap for storing and looking up strings
 * efficiently.
 */

#ifndef HYPERTABLE_CSTR_HASHMAP_H
#define HYPERTABLE_CSTR_HASHMAP_H

#include <Common/CstrHashTraits.h>

#include <unordered_map>

namespace Hypertable {

/** @addtogroup Common
 *  @{
 */

/**
 * A hash map for storing and lookup char * strings efficiently.
 * The keys are case-independent.
 */
template <typename DataT, class TraitsT = CstrHashTraits<> >
class CstrHashMap : public std::unordered_map<const char *, DataT,
                                              typename TraitsT::hasher,
                                              typename TraitsT::key_equal> {
private:
  typedef std::unordered_map<const char *, DataT, typename TraitsT::hasher,
                             typename TraitsT::key_equal> Base;
public:
  typedef typename Base::iterator iterator;
  typedef typename Base::key_type key_type;
  typedef typename Base::value_type value_type;
  typedef typename TraitsT::key_allocator key_allocator;
  typedef std::pair<iterator, bool> InsRet;

public:
  /** Default constructor creates an empty map */
  CstrHashMap() {}

  /** Overloaded Constructor creates an empty map with a specified number of
   * buckets
   *
   * @param n_buckets Number of buckets in the hash map
   */
  CstrHashMap(size_t n_buckets) : Base(n_buckets) {}

  /** Inserts a new string/data pair in the map. Hides all insert methods
   * in the base class.
   *
   * @param key The key of the key/data pair
   * @param data Reference to the data type of the key/data pair
   * @return An InsRet pair with an iterator to the new blob and a flag whether
   *        the blob already existed in the set
   */
  InsRet insert(const char *key, const DataT &data) {
    return insert_key(key, Base::insert(value_type(key, data)));
  }

  /** Returns the key allocator; required for testing
   *
   * @return The key_allocator object
   */
  key_allocator &key_alloc() { return m_alloc; }

  /** Clears the map and deletes all elements */
  void clear() { Base::clear(); }

private:
  /** The key_allocator - i.e. a CharArena instance */
  key_allocator m_alloc;

  /** Helper function to insert a string; if the string already exists then a
   * duplicate of the string is created and inserted.
   *
   * @param key The inserted key of the object
   * @param rv The InsRet pair, which is the result of the actual insert
   *        operation.
   * @return A copy of the @a rv parameter is returned.
   */
  InsRet insert_key(const char *key, InsRet rv) {
    if (rv.second) {
      char *keycopy = m_alloc.dup(key);
      const_cast<key_type &>(rv.first->first) = keycopy;
    }
    return rv;
  }
};

/** @}*/

} // namespace Hypertable

#endif //! HYPERTABLE_CSTR_HASHMAP_H
