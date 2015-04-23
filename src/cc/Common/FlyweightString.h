/*
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
/// Flyweight string set.
/// This string set stores duplicate strings efficiently. 

#ifndef HYPERTABLE_FLYWEIGHTSTRING_H
#define HYPERTABLE_FLYWEIGHTSTRING_H

#include <set>
#include <string>

#include "Common/PageArenaAllocator.h"
#include "StringExt.h"

namespace Hypertable {

  /// @addtogroup Common
  /// @{

  /**
   * The Flyweight string set stores duplicate strings efficiently.
   */
  class FlyweightString {
  public:

    FlyweightString() {
      m_strings = CstrSet(LtCstr(), CstrSetAlloc(m_arena));
    }

    /** The destructor deletes all internal pointers and clears the set;
     * pointers retrieved with @sa get() are invalidated.
     */
    ~FlyweightString() { clear(); }

    /** Returns a copy of the string; this string is valid till the
     * FlyweightString set is destructed. Duplicate strings are not inserted
     * twice.
     *
     * @param str The string to insert
     * @return A copy of the inserted string
     */
    const char *get(const char *str) {
      if (str == 0)
        return 0;
      CstrSet::iterator iter = m_strings.find(str);
      if (iter == m_strings.end())
        iter = m_strings.insert(m_arena.dup(str)).first;
      return *iter;
    }

    /** Returns a copy of the string; this string is valid till the
     * FlyweightString set is destructed. Duplicate strings are not inserted
     * twice.
     *
     * @param str The string to insert
     * @return A copy of the inserted string
     */
    const char *get(const std::string& str) {
      CstrSet::iterator iter = m_strings.find(str.c_str());
      if (iter == m_strings.end())
        iter = m_strings.insert(m_arena.dup(str)).first;
      return *iter;
    }

    /** Returns a copy of the string; this string is valid till the
     * FlyweightString set is destructed. Duplicate strings are not inserted
     * twice.
     *
     * @param str The string to insert
     * @param len The length of the string
     * @return A copy of the inserted string
     */
    const char *get(const char *str, size_t len) {
      if (str == 0)
        return 0;
      CstrSet::iterator iter = m_strings.find(str);
      if (iter == m_strings.end())
        iter = m_strings.insert(m_arena.dup(str, len)).first;
      return *iter;
    }

    /// Clears and deallocates the set of strings.
    /// This function walks through #m_strings, deallocating each string it
    /// finds, and then clears #m_strings.
    void clear() {
      m_strings.clear();
      m_arena.free();
      m_strings = CstrSet(LtCstr(), CstrSetAlloc(m_arena));
    }

  private:
    typedef PageArenaAllocator<const char*> CstrSetAlloc;
    typedef std::set<const char*, LtCstr, CstrSetAlloc> CstrSet;

    /// The page arena
    CharArena m_arena;

    /// The std::set holding the strings
    CstrSet m_strings;
  };

  /// @}

}

#endif // HYPERTABLE_FLYWEIGHTSTRING_H
