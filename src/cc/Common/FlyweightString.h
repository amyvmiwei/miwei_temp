/*
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

#include "StringExt.h"

namespace Hypertable {

  /// @addtogroup Common
  /// @{

  /**
   * The Flyweight string set stores duplicate strings efficiently.
   */
  class FlyweightString {
  public:
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
      if (iter != m_strings.end())
        return *iter;
      char *constant_str = new char [strlen(str) + 1];
      strcpy(constant_str, str);
      m_strings.insert(constant_str);
      return constant_str;
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
      char *new_str = new char [ len + 1 ];
      memcpy(new_str, str, len);
      new_str[len] = 0;
      CstrSet::iterator iter = m_strings.find(new_str);
      if (iter != m_strings.end()) {
        delete [] new_str;
        return *iter;
      }
      m_strings.insert(new_str);
      return new_str;
    }

    /// Clears and deallocates the set of strings.
    /// This function walks through #m_strings, deallocating each string it
    /// finds, and then clears #m_strings.
    void clear() {
      for (CstrSet::iterator it=m_strings.begin(); it!=m_strings.end(); ++it)
        delete [] *it;
      m_strings.clear();
    }

  private:

    /// The std::set holding the strings
    CstrSet m_strings;
  };

  /// @}

}

#endif // HYPERTABLE_FLYWEIGHTSTRING_H
