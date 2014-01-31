/* -*- c++ -*-
 * Copyright (C) 2007-2014 Hypertable, Inc.
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
/// Declarations for CellPredicate.
/// This file contains the type declarations for the CellPredicate, a class that
/// represents the cell predicate for a column family

#ifndef HYPERTABLE_CELLPREDICATE_H
#define HYPERTABLE_CELLPREDICATE_H

#include <Hypertable/Lib/ScanSpec.h>

#include<re2/re2.h>

#include <boost/shared_ptr.hpp>

#include <bitset>
#include <vector>

namespace Hypertable {

  /// @addtogroup RangeServer
  /// @{

  /// Cell predicate.
  class CellPredicate {

    struct CellPattern {
      CellPattern(const ColumnPredicate &cp, size_t id) : 
        qualifier(cp.column_qualifier), value(cp.value),
        qualifier_len(cp.column_qualifier_len), value_len(cp.value_len),
        operation(cp.operation), id(id) { }
      bool regex_qualifier_match(const char *str) {
        if (!qualifier_regex) {
          String pattern(qualifier, (size_t)qualifier_len);
          qualifier_regex.reset(new RE2(pattern));
        }
        return RE2::PartialMatch(str, *qualifier_regex);
      }
      bool regex_value_match(const char *str) {
        if (!value_regex) {
          String pattern(value, (size_t)value_len);
          value_regex.reset(new RE2(pattern));
        }
        return RE2::PartialMatch(str, *value_regex);
      }
      const char *qualifier;
      const char *value;
      uint32_t qualifier_len;
      uint32_t value_len;
      uint32_t operation;
      boost::shared_ptr<RE2> value_regex;
      boost::shared_ptr<RE2> qualifier_regex;
      size_t id;
    };

  public:

    /// Default constructor.
    CellPredicate() :
      cutoff_time(0), max_versions(0), counter(false), indexed(false) { }

    void all_matches(const char *qualifier, size_t qualifier_len,
                     const char* value, size_t value_len,
                     std::bitset<32> &matching) {
      foreach_ht (CellPattern &cp, patterns) {
        if (pattern_match(cp, qualifier, qualifier_len, value, value_len))
          matching.set(cp.id);
      }
    }

    /// Evaluates predicate for the given cell.
    /// @param qualifier Cell column qualifier
    /// @param qualifier_len Cell column qualifier length
    /// @param value Cell value
    /// @param value_len Cell value length
    /// @return <i>true</i> if predicate matches, otherwise <i>false</i>
    bool matches(const char *qualifier, size_t qualifier_len,
                 const char* value, size_t value_len) {
      if (patterns.empty())
        return true;
      foreach_ht (CellPattern &cp, patterns) {
        if (pattern_match(cp, qualifier, qualifier_len, value, value_len))
          return true;
      }
      return false;
    }

    bool pattern_match(CellPattern &cp, const char *qualifier,
                       size_t qualifier_len, const char* value,
                       size_t value_len) {

      // Qualifier match
      if (cp.operation & ColumnPredicate::QUALIFIER_MATCH) {
        if (cp.operation & ColumnPredicate::QUALIFIER_EXACT_MATCH) {
          if (qualifier_len != cp.qualifier_len ||
              memcmp(qualifier, cp.qualifier, qualifier_len))
            return false;
        }
        else if (cp.operation & ColumnPredicate::QUALIFIER_PREFIX_MATCH) {
          if (qualifier_len < cp.qualifier_len)
            return false;
          const char *p1 = qualifier;
          const char *p2 = cp.qualifier;
          const char *prefix_end = cp.qualifier + cp.qualifier_len;
          for (; p2 < prefix_end; ++p1,++p2) {
            if (*p1 != *p2)
              break;
          }
          if (p2 != prefix_end)
            return false;
        }
        else if (cp.operation & ColumnPredicate::QUALIFIER_REGEX_MATCH) {
          if (!cp.regex_qualifier_match(qualifier))
            return false;
        }
      }
      else if (*qualifier)
        return false;

      // Value match
      if (cp.operation & ColumnPredicate::VALUE_MATCH) {
        if (cp.operation & ColumnPredicate::EXACT_MATCH) {
          if (cp.value_len != value_len ||
              memcmp(cp.value, value, cp.value_len))
            return false;
        }
        else if (cp.operation & ColumnPredicate::PREFIX_MATCH) {
          if (cp.value_len > value_len ||
              memcmp(cp.value, value, cp.value_len))
            return false;
        }
        else if (cp.operation & ColumnPredicate::REGEX_MATCH) {
          if (!cp.regex_value_match(value))
            return false;
        }
      }
      return true;
    }

    void add_column_predicate(const ColumnPredicate &column_predicate, size_t id) {
      patterns.push_back(CellPattern(column_predicate, id));
    }

    /// TTL cutoff time
    int64_t cutoff_time;

    /// Max versions (0 for all versions)
    uint32_t max_versions;

    /// Column family has counter option
    bool counter;

    /// Column family is indexed
    bool indexed;

  private:

    /// Vector of patterns used in predicate match
    std::vector<CellPattern> patterns;
  };

  /// @}
}

#endif // HYPERTABLE_CELLPREDICATE_H
