/*
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

#include "Common/Compat.h"

#include <boost/algorithm/string.hpp>

#include <cstring>
#include <iostream>

#include "Common/Serialization.h"

#include "KeySpec.h"
#include "ScanSpec.h"

using namespace std;
using namespace Hypertable;
using namespace Serialization;

size_t ColumnPredicate::encoded_length() const {
  return sizeof(uint32_t)
    + encoded_length_vstr(column_family)
    + encoded_length_vstr(column_qualifier)
    + encoded_length_vstr(value_len)
    + encoded_length_vstr(column_qualifier_len);
}

void ColumnPredicate::encode(uint8_t **bufp) const {
  encode_vstr(bufp, column_family);
  encode_vstr(bufp, column_qualifier);
  encode_vstr(bufp, value, value_len);
  encode_vstr(bufp, column_qualifier, column_qualifier_len);
  encode_i32(bufp, operation);
}

void ColumnPredicate::decode(const uint8_t **bufp, size_t *remainp) {
  HT_TRY("decoding column predicate",
         column_family = decode_vstr(bufp, remainp);
         column_qualifier = decode_vstr(bufp, remainp);
         value = decode_vstr(bufp, remainp, &value_len);
         column_qualifier = decode_vstr(bufp, remainp, &column_qualifier_len);
         operation = decode_i32(bufp, remainp));
}

size_t RowInterval::encoded_length() const {
  return 2 + encoded_length_vstr(start) + encoded_length_vstr(end);
}

void RowInterval::encode(uint8_t **bufp) const {
  encode_vstr(bufp, start);
  encode_bool(bufp, start_inclusive);
  encode_vstr(bufp, end);
  encode_bool(bufp, end_inclusive);
}


void RowInterval::decode(const uint8_t **bufp, size_t *remainp) {
  HT_TRY("decoding row interval",
    start = decode_vstr(bufp, remainp);
    start_inclusive = decode_bool(bufp, remainp);
    end = decode_vstr(bufp, remainp);
    end_inclusive = decode_bool(bufp, remainp));
}

size_t CellInterval::encoded_length() const {
  return 2 + encoded_length_vstr(start_row) + encoded_length_vstr(start_column)
      + encoded_length_vstr(end_row) + encoded_length_vstr(end_column);
}

void CellInterval::encode(uint8_t **bufp) const {
  encode_vstr(bufp, start_row);
  encode_vstr(bufp, start_column);
  encode_bool(bufp, start_inclusive);
  encode_vstr(bufp, end_row);
  encode_vstr(bufp, end_column);
  encode_bool(bufp, end_inclusive);
}


void CellInterval::decode(const uint8_t **bufp, size_t *remainp) {
  HT_TRY("decoding cell interval",
    start_row = decode_vstr(bufp, remainp);
    start_column = decode_vstr(bufp, remainp);
    start_inclusive = decode_bool(bufp, remainp);
    end_row = decode_vstr(bufp, remainp);
    end_column = decode_vstr(bufp, remainp);
    end_inclusive = decode_bool(bufp, remainp));
}

size_t ScanSpec::encoded_length() const {
  size_t len = encoded_length_vi32(row_limit) +
               encoded_length_vi32(cell_limit) +
               encoded_length_vi32(cell_limit_per_family) +
               encoded_length_vi32(max_versions) +
               encoded_length_vi32(columns.size()) +
               encoded_length_vi32(row_intervals.size()) +
               encoded_length_vi32(cell_intervals.size()) +
               encoded_length_vi32(column_predicates.size()) +
               encoded_length_vstr(row_regexp) +
               encoded_length_vstr(value_regexp) +
               encoded_length_vi32(row_offset) +
               encoded_length_vi32(cell_offset) +
               rebuild_indices.encoded_length();

  foreach_ht(const char *c, columns) len += encoded_length_vstr(c);
  foreach_ht(const RowInterval &ri, row_intervals) len += ri.encoded_length();
  foreach_ht(const CellInterval &ci, cell_intervals) len += ci.encoded_length();
  foreach_ht(const ColumnPredicate &cp, column_predicates) len += cp.encoded_length();

  return len + 8 + 8 + 5;
}

void ScanSpec::encode(uint8_t **bufp) const {
  encode_vi32(bufp, row_limit);
  encode_vi32(bufp, cell_limit);
  encode_vi32(bufp, cell_limit_per_family);
  encode_vi32(bufp, max_versions);
  encode_vi32(bufp, columns.size());
  foreach_ht(const char *c, columns) encode_vstr(bufp, c);
  encode_vi32(bufp, row_intervals.size());
  foreach_ht(const RowInterval &ri, row_intervals) ri.encode(bufp);
  encode_vi32(bufp, cell_intervals.size());
  foreach_ht(const CellInterval &ci, cell_intervals) ci.encode(bufp);
  encode_vi32(bufp, column_predicates.size());
  foreach_ht(const ColumnPredicate &cp, column_predicates) cp.encode(bufp);
  encode_i64(bufp, time_interval.first);
  encode_i64(bufp, time_interval.second);
  encode_bool(bufp, return_deletes);
  encode_bool(bufp, keys_only);
  encode_vstr(bufp, row_regexp);
  encode_vstr(bufp, value_regexp);
  encode_bool(bufp, scan_and_filter_rows);
  encode_bool(bufp, do_not_cache);
  encode_bool(bufp, and_column_predicates);
  rebuild_indices.encode(bufp);
  encode_vi32(bufp, row_offset);
  encode_vi32(bufp, cell_offset);
}

void ScanSpec::decode(const uint8_t **bufp, size_t *remainp) {
  RowInterval ri;
  CellInterval ci;
  ColumnPredicate cp;
  HT_TRY("decoding scan spec",
    row_limit = decode_vi32(bufp, remainp);
    cell_limit = decode_vi32(bufp, remainp);
    cell_limit_per_family = decode_vi32(bufp, remainp);
    max_versions = decode_vi32(bufp, remainp);
    for (size_t nc = decode_vi32(bufp, remainp); nc--;)
      columns.push_back(decode_vstr(bufp, remainp));
    for (size_t nri = decode_vi32(bufp, remainp); nri--;) {
      ri.decode(bufp, remainp);
      row_intervals.push_back(ri);
    }
    for (size_t nci = decode_vi32(bufp, remainp); nci--;) {
      ci.decode(bufp, remainp);
      cell_intervals.push_back(ci);
    }
    for (size_t nri = decode_vi32(bufp, remainp); nri--;) {
      cp.decode(bufp, remainp);
      column_predicates.push_back(cp);
    }
    time_interval.first = decode_i64(bufp, remainp);
    time_interval.second = decode_i64(bufp, remainp);
    return_deletes = decode_bool(bufp, remainp);
    keys_only = decode_bool(bufp, remainp);
    row_regexp = decode_vstr(bufp, remainp);
    value_regexp = decode_vstr(bufp, remainp);
    scan_and_filter_rows = decode_bool(bufp, remainp);
    do_not_cache = decode_bool(bufp, remainp);
    and_column_predicates = decode_bool(bufp, remainp);
    rebuild_indices.decode(bufp, remainp);
    row_offset = decode_vi32(bufp, remainp);
    cell_offset = decode_vi32(bufp, remainp));
}


/** @relates RowInterval */
ostream &Hypertable::operator<<(ostream &os, const RowInterval &ri) {
  os <<"{RowInterval: ";
  if (ri.start)
    os << "\"" << ri.start << "\"";
  else
    os << "NULL";
  if (ri.start_inclusive)
    os << " <= row ";
  else
    os << " < row ";
  if (ri.end_inclusive)
    os << "<= ";
  else
    os << "< ";
  if (ri.end)
    os << "\"" << ri.end << "\"";
  else
    os << "0xff 0xff";
  os << "}";
  return os;
}

/** @relates CellInterval */
ostream &Hypertable::operator<<(ostream &os, const CellInterval &ci) {
  os <<"{CellInterval: ";
  if (ci.start_row)
    os << "\"" << ci.start_row << "\",\"" << ci.start_column << "\"";
  else
    os << "NULL";
  if (ci.start_inclusive)
    os << " <= cell ";
  else
    os << " < cell ";
  if (ci.end_inclusive)
    os << "<= ";
  else
    os << "< ";
  if (ci.end_row)
    os << "\"" << ci.end_row << "\",\"" << ci.end_column << "\"";
  else
    os << "0xff 0xff";
  os << "}";
  return os;
}


/** @relates ScanSpec */
ostream &Hypertable::operator<<(ostream &os, const ScanSpec &scan_spec) {
  os <<"\n{ScanSpec: row_limit="<< scan_spec.row_limit
     <<" cell_limit="<< scan_spec.cell_limit
     <<" cell_limit_per_family="<< scan_spec.cell_limit_per_family
     <<" max_versions="<< scan_spec.max_versions
     <<" return_deletes="<< scan_spec.return_deletes
     <<" keys_only="<< scan_spec.keys_only;
  os <<" row_regexp=" << (scan_spec.row_regexp ? scan_spec.row_regexp : "");
  os <<" value_regexp=" << (scan_spec.value_regexp ? scan_spec.value_regexp : "");
  os <<" scan_and_filter_rows=" << scan_spec.scan_and_filter_rows;
  os <<" do_not_cache=" << scan_spec.do_not_cache;
  os <<" and_column_predicates=" << scan_spec.and_column_predicates;
  os <<" rebuild_indices=" << scan_spec.rebuild_indices.to_string();
  os <<" row_offset=" << scan_spec.row_offset;
  os <<" cell_offset=" << scan_spec.cell_offset;

  if (!scan_spec.row_intervals.empty()) {
    os << "\n rows=";
    foreach_ht(const RowInterval &ri, scan_spec.row_intervals)
      os << " " << ri;
  }
  if (!scan_spec.cell_intervals.empty()) {
    os << "\n cells=";
    foreach_ht(const CellInterval &ci, scan_spec.cell_intervals)
      os << " " << ci;
  }
  if (!scan_spec.column_predicates.empty()) {
    os << "\n column_predicates=";
    foreach_ht(const ColumnPredicate &cp, scan_spec.column_predicates)
      os << " (" << cp.column_family << " " << cp.operation << " " 
         << cp.value << ")";
  }
  if (!scan_spec.columns.empty()) {
    os << "\n columns=(";
    foreach_ht (const char *c, scan_spec.columns)
      os <<"'"<< c << "' ";
    os <<')';
  }
  os <<"\n time_interval=(" << scan_spec.time_interval.first <<", "
     << scan_spec.time_interval.second <<")\n}\n";

  return os;
}


ScanSpec::ScanSpec(CharArena &arena, const ScanSpec &ss)
  : row_limit(ss.row_limit), cell_limit(ss.cell_limit), 
    cell_limit_per_family(ss.cell_limit_per_family),
    row_offset(ss.row_offset), cell_offset(ss.cell_offset),
    max_versions(ss.max_versions), columns(CstrAlloc(arena)), 
    row_intervals(RowIntervalAlloc(arena)),
    cell_intervals(CellIntervalAlloc(arena)),
    column_predicates(ColumnPredicateAlloc(arena)),
    time_interval(ss.time_interval.first, ss.time_interval.second),
    row_regexp(arena.dup(ss.row_regexp)), value_regexp(arena.dup(ss.value_regexp)),
    return_deletes(ss.return_deletes), keys_only(ss.keys_only),
    scan_and_filter_rows(ss.scan_and_filter_rows),
    do_not_cache(ss.do_not_cache), and_column_predicates(ss.and_column_predicates),
    rebuild_indices(ss.rebuild_indices) {
  columns.reserve(ss.columns.size());
  row_intervals.reserve(ss.row_intervals.size());
  cell_intervals.reserve(ss.cell_intervals.size());
  column_predicates.reserve(ss.column_predicates.size());

  foreach_ht(const char *c, ss.columns)
    add_column(arena, c);

  foreach_ht(const RowInterval &ri, ss.row_intervals)
    add_row_interval(arena, ri.start, ri.start_inclusive,
                     ri.end, ri.end_inclusive);

  foreach_ht(const CellInterval &ci, ss.cell_intervals)
    add_cell_interval(arena, ci.start_row, ci.start_column, ci.start_inclusive,
                      ci.end_row, ci.end_column, ci.end_inclusive);

  foreach_ht(const ColumnPredicate &cp, ss.column_predicates)
    add_column_predicate(arena, cp.column_family, cp.column_qualifier,
                         cp.operation, cp.value);
}

void
ScanSpec::parse_column(const char *column_str, String &family, 
                       const char **qualifier, size_t *qualifier_len,
                       bool *has_qualifier, bool *is_regexp, bool *is_prefix)
{
  const char *raw_qualifier;
  size_t raw_qualifier_len;
  const char *colon = strchr(column_str, ':');
  *is_regexp = false;
  *is_prefix = false;
  *qualifier = "";
  *qualifier_len = 0;

  if (colon == 0) {
    *has_qualifier = false;
    family = column_str;
    return;
  }
  *has_qualifier = true;

  family = String(column_str, (size_t)(colon-column_str));

  raw_qualifier = colon+1;
  raw_qualifier_len = strlen(raw_qualifier);

  if (raw_qualifier_len == 0)
    return;

  if (raw_qualifier_len > 2 &&
      raw_qualifier[0] == '/' && raw_qualifier[raw_qualifier_len-1] == '/') {
    *is_regexp = true;
    *qualifier = raw_qualifier+1;
    *qualifier_len = raw_qualifier_len - 2;
  }
  else if (*raw_qualifier == '*') {
    *is_prefix = true;
  }
  else {
    if (*raw_qualifier == '^') {
      *is_prefix = true;
      strip_enclosing_quotes(raw_qualifier+1, raw_qualifier_len-1,
                             qualifier, qualifier_len);
    }
    else
      strip_enclosing_quotes(raw_qualifier, raw_qualifier_len,
                             qualifier, qualifier_len);
  }
}
