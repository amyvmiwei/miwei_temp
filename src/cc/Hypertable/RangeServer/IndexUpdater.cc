/** -*- c++ -*-
 * Copyright (C) 2011 Hypertable Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
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
#include "Common/Filesystem.h"
#include "Common/Config.h"
#include "Hypertable/Lib/LoadDataEscape.h"
#include "Hypertable/Lib/Schema.h"
#include "Hypertable/Lib/ResultCallback.h"
#include "IndexUpdater.h"

namespace Hypertable {

class IndexUpdaterCallback : public ResultCallback {
public:
  virtual void scan_ok(TableScannerAsync *scanner, ScanCellsPtr &cells) { }
  virtual void scan_error(TableScannerAsync *scanner, int error,
          const String &error_msg, bool eos) { }
  virtual void update_ok(TableMutatorAsync *mutator) { }
  virtual void update_error(TableMutatorAsync *mutator, int error,
          FailedMutations &failedMutations) { }
};

IndexUpdater::IndexUpdater(SchemaPtr &primary_schema, TablePtr index_table, 
                           TablePtr qualifier_index_table)
  : m_index_mutator(0), m_qualifier_index_mutator(0), m_highest_column_id(0)
{
  m_cb = new IndexUpdaterCallback();
  if (index_table)
    m_index_mutator = index_table->create_mutator_async(m_cb);
  if (qualifier_index_table)
    m_qualifier_index_mutator = qualifier_index_table->create_mutator_async(m_cb);
  memset(&m_index_map[0], 0, sizeof(m_index_map));
  memset(&m_qualifier_index_map[0], 0, sizeof(m_qualifier_index_map));

  foreach_ht (const Schema::ColumnFamily *cf, 
          primary_schema->get_column_families()) {
    if (!cf || cf->deleted)
      continue;
    if (cf->id > m_highest_column_id)
      m_highest_column_id = cf->id;

    if (cf->has_index)
      m_index_map[cf->id] = true;
    if (cf->has_qualifier_index)
      m_qualifier_index_map[cf->id] = true;
    m_cf_namemap[cf->id] = cf->name;
  }
}

void IndexUpdater::purge(const Key &key, const ByteString &value)
{
  const uint8_t *dptr = value.ptr;
  size_t value_len = Serialization::decode_vi32(&dptr);

  HT_ASSERT(key.column_family_code != 0);

  try {
    if (m_index_map[key.column_family_code]) {
      // create the key for the index
      KeySpec k;
      k.timestamp = key.timestamp;
      k.flag = FLAG_DELETE_ROW;
      k.column_family = "v1";

      // every \t in the original row key gets escaped
      const char *row;
      size_t rowlen;
      LoadDataEscape lde, ldev;
      lde.escape(key.row, key.row_len, &row, &rowlen);

      // also escape the value if it contains \0
      const char *val_ptr = (const char *)dptr;
      for (const char *v = val_ptr; v < val_ptr + value_len; v++) {
        if (*v == '\0') {
          const char *outp;
          ldev.escape(val_ptr, (size_t)value_len, 
                      &outp, (size_t *)&value_len);
          dptr = (const uint8_t *)outp;
          break;
        }
      }

      // in a normal (non-qualifier) index the format of the new row
      // key is "value\trow"
      StaticBuffer sb(4 + value_len + rowlen + 1 + 1);
      char *p = (char *)sb.base;
      sprintf(p, "%d,", (int)key.column_family_code);
      p     += strlen(p);
      memcpy(p, dptr, value_len);
      p     += value_len;
      *p++  = '\t';
      memcpy(p, row, rowlen);
      p     += rowlen;
      *p++  = '\0';
      k.row = sb.base;
      k.row_len = p - 1 - (const char *)sb.base; /* w/o the terminating zero */

      // and insert it
      m_index_mutator->set_delete(k);
    }

    if (m_qualifier_index_map[key.column_family_code]) {
      // create the key for the index
      KeySpec k;
      k.timestamp = key.timestamp;
      k.flag = FLAG_DELETE_ROW;
      k.column_family = "v1";

      // every \t in the original row key gets escaped
      const char *row;
      size_t rowlen;
      LoadDataEscape lde;
      lde.escape(key.row, key.row_len, &row, &rowlen);

      // in a qualifier index the format of the new row key is "qualifier\trow"
      size_t qlen = key.column_qualifier ? strlen(key.column_qualifier) : 0;
      StaticBuffer sb(4 + qlen + rowlen + 1 + 1);
      char *p = (char *)sb.base;
      sprintf(p, "%d,", (int)key.column_family_code);
      p     += strlen(p);
      if (qlen) {
        memcpy(p, key.column_qualifier, qlen);
        p   += qlen;
      }
      *p++  = '\t';
      memcpy(p, row, rowlen);
      p     += rowlen;
      *p++  = '\0';
      k.row = sb.base;
      k.row_len = p - 1 - (const char *)sb.base; /* w/o the terminating zero */

      // and insert it
      m_qualifier_index_mutator->set_delete(k);
    }
  }
  // log errors, but don't re-throw them; otherwise the whole compaction 
  // will stop
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
  }
}

IndexUpdater *IndexUpdaterFactory::create(const String &table_id,
                    SchemaPtr &schema, bool has_index, bool has_qualifier_index)
{
  TablePtr index_table;
  TablePtr qualifier_index_table;

  HT_ASSERT(has_index || has_qualifier_index);

  ScopedLock lock(ms_mutex);

  // check if we've cached Table pointers for the indices
  if (has_index)
    index_table = ms_index_cache[table_id];
  if (has_qualifier_index)
    qualifier_index_table = ms_qualifier_index_cache[table_id];

  if ((has_index && index_table) 
        && (has_qualifier_index && qualifier_index_table)) {
    return new IndexUpdater(schema, index_table, qualifier_index_table);
  }

  // at least one index table was not cached: load it
  if (!ms_namemap)
    ms_namemap = new NameIdMapper(Global::hyperspace, Global::toplevel_dir);

  String table_name;
  if (!ms_namemap->id_to_name(table_id, table_name)) {
    HT_WARNF("Failed to map table id %s to table name", table_id.c_str());
    return 0;
  }

  if (has_index && !index_table) {
    String dir = Filesystem::dirname(table_name);
    String base = Filesystem::basename(table_name);
    String indexname = dir != "." 
                        ? dir + "/^" + base
                        : "^" + base;
    index_table = load_table(indexname);
    HT_ASSERT(index_table != 0);
    ms_index_cache[table_id] = index_table;
  }

  if (has_qualifier_index && !qualifier_index_table) {
    String dir = Filesystem::dirname(table_name);
    String base = Filesystem::basename(table_name);
    String indexname = dir != "." 
                        ? dir + "/^^" + base
                        : "^^" + base;
    qualifier_index_table = load_table(indexname);
    HT_ASSERT(qualifier_index_table != 0);
    ms_qualifier_index_cache[table_id] = qualifier_index_table;
  }

  if (index_table || qualifier_index_table)
    return new IndexUpdater(schema, index_table, qualifier_index_table);
  else
    return (0);
}

void IndexUpdaterFactory::close()
{
  ScopedLock lock(ms_mutex);

  ms_namemap = 0;

  ms_index_cache.clear();
  ms_qualifier_index_cache.clear();
}

Table *IndexUpdaterFactory::load_table(const String &table_name)
{
  return new Table(Config::properties, Global::conn_manager,
                       Global::hyperspace, ms_namemap, table_name);
}

Mutex IndexUpdaterFactory::ms_mutex;
NameIdMapperPtr IndexUpdaterFactory::ms_namemap;
IndexUpdaterFactory::TableMap IndexUpdaterFactory::ms_index_cache;
IndexUpdaterFactory::TableMap IndexUpdaterFactory::ms_qualifier_index_cache;

} // namespace Hypertable

