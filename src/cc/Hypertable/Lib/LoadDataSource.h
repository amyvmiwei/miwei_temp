/* -*- c++ -*-
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

#ifndef Hypertable_Lib_LoadDataSource_h
#define Hypertable_Lib_LoadDataSource_h

#include <Hypertable/Lib/DataSource.h>
#include <Hypertable/Lib/FixedRandomStringGenerator.h>
#include <Hypertable/Lib/LoadDataFlags.h>

#include <Common/ByteString.h>
#include <Common/DynamicBuffer.h>
#include <Common/String.h>

#include <boost/iostreams/device/file.hpp>
#include <boost/iostreams/filtering_stream.hpp>

#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

namespace Hypertable {

  enum {
    LOCAL_FILE = 1,
    DFS_FILE,
    STDIN
  };

  class LoadDataSource {

  public:
    LoadDataSource(const std::string &header_fname,
                   int row_uniquify_chars = 0,
                   int load_flags = 0);

    virtual ~LoadDataSource() { delete [] m_type_mask; return; }

    bool has_timestamps() {
      return m_leading_timestamps || (m_timestamp_index != -1);
    }

    virtual bool next(KeySpec *keyp, uint8_t **valuep, uint32_t *value_lenp,
                      bool *is_deletep, uint32_t *consumedp);

    virtual void init(const std::vector<String> &key_columns, 
                      const std::string &timestamp_column,
                      char field_separator);

    int64_t get_current_lineno() { return m_cur_line; }
    unsigned long get_source_size() const { return m_source_size; }

  protected:

    bool get_next_line(String &line) {
      if (m_first_line_cached) {
        line = m_first_line;
        m_first_line_cached = false;
        return true;
      }
      getline(m_fin, line);
      if (m_fin.eof() && line.empty())
        return false;
      return true;
    }

    virtual void parse_header(const String& header,
                              const std::vector<String> &key_columns,
                              const std::string &timestamp_column);
    virtual void init_src()=0;
    virtual uint64_t incr_consumed()=0;

    bool should_skip(int idx, const uint32_t *masks) {
      uint32_t bm = masks[idx];
      return bm && ((bm & TIMESTAMP) ||
            !(LoadDataFlags::duplicate_key_columns(m_load_flags) 
                && (bm & ROW_KEY)));
    }

    class KeyComponentInfo {
    public:
      KeyComponentInfo()
          : index(0), width(0), left_justify(false), pad_character(' ') {}
      void clear() { index=0; width=0; left_justify=false; pad_character=' '; }
      int index;
      int width;
      bool left_justify;
      char pad_character;
    };

    enum TypeMask {
      ROW_KEY =           (1 << 0),
      TIMESTAMP =         (1 << 1)
    };

    std::string get_header();

    bool parse_date_format(const char *str, int64_t &timestamp);
    bool parse_sec(const char *str, char **end_ptr, int64_t &ns);
    bool add_row_component(int index);

    struct ColumnInfo {
      std::string family;
      std::string qualifier;
    };

    std::vector<ColumnInfo> m_column_info;
    std::vector<const char *> m_values;
    std::vector<KeyComponentInfo> m_key_comps;
    uint32_t *m_type_mask;
    size_t m_next_value;
    boost::iostreams::filtering_istream m_fin;
    int64_t m_cur_line;
    DynamicBuffer m_line_buffer;
    DynamicBuffer m_row_key_buffer;
    bool m_hyperformat;
    bool m_leading_timestamps;
    int m_timestamp_index;
    int64_t m_timestamp;
    size_t m_limit;
    uint64_t m_offset;
    bool m_zipped;
    FixedRandomStringGenerator *m_rsgen;
    std::string m_header_fname;
    int m_row_uniquify_chars;
    int m_load_flags;
    std::string m_first_line;
    unsigned long m_source_size;
    bool m_first_line_cached;
    char m_field_separator;
  };

  /// Smart pointer to LoadDataSource
  typedef std::shared_ptr<LoadDataSource> LoadDataSourcePtr;

}

#endif // Hypertable_Lib_LoadDataSource_h
