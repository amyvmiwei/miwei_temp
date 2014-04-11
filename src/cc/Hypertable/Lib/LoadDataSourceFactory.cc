/** -*- c++ -*-
 * Copyright (C) 2007-2012 Hypertable, Inc.
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
#include "HqlInterpreter.h"
#include "LoadDataSourceFactory.h"
#include "LoadDataSourceFileLocal.h"
#include "LoadDataSourceFileDfs.h"
#include "LoadDataSourceStdin.h"

using namespace Hypertable;
using namespace std;

/**
 *
 */
LoadDataSource *
LoadDataSourceFactory::create(FsBroker::ClientPtr &dfs_client,
    const String &input_fname, const int src,
    const String &header_fname, const int header_src,
    const std::vector<String> &key_columns, const String &timestamp_column,
    char field_separator, int row_uniquify_chars, int load_flags) {

  LoadDataSource *lds;

  switch (src) {
    case LOCAL_FILE:
      lds = new LoadDataSourceFileLocal(input_fname, header_fname,
                                        row_uniquify_chars, load_flags);
      break;
    case DFS_FILE:
      lds = new LoadDataSourceFileDfs(dfs_client, input_fname, header_fname,
                                      row_uniquify_chars, load_flags );
      break;
    case STDIN:
      lds = new LoadDataSourceStdin(header_fname, row_uniquify_chars, load_flags);
      break;
    default:
      HT_THROW(Error::HQL_PARSE_ERROR, "LOAD DATA - bad filename");
  }

  lds->init(key_columns, timestamp_column, field_separator);
  return lds;
}

