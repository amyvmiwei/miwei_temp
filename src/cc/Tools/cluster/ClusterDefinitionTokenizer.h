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

#ifndef Tools_cluster_ClusterDefinitionTokenizer_h
#define Tools_cluster_ClusterDefinitionTokenizer_h

#include <string>

namespace Hypertable {

  using namespace std;

  class ClusterDefinitionTokenizer {

  public:

    class Token {
    public:
      enum {
        NONE=0,
        VARIABLE=1,
        ROLE=2,
        TASK=3,
        FUNCTION=4,
        COMMENT=5,
        CODE=6
      };
      void clear() { type=NONE; text.clear(); line=0; fname.clear(); }
      int type {};
      string text;
      size_t line {};
      string fname;
    };

    ClusterDefinitionTokenizer(const string &fname);

    bool next(Token &token);

  private:

    int identify_line_type(const char *base, const char *end);

    bool accumulate(const char **basep, const char *endp, int type, Token &token);

    string m_definition_file;
    const char *m_next {};
    size_t m_line {};
  };

}

#endif // Tools_cluster_ClusterDefinitionTokenizer_h
