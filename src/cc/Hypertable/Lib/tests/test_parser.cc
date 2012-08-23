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

#include "Hypertable/Lib/HqlParser2.h"

using namespace std;

int main(int argc, char **argv) {

  if (!(argc == 2 && !strcmp(argv[1], "-q")))
    cout << "Enter HQL statements, one-per-line:\n";

  using boost::spirit::ascii::space;
  typedef string::const_iterator iterator_type;
  typedef Hql::hql_parser<iterator_type> hql_parser;

  hql_parser g; // Our grammar
  string str;
  while (getline(cin, str)) {
      if (str.empty() || str[0] == '#')
        continue;

      Hql::ParserState state;
      string::const_iterator iter = str.begin();
      string::const_iterator end = str.end();
      bool r = phrase_parse(iter, end, g, space, state);

      if (r && iter == end) {
        cout << state << endl;
      }
      /*
      else {
        cout << "Parsing failed: " << *iter << endl;
      }
      */
  }

  return 0;
}
