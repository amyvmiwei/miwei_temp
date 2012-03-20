/**
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
#include "Common/Init.h"

#include <iostream>

#include "Checksum.h"
#include "FileUtils.h"
#include "WordStream.h"

using namespace Hypertable;
using namespace Hypertable::Config;
using namespace std;

namespace {

  const char *usage =
    "\n"
    "Usage: ht_wordstream [options] <words-per-record> [<word-file>]\n\n"
    "Description:\n"
    "  This program is used to generate a stream of records, where each recored\n"
    "  constists of one or more randomly chosen words concatenated together with\n"
    "  a ' ' character.  The <words-per-record> argument controls how many words\n"
    "  each record should consist of, and the <word-file> argument can be used to\n"
    "  specify the file containing the source words, this argument defaults to\n"
    "  /usr/share/dict/words.\n\n"
    "Options";

  struct MyPolicy : Config::Policy {
    static void init_options() {
      cmdline_desc(usage).add_options()
        ("limit,l", i64(), "Limit on the number of records to output")
        ("seed,s", i32()->default_value(1), "Pseudo-random number generator seed")
        ;
      cmdline_hidden_desc().add_options()
        ("words-per-record", i32(), "Number of words for each output record.")
        ("word-file", str()->default_value("/usr/share/dict/words"),
         "Number of words for each output record.")
        ;
      cmdline_positional_desc().add("words-per-record", 1).add("word-file", -1);
    }
  };
}


typedef Meta::list<MyPolicy, DefaultPolicy> Policies;



/**
 *
 */
int main(int argc, char **argv) {

  try {
    init_with_policies<Policies>(argc, argv);

    if (!has("words-per-record")) {
      cout << cmdline_desc() << flush;
      _exit(0);
    }

    WordStreamPtr word_stream = new WordStream(get_str("word-file"), 1, 2, false);

    while (true)
      cout << word_stream->next() << "\n";

  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    _exit(1);
  }

  cout << std::flush;
  _exit(0);
}
