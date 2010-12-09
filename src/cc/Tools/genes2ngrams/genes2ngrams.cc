/**
 * Copyright (C) 2009 Doug Judd (Zvents, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License.
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

#include <cctype>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>
#include <map>

extern "C" {
#include <time.h>
#include <sys/time.h>
}

#include "AsyncComm/Config.h"
#include "Common/DynamicBuffer.h"
#include "Common/Init.h"
#include "Common/Logger.h"
#include "Common/PageArena.h"
#include "Common/StringExt.h"

using namespace Hypertable;
using namespace Hypertable::Config;
using namespace std;

namespace {

  const char *usage =
    "\nusage: prune_tsv [options] <past-date-offset>\n\n"
    "description:\n"
    "  This program converts a Genes table load file (.tsv) to an Ngrams table\n"
    "  load file\n"
    "options";

  struct AppPolicy : Policy {
    static void init_options() {
      cmdline_desc(usage).add_options()
        ("n", i32()->default_value(17), "Length of ngram to generate")
        ;
    }
  };

  typedef Meta::list<AppPolicy, DefaultCommPolicy> Policies;

  inline char *get_field(char *line, int fieldno, char **endptr) {
    char *ptr, *base = line;

    ptr = strchr(base, '\t');
    while (fieldno && ptr) {
      fieldno--;
      base = ptr+1;
      ptr = strchr(base, '\t');
    }

    if (fieldno > 0)
      return 0;

    if (ptr) {
      *ptr = 0;
      *endptr = ptr;
    }
    return base;
  }

  size_t n;

  struct ltngram {
    bool operator()(const char* s1, const char* s2) const {
      return strncmp(s1, s2, n) < 0;
    }
  };
}



/**
 *
 */
int main(int argc, char **argv) {
  const char *sequence;
  char *ptr, *value, *end = 0;
  char buf[32];
  size_t length;
  uint64_t lineno = 0;
  DynamicBuffer output;
  map<const char*, const char *, ltngram> ngrams;
  map<const char*, const char *, ltngram>::iterator iter;
  CharArena arena;
  char *bad_ngram = new char [n+1];

  // setup bad ngram buffer (all 'N')
  memset(bad_ngram, 'N', n);
  bad_ngram[n]=0;

  char *line_buffer = new char [ 50 * 1024 * 1024 ];

  output.reserve(50*1024*1024);

  ios::sync_with_stdio(false);

  try {
    init_with_policies<Policies>(argc, argv);

    n = get_i32("n");

    while (!cin.eof()) {

      if (cin.fail()) {
	cerr << "istream failure (line " << lineno << ")" << endl;
	cin.clear();
      }

      cin.getline(line_buffer, 50*1024*1024);
      lineno++;

      if (*line_buffer == '#')
	continue;

      if ((sequence = get_field(line_buffer, 2, &end)) == 0) {
	cerr << "invalid format on line " << lineno << endl;
	continue;
      }

      arena.free();
      ngrams.clear();

      length = (end) ? end - sequence : strlen(sequence);

      if (length >= n) {
	ptr = strchr(line_buffer, '\t');
	if (ptr == 0)
	  continue;
	*ptr = 0;

	// Setup buffer
	output.ptr = output.base + n;
	strcpy((char *)output.ptr, "\tSequence:");
	output.ptr += strlen((const char *)output.ptr);
	strcpy((char *)output.ptr, line_buffer);
	output.ptr += strlen((const char *)output.ptr);
	*output.ptr++ = '\t';
	*output.ptr = 0;

	for (size_t i=0; i<=length-n; i++) {
	  if (!strncmp(&sequence[i], bad_ngram, n))
	    continue;
	  sprintf(buf, "%u", (unsigned)i);
	  if ((iter = ngrams.find(&sequence[i])) == ngrams.end())
	    ngrams[&sequence[i]] = arena.dup(buf);
	  else {
	    value = arena.alloc(strlen(buf) + strlen((*iter).second) + 2);
	    strcpy(value, (*iter).second);
	    strcat(value, ",");
	    strcat(value, buf);
	    ngrams[&sequence[i]] = value;
	  }
	}

	for (iter = ngrams.begin(); iter != ngrams.end(); ++iter) {
	  memcpy(output.base, (*iter).first, n);
	  strcpy((char *)output.ptr, (*iter).second);
	  cout << (const char *)output.base << "\n";
	}
      }
    }
    cout << flush;

  }
  catch (std::exception &e) {
    cerr << "Error - " << e.what() << endl;
    exit(1);
  }
}
