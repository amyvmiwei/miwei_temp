/**
 * Copyright (C) 2007-2012 Hypertable, Inc.
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

#include "Common/Compat.h"
#include "Common/Escaper.h"
#include "Common/Init.h"
#include "Common/Logger.h"

extern "C" {
#include <string.h>
}

using namespace Hypertable;

void run_test(const String &original, const String &escape_chars) {
  String str = original;
  const char *base, *ptr;

  escape(str, escape_chars);

  for (size_t i=0; i<escape_chars.length(); i++) {
    base = str.c_str();
    ptr = base-1;
    while ((ptr = strchr(ptr+1, escape_chars[i])) != 0) {
      if (*(ptr-1) != '\\') {
        HT_FATALF("escape failure: error at position %lu of output(%s) escape_chars(%s)",
                  (unsigned long)(ptr-base), str.c_str(), escape_chars.c_str());
      }
    }
  }

  unescape(str);

  if (str != original)
    HT_FATALF("escape->unescape failure: input(%s) escape_chars(%s) output(%s)",
              original.c_str(), escape_chars.c_str(), str.c_str());
}

int main(int ac, char *av[]) {
  Config::init(ac, av);
  run_test("hello world", ",!");
  run_test("hello,world", ",!");
  run_test("hello,world", ",");
  run_test(",hello,world,", ",");
  run_test("\\\\\\\t", ",");
  run_test("\\\\", ",");
  run_test("'hello,world'", "'");
  run_test("'hello,\\world'", "'");
  _exit(0);
}
