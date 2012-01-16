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
#include "Common/md5.h"
#include "Common/TestUtils.h"

using namespace Hypertable;

int main(int ac, char *av[]) {
  char input[5][13] = { {"foobarfoobar"}, {"himnotpotchi"}, {"base64encode"}, {"ABCDEFGHIJKL"},
                        {"?kw??lr??Й?"}};
  char golden[5][17] = { {"Zm9vYmFyZm9vYmFy"}, {"aGltbm90cG90Y2hp"}, {"YmFzZTY0ZW5jb2Rl"},
                         {"QUJDREVGR0hJSktM"}, {"P2t3Pz9scj8_0Jk_"}};
  int64_t golden_hashes[5] = { 7891606660087872089LL, 241383655341626916LL,
                               -2486747253553136565LL, 7702431781597023531LL,
                               -8752389877473518803LL };

  char output[17];

  for (int ii=0; ii<5; ++ii) {
    digest_to_trunc_modified_base64(input[ii], output);

    if (strcmp(output, golden[ii])) {
      printf( "Test failed expected output %s got %s\n", golden[ii], output);
      exit(1);
    }
    else
      printf("Test passed expected output %s got %s\n", golden[ii], output);

    HT_ASSERT(md5_hash((const char *)input[ii]) == golden_hashes[ii]);
  }
  exit(0);

}
