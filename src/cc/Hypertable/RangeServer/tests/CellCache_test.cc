/*
 * Copyright (C) 2007-2015 Hypertable, Inc.
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
#include <Common/Init.h>
#include <Common/Stopwatch.h>
#include <Common/SystemInfo.h>
#include <Common/md5.h>

#include <Hypertable/Lib/Key.h>

#include "Hypertable/RangeServer/CellCache.h"
#include "Hypertable/RangeServer/CellCacheScanner.h"
#include "Hypertable/RangeServer/MemoryTracker.h"
#include "Hypertable/RangeServer/Global.h"

using namespace Hypertable;
using namespace Config;
using namespace std;

namespace {

struct MyPolicy : Config::Policy {
  static void init_options() {
    cmdline_desc("Usage: %s [Options] [<num_items>]\nOptions").add_options()
      ("repeats,r", i32()->default_value(10), "number of repeats for tests")
      ("length", i16()->default_value(12), "length of test row keys")
      ;
    cmdline_hidden_desc().add_options()
      ("items,n", i32()->default_value(250*K), "number of items")
      ;
    cmdline_positional_desc().add("items", -1);
  }
};

typedef Cons<MyPolicy, DefaultPolicy> AppPolicy;

#define LOG(_label_, _r_) \
  cout << _label_ << ": " << setprecision(3) << fixed << _r_ << "/s" << endl;

#define MEASURE(_label_, _code_, _n_, _out_) do { \
  Stopwatch w; _code_; w.stop(); \
  _out_ = (_n_) / w.elapsed(); \
  LOG(_label_, _out_) \
} while (0)

struct CellCacheTest {
  CellCacheTest(size_t n, size_t len)
    : nitems(n) {
    sprintf(fmt, "%%-%ds", (int)len);
    keys.reserve(nitems * len);
    double rate;
    MEASURE("generated keys",
      for (size_t i = 0; i < nitems; ++i)
       generate_key(i, len), nitems, rate);

    search_keys.reserve(nitems);
    MEASURE("generated search keys",
      Key key;
      const uint8_t* ptr = keys.base;
      for (size_t i = 0; i < nitems; ++i) {
         search_keys.push_back(ptr);
         key.load(ptr);
         ptr += key.length;
      }
      for (size_t i = 0; i < nitems; ++i) {
        size_t a = rand() % nitems;
        size_t b = rand() % nitems;
        if (a != b) {
          const uint8_t* ptr_a = search_keys[a];
          search_keys[a] = search_keys[b];
          search_keys[b] = ptr_a;
        }
      }, nitems, rate);

    cout << endl;
  }

  void run() {
#ifdef _WIN32
    SetThreadPriority(GetCurrentThread(), THREAD_PRIORITY_HIGHEST);
    SetThreadAffinityMask(GetCurrentThread(), 2);
#endif
    double t1 = 0, t2 = 0, t3 = 0;
    const ByteString none;
    Key key;
    int repeats = get_i32("repeats");
    for (int r = 0; r < repeats; ++r) {
      CellCachePtr cell_cache = new CellCache();
      double rate;
      MEASURE("insert keys",
        const uint8_t* ptr = keys.base;
        for (size_t i = 0; i < nitems; ++i) {
          key.load(ptr);
          ptr += key.length;
          cell_cache->add(key, none);
        }, nitems, rate);
      t1 += rate;

      ScanContextPtr scan_ctx = new ScanContext();
      DynamicBuffer s, e;
      create_key_and_append(s, "");
      create_key_and_append(e, Key::END_ROW_MARKER);
      scan_ctx->start_serkey.ptr = s.base;
      scan_ctx->end_serkey.ptr = e.base;
      CellListScannerPtr scanner = cell_cache->create_scanner(scan_ctx);
      Key key;
      ByteString value;

      int cells = 0;
      MEASURE("sequential scan",
        while (scanner->get(key, value)) {
          scanner->forward();
          ++cells;
        }, cells, rate);
      HT_ASSERT(nitems == cells);
      t2 += rate;

      cells = 0;
      MEASURE("single key lookup",
        for (size_t i = 0; i < nitems; ++i) {
          scan_ctx->start_serkey.ptr = search_keys[i];
          scan_ctx->end_serkey.ptr = scan_ctx->start_serkey.ptr;
          scanner = cell_cache->create_scanner(scan_ctx);
          if (scanner->get(key, value))
            ++cells;
        }, cells, rate);
      HT_ASSERT(nitems == cells);
      t3 += rate;

      cout << endl;
    }

    LOG("insert keys avg   ", t1 / repeats);
    LOG("sequential scan   ", t2 / repeats);
    LOG("single lookup avg ", t3 / repeats);
  }

  void generate_key(size_t i, size_t len) {
    char buf[33];
    md5_hex(&i, sizeof(i), buf);
    String tmp = format(fmt, buf);
    create_key_and_append(keys, String(tmp.data(), len).c_str());
  }

  DynamicBuffer keys;
  vector<const uint8_t*> search_keys;
  size_t nitems;
  char fmt[33];
};

} // local namespace

int main(int ac, char *av[]) {
  try {
    init_with_policy<AppPolicy>(ac, av);
    Global::memory_tracker = new MemoryTracker(0, 0);
    Global::cell_cache_scanner_cache_size = 1024; // default value
    CellCacheTest cell_cash_test(get_i32("items"), get_i16("length"));

    cell_cash_test.run();
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    return 1;
  }
}
