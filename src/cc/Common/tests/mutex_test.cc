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
#include "Common/Mutex.h"
#include "Common/atomic.h"
#include "Common/TestUtils.h"
#include "Common/Init.h"

using namespace Hypertable;
using namespace Hypertable::Config;
using namespace boost;

namespace {

volatile int g_vcount = 0;
int g_count = 0;
Mutex g_mutex;
RecMutex g_recmutex;
atomic_t g_av;

void test_loop(int n) {
  HT_BENCH(Hypertable::format("%d: loop", g_vcount), ++g_vcount, n);
}

void test_atomic(int n) {
  HT_BENCH1(Hypertable::format("%d: atomic", g_count), for (int i = n; i--;)
    atomic_inc(&g_av); g_count = atomic_read(&g_av), n);
}

void test_mutex(int n) {
  HT_BENCH(Hypertable::format("%d: mutex", g_count),
    ScopedLock lock(g_mutex); ++g_count, n);
}

void test_recmutex(int n) {
  HT_BENCH(Hypertable::format("%d: recmutex", g_count),
    ScopedRecLock lock(g_recmutex); ++g_count, n);
}

} // local namespace

int main(int ac, char *av[]) {
  init_with_policy<DefaultTestPolicy>(ac, av);
  int n = get_i32("num-items");

  run_test(bind(test_loop, n));
  run_test(bind(test_atomic, n));
  run_test(bind(test_mutex, n));
  run_test(bind(test_recmutex, n));
}
