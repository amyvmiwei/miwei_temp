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
#include "Common/ScopeGuard.h"
#include "Common/Logger.h"

#include <stdio.h>

using namespace Hypertable;

namespace {

const size_t EXPECTED_HITS = 7;
size_t hits = 0;

void fun0() { puts(HT_FUNC); ++hits; }
void fun1(int) { puts(HT_FUNC); ++hits; }
void fun2(int, int) { puts(HT_FUNC); ++hits; }
void fun3(int, int, int) { puts(HT_FUNC); ++hits; }

struct Obj {
  void meth0() { puts(HT_FUNC); ++hits; }
  void meth1(int) { puts(HT_FUNC); ++hits; }
  void meth2(int, int) { puts(HT_FUNC); ++hits; }
};

void test_fun0() { HT_ON_SCOPE_EXIT(&fun0); }
void test_fun1() { HT_ON_SCOPE_EXIT(&fun1, 1); }
void test_fun2() { HT_ON_SCOPE_EXIT(&fun2, 1, 2); }
void test_fun3() { HT_ON_SCOPE_EXIT(&fun3, 1, 2, 3); }

void test_meth0(Obj &obj) { HT_ON_OBJ_SCOPE_EXIT(obj, &Obj::meth0); }
void test_meth1(Obj &obj) { HT_ON_OBJ_SCOPE_EXIT(obj, &Obj::meth1, 1); }
void test_meth2(Obj &obj) { HT_ON_OBJ_SCOPE_EXIT(obj, &Obj::meth2, 1, 2); }

inline void incr(int &i) { ++i; }

void test_by_ref() {
#if defined(__GNUC__) && __GNUC__ == 4 && __GNUC_MINOR__ == 2
  // http://gcc.gnu.org/bugzilla/show_bug.cgi?id=31947
#else
  int i = 0;
  { HT_ON_SCOPE_EXIT(&incr, by_ref(i)); }
  HT_ASSERT(i == 1);
#endif
}

} // local namespace

int main() {
  test_fun0();
  test_fun1();
  test_fun2();
  test_fun3();

  Obj obj;
  test_meth0(obj);
  test_meth1(obj);
  test_meth2(obj);

  HT_ASSERT(hits == EXPECTED_HITS);

  test_by_ref();

  return 0;
}
