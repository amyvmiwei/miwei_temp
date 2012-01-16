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
#include "Common/Error.h"
#include "Common/Logger.h"
#include "Common/Init.h"

using namespace Hypertable;

namespace ExceptionTest {

void test_ex4(double) {
  HT_THROW(Error::EXTERNAL, "throw an external error");
}

void test_ex3(uint64_t x) {
  HT_TRY("testing ex4", test_ex4(x));
}

void test_ex2(uint32_t x) {
  HT_TRY("testing ex3", test_ex3(x));
}

void test_ex1(uint16_t x) {
  HT_TRY("testing ex2", test_ex2(x));
}

void test_ex0(uint8_t x) {
  try { test_ex1(x); }
  catch (Exception &e) {
    HT_THROW2(Error::PROTOCOL_ERROR, e, "testing ex1");
  }
}

} // namespace ExceptionTest

int main(int ac, char *av[]) {
  try {
    Config::init(ac, av);

    ExceptionTest::test_ex0(0);
  }
  catch (Exception &e) {
    HT_INFO_OUT <<"Error: "<< e.messages() << HT_END;
    HT_INFO_OUT <<"Exception trace: "<< e << HT_END;
  }
}
