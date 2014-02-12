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
#include "Common/InetAddr.h"
#include "Common/Logger.h"
#include "Common/Stopwatch.h"
#include "Common/Init.h"

#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>

using namespace Hypertable;

#define MEASURE(_code_, _n_) do { \
  InetAddr addr; \
  addr.sin_port = htons(15865); \
  Stopwatch w; \
  for (int i = _n_; i--; ) { _code_; } \
  w.stop(); \
  std::cout << addr.hex() <<": "<< (_n_) / w.elapsed() <<"/s"<< std::endl; \
} while (0)

namespace {

void test_localhost() {
  InetAddr addr("localhost", 8888);
  uint32_t expected_localhost_addr = (127 << 24) + 1;
  HT_ASSERT(addr.sin_addr.s_addr == htonl(expected_localhost_addr));
}

bool test_parse_ipv4(const char *ipstr) {
  InetAddr addr;
  if (InetAddr::parse_ipv4(ipstr, 0, addr)) {
    struct hostent *he = gethostbyname(ipstr);
    HT_ASSERT(he && *(uint32_t *)he->h_addr_list[0] == addr.sin_addr.s_addr);
    return true;
  }
  return false;
}

bool test_is_ipv4(const char *ipstr) {
  return InetAddr::is_ipv4(ipstr);
}

void bench_loop(uint32_t n, sockaddr_in &addr) {
  addr.sin_addr.s_addr = htonl(n);
}

void bench_parse_ipv4(const char *ipstr, sockaddr_in &addr, int base = 0) {
  InetAddr::parse_ipv4(ipstr, 15865, addr, base);
}

void bench_gethostbyname(const char *ipstr, sockaddr_in &addr) {
  struct hostent *he = gethostbyname(ipstr);
  addr.sin_addr.s_addr = *(uint32_t *)he->h_addr_list[0];
  addr.sin_family = AF_INET;
}

} // local namespace

int main(int argc, char *argv[]) {
  Config::init(argc, argv);

  test_localhost();
  HT_ASSERT(test_parse_ipv4("10.8.238.1"));
  HT_ASSERT(test_parse_ipv4("1491994634"));
  //HT_ASSERT(test_parse_ipv4("0xa08ee01")); doesn't work on linux
  HT_ASSERT(test_parse_ipv4("127.0.0.1"));
  HT_ASSERT(test_parse_ipv4("127.0.0.1_") == false);

  HT_ASSERT(test_is_ipv4("10.8.238.1"));
  HT_ASSERT(test_is_ipv4("192.168.238.100"));
  HT_ASSERT(test_is_ipv4("1.2.3.4"));
  HT_ASSERT(test_is_ipv4("1491.8.1.3") == false);
  HT_ASSERT(test_is_ipv4("149.8.1") == false);
  HT_ASSERT(test_is_ipv4("149.8.1.2.3") == false);
  HT_ASSERT(test_is_ipv4("700.8.1.2") == false);
  HT_ASSERT(test_is_ipv4("12.foo.bar.com") == false);
  HT_ASSERT(test_is_ipv4("127.0.0.1_") == false);
  HT_ASSERT(test_is_ipv4("foo.bar.com.baz") == false);

  if (argc > 1) {
    size_t n = atoi(argv[1]);
    MEASURE(bench_loop(0xa08ee58, addr), n);
    MEASURE(bench_parse_ipv4("10.8.238.88", addr), n);
    MEASURE(bench_parse_ipv4("168357464", addr), n);
    MEASURE(bench_parse_ipv4("0xa08ee58", addr), n);
    MEASURE(bench_parse_ipv4("a08ee58", addr, 16), n);
    MEASURE(bench_gethostbyname("10.8.238.88", addr), n);
  }
  return 0;
}
