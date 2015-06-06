/*
 * Copyright (C) 2007-2015 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 3
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

#include <Common/Compat.h>
#include <Common/Serializable.h>
#include <Common/Serialization.h>
#include <Common/Logger.h>

#include <bitset>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>

using namespace Hypertable;
using namespace std;

namespace {

  class Test1 : public Serializable {
  public:

    Test1() { }

    Test1(int32_t num, const string &str, bool bval) {
      set_num(num);
      set_str(str);
      set_bval(bval);
    }

    void set_generation(uint32_t generation) { m_generation = generation; }

    bool isset_num() const { return m_isset.test(0); }
    bool isset_str() const { return m_isset.test(1); }
    bool isset_bval() const { return m_isset.test(2); }

    int32_t get_num() const { return m_num; }
    const char *get_str() const { return m_str.c_str(); }
    bool get_bval() const { return m_bval; }

  private:

    void set_num(int32_t num) {
      m_num = num;
      m_isset.set(0);
    }

    void set_str(const string &str) {
      m_str = str;
      m_isset.set(1);
    }

    void set_bval(bool bval) {
      m_bval = bval;
      m_isset.set(2);
    }

    uint8_t encoding_version() const override { return 1; }

    size_t encoded_length_internal() const override {
      size_t length = 4;
      if (m_generation > 0) {
        length += Serialization::encoded_length_vstr(m_str);
        if (m_generation > 1)
          length += 1;
      }
      return length;
    }

    void encode_internal(uint8_t **bufp) const override {
      Serialization::encode_i32(bufp, m_num);
      if (m_generation > 0) {
        Serialization::encode_vstr(bufp, m_str);
        if (m_generation > 1)
          Serialization::encode_bool(bufp, m_bval);
      }
    }

    void decode_internal(uint8_t version, const uint8_t **bufp,
                         size_t *remainp) override {
      set_num(Serialization::decode_i32(bufp, remainp));
      if (m_generation < 1 || *remainp == 0)
        return;
      set_str(Serialization::decode_vstr(bufp, remainp));
      if (m_generation < 2 || *remainp == 0)
        return;
      set_bval(Serialization::decode_bool(bufp, remainp));
    }

    uint8_t m_generation {};
    bitset<3> m_isset;
    int32_t m_num {};
    string m_str;
    bool m_bval {};
  };

  std::ostream &operator<<(std::ostream &os, const Test1 &test1) {
    bool comma_needed {};
    if (test1.isset_num()) {
      os << "num=" << test1.get_num();
      comma_needed = true;
    }
    if (test1.isset_str()) {
      if (comma_needed)
        os << ", ";
      comma_needed = true;
      os << "str=" << test1.get_str();
    }
    if (test1.isset_bval()) {
      if (comma_needed)
        os << ", ";
      comma_needed = true;
      os << "bval=" << test1.get_bval();
    }
    return os;
  }

  class Test2 : public Serializable {
  public:

    Test2() { }

    Test2(Test1 obj, int64_t num64, const string &str2) {
      set_obj(obj);
      set_num64(num64);
      set_str2(str2);
    }

    void set_generation(uint32_t generation) { m_generation = generation; }

    bool isset_obj() const { return m_isset.test(0); }
    bool isset_num64() const { return m_isset.test(1); }
    bool isset_str2() const { return m_isset.test(2); }

    Test1 get_obj() const { return m_obj; }
    int64_t get_num64() const { return m_num64; }
    const char *get_str2() const { return m_str2.c_str(); }

    void set_obj(Test1 obj) {
      m_obj = obj;
      m_isset.set(0);
    }

    void set_num64(int64_t num64) {
      m_num64 = num64;
      m_isset.set(1);
    }

    void set_str2(const string &str2) {
      m_str2 = str2;
      m_isset.set(2);
    }

  private:

    uint8_t encoding_version() const override { return 1; }

    size_t encoded_length_internal() const override {
      size_t length = m_obj.encoded_length() + 8;
      if (m_generation > 0)
        length += Serialization::encoded_length_vstr(m_str2);
      return length;
    }

    void encode_internal(uint8_t **bufp) const override {
      m_obj.encode(bufp);
      Serialization::encode_i64(bufp, m_num64);
      if (m_generation > 0)
        Serialization::encode_vstr(bufp, m_str2);
    }

    void decode_internal(uint8_t version, const uint8_t **bufp,
                         size_t *remainp) override {
      m_obj.decode(bufp, remainp);
      set_num64(Serialization::decode_i64(bufp, remainp));
      if (m_generation < 1 || *remainp == 0)
        return;
      set_str2(Serialization::decode_vstr(bufp, remainp));
    }

    uint8_t m_generation {};
    bitset<3> m_isset;
    Test1 m_obj;
    int64_t m_num64 {};
    string m_str2;
  };

  std::ostream &operator<<(std::ostream &os, const Test2 &test2) {
    os << test2.get_obj();
    if (test2.isset_num64())
      os << ", num64=" << test2.get_num64();
    if (test2.isset_str2())
      os << ", str2=" << test2.get_str2();
    return os;
  }

  const char *usage_str =
    "\n"
    "usage:  Serializable_test [ <golden-file> ]\n"
    "\n"
    "When run without any arguments, this program runs the Serializable\n"
    "test and writes the output to the terminal.  If the <golden-file>\n"
    "argument is supplied, then the test output is written to a file and\n"
    "diff'ed against <golden-file>.  The program exit status is 0 if there\n"
    "are no diffs, 1 otherwise.\n";
}

int main(int argc, char **argv) {
  Test1 test1a(123, "joe", true);
  Test1 test1b;
  size_t length;
  shared_ptr<uint8_t> buffer;
  uint8_t *ptr;
  const uint8_t *cptr;
  string golden;
  ofstream fout;

  if (argc == 2) {
    if (!strcmp(argv[1], "--help")) {
      cout << usage_str << endl;
      quick_exit(EXIT_SUCCESS);
    }
    golden = argv[1];
  }
  else if (argc > 2) {
    cout << usage_str << endl;
    quick_exit(EXIT_FAILURE);
  }

  if (!golden.empty())
    fout.open("Serializable_test.output");

  ostream &out = golden.empty() ? cout : fout;

  for (uint32_t gena=0; gena<3; gena++) {
    test1a.set_generation(gena);
    length = test1a.encoded_length();
    buffer.reset(new uint8_t[length], [](uint8_t *p) {delete [] p;});
    ptr = buffer.get();
    test1a.encode(&ptr);
    for (uint32_t i=0; i<3; i++) {
      test1b = Test1();
      test1b.set_generation(i);
      cptr = buffer.get();
      length = test1a.encoded_length();
      test1b.decode(&cptr, &length);
      HT_ASSERT(length == 0);
      out << test1b << endl;
    }
  }

  /// Newest encoding
  test1a.set_generation(2);
  Test2 test2a(test1a, 123456789LL, "Palo Alto");
  Test2 test2b;

  test2a.set_generation(1);
  length = test2a.encoded_length();
  buffer.reset(new uint8_t[length], [](uint8_t *p) {delete [] p;});
  ptr = buffer.get();
  test2a.encode(&ptr);

  /// Newest decoder
  {
    test1b = Test1();
    test1b.set_generation(2);
    test2b = Test2();
    test2b.set_generation(1);
    test2b.set_obj(test1b);
    cptr = buffer.get();
    length = test2a.encoded_length();
    test2b.decode(&cptr, &length);
    HT_ASSERT(length == 0);
    out << test2b << endl;
  }

  /// Oldest decoder
  {
    test1b = Test1();
    test2b = Test2();
    test2b.set_obj(test1b);
    cptr = buffer.get();
    length = test2a.encoded_length();
    test2b.decode(&cptr, &length);
    HT_ASSERT(length == 0);
    out << test2b << endl;
  }

  /// Oldest encoding
  test1a.set_generation(0);
  test2a = Test2(test1a, 123456789LL, "Palo Alto");
  test2a.set_generation(0);

  length = test2a.encoded_length();
  buffer.reset(new uint8_t[length], [](uint8_t *p) {delete [] p;});
  ptr = buffer.get();
  test2a.encode(&ptr);

  /// Oldest decoder
  {
    test1b = Test1();
    test2b = Test2();
    test2b.set_obj(test1b);
    cptr = buffer.get();
    length = test2a.encoded_length();
    test2b.decode(&cptr, &length);
    HT_ASSERT(length == 0);
    out << test2b << endl;
  }

  /// Newest decoder
  {
    test1b = Test1();
    test1b.set_generation(2);
    test2b = Test2();
    test2b.set_generation(1);
    test2b.set_obj(test1b);
    cptr = buffer.get();
    length = test2a.encoded_length();
    test2b.decode(&cptr, &length);
    HT_ASSERT(length == 0);
    out << test2b << endl;
  }

  fout.close();

  if (!golden.empty()) {
    string cmd("diff Serializable_test.output ");
    cmd.append(golden);
    if (system(cmd.c_str()))
      quick_exit(EXIT_FAILURE);
  }
  
  quick_exit(EXIT_SUCCESS);
}
