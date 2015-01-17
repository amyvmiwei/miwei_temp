/*
 * Copyright (C) 2007-2013 Hypertable, Inc.
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

/** @file
 * Definitions for SystemVariable.
 * This file contains the definitions for SystemVariable which includes
 * conversion functions for representing system variables.
 */

#include <Common/Compat.h>
#include "SystemVariable.h"

#include <Common/Logger.h>
#include <Common/Serialization.h>
#include <Common/String.h>
#include <Common/StringExt.h>

#include <unordered_map>

using namespace Hypertable;

namespace {

  struct VariableInfo {
    int          code;
    const char  *text;
    bool         default_value;
  };

  VariableInfo variable_info[] = {
    { SystemVariable::READONLY, "READONLY", false },
    { 0, 0, false }
  };

  typedef std::unordered_map<int, const char *> CodeToStringMap;

  CodeToStringMap &build_code_to_string_map() {
    CodeToStringMap *map = new CodeToStringMap();
    for (int i=0; variable_info[i].text != 0; i++)
      (*map)[variable_info[i].code] = variable_info[i].text;
    HT_ASSERT(map->size() == SystemVariable::COUNT);
    return *map;
  }

  CodeToStringMap &code_to_string_map = build_code_to_string_map();

  typedef std::unordered_map<String, int> StringToCodeMap;

  StringToCodeMap &build_string_to_code_map() {
    StringToCodeMap *map = new StringToCodeMap();
    for (int i=0; variable_info[i].text != 0; i++)
      (*map)[variable_info[i].text] = variable_info[i].code;
    HT_ASSERT(map->size() == SystemVariable::COUNT);
    return *map;
  }

  StringToCodeMap &string_to_code_map = build_string_to_code_map();

  std::vector<bool> build_default_value_vector() {
    std::vector<bool> vec;
    for (int i=0; variable_info[i].text != 0; i++)
      vec.push_back(variable_info[i].default_value);
    HT_ASSERT(vec.size() == SystemVariable::COUNT);
    return vec;
  }

  std::vector<bool> defaults = build_default_value_vector();

} // local namespace


const char *SystemVariable::code_to_string(int var_code) {
  const char *text = code_to_string_map[var_code];
  HT_ASSERT(text);
  return text;
}

int SystemVariable::string_to_code(const String &var_string) {
  if (string_to_code_map.find(var_string) == string_to_code_map.end())
    return -1;
  return string_to_code_map[var_string];
}

bool SystemVariable::default_value(int var_code) {
  HT_ASSERT(var_code < (int)defaults.size());
  return defaults[var_code];
}

String SystemVariable::specs_to_string(const std::vector<Spec> &specs) {
  bool first = true;
  String str;
  for (auto &spec : specs) {
    if (!first)
      str += ",";
    if (spec.code < SystemVariable::COUNT)
      str += code_to_string_map[spec.code];
    else
      str += String("") + spec.code;
    str += String("=") + (spec.value ? "true" : "false");
    first = false;
  }
  return str;
}


uint8_t SystemVariable::Spec::encoding_version() const {
  return 1;
}

size_t SystemVariable::Spec::encoded_length_internal() const {
  return 5;
}

/// @details
/// Encoding is as follows:
/// <table>
/// <tr>
/// <th>Encoding</th>
/// <th>Description</th>
/// </tr>
/// <tr>
/// <td>i32</td>
/// <td>Variable code</td>
/// </tr>
/// <tr>
/// <td>bool</td>
/// <td>Variable value</td>
/// </tr>
/// </table>
void SystemVariable::Spec::encode_internal(uint8_t **bufp) const {
  Serialization::encode_i32(bufp, code);
  Serialization::encode_bool(bufp, value);
}

void SystemVariable::Spec::decode_internal(uint8_t version, const uint8_t **bufp,
                                           size_t *remainp) {
  code = Serialization::decode_i32(bufp, remainp);
  value = Serialization::decode_bool(bufp, remainp);
}



size_t SystemVariable::encoded_length_specs(const std::vector<Spec> &specs) {
  return 4 + (5 * specs.size());
}

void SystemVariable::encode_specs(const std::vector<Spec> &specs, uint8_t **bufp) {
  Serialization::encode_i32(bufp, specs.size());
  for (auto &spec : specs) {
    Serialization::encode_i32(bufp, spec.code);
    Serialization::encode_bool(bufp, spec.value);
  }
}

void SystemVariable::decode_specs(std::vector<Spec> &specs,
                                  const uint8_t **bufp, size_t *remainp) {
  Spec spec;
  int count = Serialization::decode_i32(bufp, remainp);
  for (int i=0; i<count; i++) {
    spec.code = Serialization::decode_i32(bufp, remainp);
    spec.value = Serialization::decode_bool(bufp, remainp);
    specs.push_back(spec);
  }
}
