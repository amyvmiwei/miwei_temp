/*
 * Copyright (C) 2007-2015 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 3 of the
 * License, or any later version.
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
 * Definitions for ServerState.
 * This file contains the definitions for ServerState, a class for holding
 * and providing access to dynamic server state.
 */

#include <Common/Compat.h>

#include "ServerState.h"

#include <Common/Logger.h>

using namespace Hypertable;
using namespace std;

ServerState::ServerState() {
  SystemVariable::Spec spec;
  for (int i=0; i<SystemVariable::COUNT; i++) {
    spec.code = i;
    spec.value = SystemVariable::default_value(i);
    m_specs.push_back(spec);
  }
}

bool ServerState::readonly() {
  return m_specs[SystemVariable::READONLY].value;
}

void ServerState::set(int64_t generation, const std::vector<SystemVariable::Spec> &specs) {
  lock_guard<mutex> lock(m_mutex);
  if (generation > m_generation) {
    for (auto &spec : specs) {
      if (spec.code < (int)m_specs.size())
        m_specs[spec.code].value = spec.value;
      else
        HT_WARNF("Attempt to set unknown server state variable %d to %s",
                 spec.code, spec.value ? "true" : "false");
    }
    m_generation = generation;
  }
}
