/** -*- c++ -*-
 * Copyright (C) 2011 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
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

#include "Common/Compat.h"
#include "Common/Error.h"

#include "OperationLoadBalancer.h"
#include "LoadBalancer.h"

using namespace Hypertable;

OperationLoadBalancer::OperationLoadBalancer(ContextPtr &context, const String &algorithm)
  : Operation(context, MetaLog::EntityType::OPERATION_LOAD_BALANCER), m_algorithm(algorithm) {
  m_dependencies.insert(Dependency::INIT);
  m_dependencies.insert(Dependency::METADATA);
  m_dependencies.insert(Dependency::SYSTEM);
}


void OperationLoadBalancer::execute() {
  HT_INFOF("Entering LoadBalancer-%lld algorithm=%s", (Lld)header.id, m_algorithm.c_str());

  try {
    m_context->balancer->balance(m_algorithm);
  }
  catch (Exception &e) {
    HT_THROW2(e.code(), e, "Load Balancer");
  }
  complete_ok_no_log();
  HT_INFOF("Leaving LoadBalancer-%lld algorithm=%s", (Lld)header.id, m_algorithm.c_str());
}

const String OperationLoadBalancer::name() {
  return "OperationLoadBalancer";
}

const String OperationLoadBalancer::label() {
  return "LoadBalancer";
}

