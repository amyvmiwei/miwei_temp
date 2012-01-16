/** -*- c++ -*-
 * Copyright (C) 2007-2012 Hypertable, Inc.
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

#include "Common/Compat.h"
#include "Common/Error.h"

#include "OperationLoadBalancer.h"
#include "OperationBalance.h"
#include "LoadBalancer.h"

using namespace Hypertable;

OperationLoadBalancer::OperationLoadBalancer(ContextPtr &context)
  : Operation(context, MetaLog::EntityType::OPERATION_LOAD_BALANCER) {
  m_plan = new BalancePlan();
  initialize_dependencies();
}

OperationLoadBalancer::OperationLoadBalancer(ContextPtr &context, EventPtr &event)
  : Operation(context, event, MetaLog::EntityType::OPERATION_LOAD_BALANCER) {
  const uint8_t *ptr = event->payload;
  size_t remaining = event->payload_len;
  m_plan = new BalancePlan();
  decode_request(&ptr, &remaining);
  initialize_dependencies();
}

void OperationLoadBalancer::initialize_dependencies() {
  m_dependencies.insert(Dependency::INIT);
  m_dependencies.insert(Dependency::METADATA);
  m_dependencies.insert(Dependency::SYSTEM);
}

void OperationLoadBalancer::execute() {
  HT_INFOF("Entering LoadBalancer-%lld", (Lld)header.id);

  try {
    String algorithm = get_algorithm();
    if (!m_plan->moves.empty())
      m_context->op_balance->register_plan(m_plan);
    else if (algorithm.size() > 0)
      m_context->balancer->balance(algorithm);
    else
      m_context->balancer->balance();
  }
  catch (Exception &e) {
    HT_THROW2(e.code(), e, "Load Balancer");
  }
  complete_ok_no_log();
  HT_INFOF("Leaving LoadBalancer-%lld", (Lld)header.id);
}

const String OperationLoadBalancer::name() {
  return "OperationLoadBalancer";
}

const String OperationLoadBalancer::label() {
  return "LoadBalancer";
}

const String OperationLoadBalancer::get_algorithm() {
  return m_plan->algorithm;
}

void OperationLoadBalancer::decode_request(const uint8_t **bufp, size_t *remainp) {

  m_plan->decode(bufp, remainp);
}
