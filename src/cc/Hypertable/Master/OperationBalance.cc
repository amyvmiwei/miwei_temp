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

#include <Common/Compat.h>

#include "LoadBalancer.h"
#include "OperationBalance.h"
#include "OperationProcessor.h"
#include "Utility.h"
#include "BalancePlanAuthority.h"

#include <Hypertable/Lib/Key.h>
#include <Hypertable/Lib/LegacyDecoder.h>
#include <Hypertable/Lib/TableScanner.h>

#include <AsyncComm/ResponseCallback.h>

#include <Common/Error.h>
#include <Common/FailureInducer.h>
#include <Common/ScopeGuard.h>
#include <Common/Serialization.h>
#include <Common/StringExt.h>
#include <Common/System.h>
#include <Common/md5.h>

#include <chrono>
#include <sstream>
#include <thread>

using namespace Hypertable;
using namespace Hyperspace;
using namespace std;

OperationBalance::OperationBalance(ContextPtr &context)
  : Operation(context, MetaLog::EntityType::OPERATION_BALANCE) {
  initialize_dependencies();
  m_plan = make_shared<BalancePlan>();
}

OperationBalance::OperationBalance(ContextPtr &context,
                                   const MetaLog::EntityHeader &header_)
  : Operation(context, header_) {
  m_hash_code = md5_hash("OperationBalance");
}

OperationBalance::OperationBalance(ContextPtr &context, EventPtr &event)
  : Operation(context, event, MetaLog::EntityType::OPERATION_BALANCE) {
  initialize_dependencies();
  const uint8_t *ptr = event->payload;
  size_t remaining = event->payload_len;
  m_params.decode(&ptr, &remaining);
  m_plan = make_shared<BalancePlan>(m_params.plan());
}

void OperationBalance::initialize_dependencies() {
  m_hash_code = md5_hash("OperationBalance");
  m_dependencies.clear();
  m_dependencies.insert(Dependency::INIT);
  m_dependencies.insert(Dependency::SERVERS);
  m_dependencies.insert(Dependency::ROOT);
  m_dependencies.insert(Dependency::METADATA);
  m_dependencies.insert(Dependency::SYSTEM);
  m_dependencies.insert(Dependency::RECOVER_SERVER);
}


void OperationBalance::execute() {
  int32_t state = get_state();

  HT_INFOF("Entering Balance-%lld algorithm= %s state=%s",
           (Lld)header.id, m_plan->algorithm.c_str(),
           OperationState::get_text(state));

  switch (state) {

  case OperationState::INITIAL:

    try {
      std::vector<RangeServerConnectionPtr> balanced;
      std::vector<MetaLog::EntityPtr> entities;
      int generation = m_context->get_balance_plan_authority()->get_generation();

      if (m_plan->empty())
        m_context->balancer->create_plan(m_plan, balanced);

      set_state(OperationState::STARTED);
      entities.push_back(shared_from_this());
      for (auto & rsc : balanced) {
        rsc->set_balanced();
        entities.push_back(rsc);
      }

      if (!m_context->get_balance_plan_authority()->register_balance_plan(m_plan, generation, entities))
        return;

      // Un-pause the balancer
      m_context->balancer->unpause();
    }
    catch (Exception &e) {
      complete_error(e);
      HT_ERROR_OUT << e << HT_END;
      break;
    }

  case OperationState::STARTED:
    {
      RangeServer::Client rsc(m_context->comm);
      CommAddress addr;
      MetaLog::EntityPtr bpa_entity;

      if (!m_plan->moves.empty()) {
        uint32_t wait_millis = m_plan->duration_millis / m_plan->moves.size();

        for (auto &move : m_plan->moves) {
          addr.set_proxy(move->source_location);
          try {
            rsc.relinquish_range(addr, move->table, move->range);
            this_thread::sleep_for(chrono::milliseconds(wait_millis));
          }
          catch (Exception &e) {
            move->complete = true;
            move->error = e.code();
            if (!bpa_entity)
              m_context->get_balance_plan_authority(bpa_entity);
            static_pointer_cast<BalancePlanAuthority>(bpa_entity)->balance_move_complete(move->table, move->range);
          }
        }
        std::stringstream sout;
        for (auto &move : m_plan->moves) {
          sout.str("");
          sout << *move;
          HT_INFOF("%s", sout.str().c_str());
        }
      }
      complete_ok(bpa_entity);
    }
    break;

  default:
    HT_FATALF("Unrecognized state %d", state);
  }

  HT_INFOF("Leaving Balance-%lld algorithm=%s", (Lld)header.id,
           m_plan->algorithm.c_str());
}

void OperationBalance::display_state(std::ostream &os) {
  os << *(m_plan.get());
}

const String OperationBalance::name() {
  return "OperationBalance";
}

const String OperationBalance::label() {
  if (m_plan)
    return format("Balance %s (%u moves)", m_plan->algorithm.c_str(),
                  (unsigned)m_plan->moves.size());
  return name();
}


uint8_t OperationBalance::encoding_version_state() const {
  return 1;
}

size_t OperationBalance::encoded_length_state() const {
  return m_params.encoded_length() + m_plan->encoded_length();
}

void OperationBalance::encode_state(uint8_t **bufp) const {
  m_params.encode(bufp);
  m_plan->encode(bufp);
}

void OperationBalance::decode_state(uint8_t version,
                                    const uint8_t **bufp,
                                    size_t *remainp) {
  m_params.decode(bufp, remainp);
  m_plan = make_shared<BalancePlan>();
  m_plan->decode(bufp, remainp);
}

void OperationBalance::decode_state_old(uint8_t version,
                                        const uint8_t **bufp,
                                        size_t *remainp) {
  m_plan = make_shared<BalancePlan>();
  legacy_decode(bufp, remainp, m_plan.get());
}
