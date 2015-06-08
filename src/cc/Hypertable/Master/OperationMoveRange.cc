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

#include "OperationMoveRange.h"
#include "OperationProcessor.h"
#include "Utility.h"
#include "BalancePlanAuthority.h"

#include <Hypertable/Lib/Key.h>
#include <Hypertable/Lib/LegacyDecoder.h>

#include <AsyncComm/ResponseCallback.h>

#include <Common/Error.h>
#include <Common/FailureInducer.h>
#include <Common/ScopeGuard.h>
#include <Common/Serialization.h>
#include <Common/System.h>
#include <Common/md5.h>

#include <chrono>
#include <string>
#include <thread>

using namespace Hypertable;
using namespace Hyperspace;
using namespace std;

OperationMoveRange::
OperationMoveRange(ContextPtr &context,const string &source, int64_t range_id,
                   const TableIdentifier &table, const RangeSpec &range,
                   const string &transfer_log, int64_t soft_limit,bool is_split)
  : Operation(context, MetaLog::EntityType::OPERATION_MOVE_RANGE),
    m_params(source, range_id, table, range, transfer_log, soft_limit, is_split) {
  m_range_name = format("%s[%s..%s]", table.id, range.start_row,
                        range.end_row);
  initialize_dependencies();
  m_hash_code = hash_code(table, range, source, range_id);
  set_remove_approval_mask(0x03);
}

OperationMoveRange::OperationMoveRange(ContextPtr &context,
                                       const MetaLog::EntityHeader &header_)
  : Operation(context, header_) {
}

OperationMoveRange::OperationMoveRange(ContextPtr &context, EventPtr &event)
  : Operation(context, event, MetaLog::EntityType::OPERATION_MOVE_RANGE) {
  const uint8_t *ptr = event->payload;
  size_t remaining = event->payload_len;
  m_params.decode(&ptr, &remaining);
  m_range_name = format("%s[%s..%s]", m_params.table().id,
                        m_params.range_spec().start_row,
          m_params.range_spec().end_row);
  m_hash_code = hash_code(m_params.table(), m_params.range_spec(),
                          m_params.source(), m_params.range_id());
  set_remove_approval_mask(0x03);
  initialize_dependencies();
}

void OperationMoveRange::initialize_dependencies() {
  m_exclusivities.insert(Utility::range_hash_string(m_params.table(),
                                                    m_params.range_spec(),
              "OperationMoveRange"));

  m_dependencies.insert(Dependency::INIT);
  m_dependencies.insert(Dependency::SERVERS);
  m_dependencies.insert(Utility::range_hash_string(m_params.table(),
                                                   m_params.range_spec(), ""));

  if (!strcmp(m_params.table().id, TableIdentifier::METADATA_ID)) {
    m_dependencies.insert(Dependency::ROOT);
  }
  else if (!strncmp(m_params.table().id, "0/", 2)) {
    m_dependencies.insert(Dependency::ROOT);
    m_dependencies.insert(Dependency::METADATA);
  }
  else {
    m_dependencies.insert(Dependency::ROOT);
    m_dependencies.insert(Dependency::METADATA);
    m_dependencies.insert(Dependency::SYSTEM);
  }
  m_obstructions.insert(string("OperationMove ") + m_range_name);
  m_obstructions.insert(string(m_params.table().id) + " move range");
}

void OperationMoveRange::execute() {
  int32_t state = get_state();
  string old_destination = m_destination;
  MetaLog::EntityPtr bpa_entity;
  m_context->get_balance_plan_authority(bpa_entity);
  BalancePlanAuthorityPtr bpa
    = static_pointer_cast<BalancePlanAuthority>(bpa_entity);

  HT_INFOF("Entering MoveRange-%lld %s (id=%lld) state=%s",
           (Lld)header.id, m_range_name.c_str(), (Lld)m_params.range_id(),
           OperationState::get_text(state));

  switch (state) {

  case OperationState::INITIAL:

    if (!bpa->get_balance_destination(m_params.table(), m_params.range_spec(),
                                      m_destination)) {
      m_context->op->unblock(Dependency::SERVERS);
      return;
    }

    // if the destination changed (i.e. because the old destination is
    // recovered) then update the metalog!
    HT_INFOF("MoveRange %s: destination is %s (previous: %s)",
            m_range_name.c_str(), m_destination.c_str(),
            old_destination.c_str());
    set_state(OperationState::STARTED);
    HT_MAYBE_FAIL("move-range-INITIAL-a");
    m_context->mml_writer->record_state(shared_from_this());
    HT_MAYBE_FAIL("move-range-INITIAL-b");
    return;

  case OperationState::STARTED:
    if (m_event) {
      try {
        ResponseCallback cb(m_context->comm, m_event);
        cb.response_ok();
      }
      catch (Exception &e) {
        HT_WARN_OUT << e << HT_END;
      }
      m_event = 0;
    }
    set_state(OperationState::LOAD_RANGE);
    HT_MAYBE_FAIL("move-range-STARTED");

  case OperationState::LOAD_RANGE:
    try {
      RangeServer::Client rsc(m_context->comm);
      CommAddress addr;
      RangeState range_state;

      addr.set_proxy(m_destination);
      range_state.soft_limit = m_params.soft_limit();
      range_state.transfer_log = m_params.transfer_log().c_str();
      if (m_context->test_mode)
        HT_WARNF("Skipping %s::load_range() because in TEST MODE",
                m_destination.c_str());
      else
        rsc.load_range(addr, m_params.table(), m_params.range_spec(),
                       range_state, !m_params.is_split());
    }
    catch (Exception &e) {
      if (e.code() != Error::RANGESERVER_RANGE_ALREADY_LOADED) {

        // If not yet relinquished, wait a couple of seconds and try again
        if (e.code() == Error::RANGESERVER_RANGE_NOT_YET_RELINQUISHED) {
          HT_INFOF("%s - %s", Error::get_text(e.code()), e.what());
          this_thread::sleep_for(chrono::milliseconds(2000));
          return;
        }

        if (!Utility::table_exists(m_context, m_params.table().id)) {
          HT_WARNF("Aborting MoveRange %s because table no longer exists",
                   m_range_name.c_str());
          bpa->balance_move_complete(m_params.table(), m_params.range_spec());
          remove_approval_add(0x03);
          complete_ok(bpa_entity);
          OperationPtr operation
            = dynamic_pointer_cast<Operation>(shared_from_this());
          m_context->remove_move_operation(operation);
          return;
        }

        // server might be down - go back to the initial state and pick a
        // new destination
        HT_WARNF("Problem moving range %s to %s: %s - %s",
                 m_range_name.c_str(), m_destination.c_str(),
                 Error::get_text(e.code()), e.what());
        this_thread::sleep_for(chrono::milliseconds(5000));
        set_state(OperationState::INITIAL);
        return;
      }
    }
    HT_MAYBE_FAIL("move-range-LOAD_RANGE");
    set_state(OperationState::ACKNOWLEDGE);
    m_context->mml_writer->record_state(shared_from_this());

  case OperationState::ACKNOWLEDGE:
    {
      RangeServer::Client rsc(m_context->comm);
      CommAddress addr;

      addr.set_proxy(m_destination);
      if (m_context->test_mode) {
        HT_WARNF("Skipping %s::acknowledge_load() because in TEST MODE",
                m_destination.c_str());
      }
      else {
        try {
          QualifiedRangeSpec qrs(m_params.table(), m_params.range_spec());
          vector<QualifiedRangeSpec *> range_vec;
          map<QualifiedRangeSpec, int> response_map;
          range_vec.push_back(&qrs);
          rsc.acknowledge_load(addr, range_vec, response_map);
          map<QualifiedRangeSpec, int>::iterator it = response_map.begin();
          if (it->second != Error::OK &&
              it->second != Error::TABLE_NOT_FOUND &&
              it->second != Error::RANGESERVER_RANGE_NOT_FOUND)
            HT_THROWF(it->second, "Problem acknowledging load range %s to %s",
                      m_range_name.c_str(), m_destination.c_str());
        }
        catch (Exception &e) {
          // Destination might be down - go back to the initial state
          HT_WARNF("Problem acknowledging load range %s: %s - %s (dest %s)",
                   m_range_name.c_str(), Error::get_text(e.code()),
                   e.what(), m_destination.c_str());
          this_thread::sleep_for(chrono::milliseconds(5000));
          // Fetch new destination, if it changed, and then try again
          if (!bpa->get_balance_destination(m_params.table(),
                                            m_params.range_spec(),
                                            m_destination))
            m_context->op->unblock(Dependency::SERVERS);
          return;
        }
      }
    }
    bpa->balance_move_complete(m_params.table(), m_params.range_spec());
    {
      bool remove_ok = remove_approval_add(0x02);
      complete_ok(bpa_entity);
      if (remove_ok) {
        OperationPtr operation
          = dynamic_pointer_cast<Operation>(shared_from_this());
        m_context->remove_move_operation(operation);
      }
    }
    break;

  case OperationState::COMPLETE:
    break;

  default:
    HT_FATALF("Unrecognized state %d", state);
  }

  HT_INFOF("Leaving MoveRange-%lld %s (id=%lld) -> %s (state=%s)",
           (Lld)header.id, m_range_name.c_str(), (Lld)m_params.range_id(),
           m_destination.c_str(), OperationState::get_text(m_state));
}

void OperationMoveRange::display_state(std::ostream &os) {
  os << " " << m_params.table() << " " << m_params.range_spec()
     << " transfer-log='" << m_params.transfer_log()
     << "' soft-limit=" << m_params.soft_limit() << " is_split="
     << (m_params.is_split() ? "true" : "false")
     << "source='" << m_params.source() << "' location='"
     << m_destination << "' ";
}

uint8_t OperationMoveRange::encoding_version_state() const {
  return 1;
}

size_t OperationMoveRange::encoded_length_state() const {
  return m_params.encoded_length() +
    Serialization::encoded_length_vstr(m_destination);
}

void OperationMoveRange::encode_state(uint8_t **bufp) const {
  m_params.encode(bufp);
  Serialization::encode_vstr(bufp, m_destination);
}

void OperationMoveRange::decode_state(uint8_t version, const uint8_t **bufp,
                                      size_t *remainp) {
  m_params.decode(bufp, remainp);
  m_destination = Serialization::decode_vstr(bufp, remainp);
  m_range_name
    = format("%s[%s..%s]", m_params.table().id, m_params.range_spec().start_row,
             m_params.range_spec().end_row);
  m_hash_code
    = Utility::range_hash_code(m_params.table(), m_params.range_spec(),
                               string("OperationMoveRange-")+m_params.source());
}

void OperationMoveRange::decode_state_old(uint8_t version, const uint8_t **bufp,
                                          size_t *remainp) {
  {
    string source;
    if (get_original_type() != MetaLog::EntityType::OLD_OPERATION_MOVE_RANGE)
      source = Serialization::decode_vstr(bufp, remainp);
    TableIdentifier table;
    legacy_decode(bufp, remainp, &table);
    RangeSpec range_spec;
    legacy_decode(bufp, remainp, &range_spec);
    string transfer_log = Serialization::decode_vstr(bufp, remainp);
    int64_t soft_limit = Serialization::decode_i64(bufp, remainp);
    bool is_split = Serialization::decode_bool(bufp, remainp);
    m_params = Lib::Master::Request::Parameters::MoveRange(source, 0, table, range_spec,
                                                           transfer_log, soft_limit,
                                                           is_split);
  }
  m_destination = Serialization::decode_vstr(bufp, remainp);
  m_range_name = format("%s[%s..%s]", m_params.table().id,
                        m_params.range_spec().start_row,
          m_params.range_spec().end_row);
  m_hash_code = Utility::range_hash_code(m_params.table(),m_params.range_spec(),
          string("OperationMoveRange-") + m_params.source());
  initialize_dependencies();
  set_remove_approval_mask(0x03);
}

void OperationMoveRange::decode_result(const uint8_t **bufp, size_t *remainp) {
  set_remove_approval_mask(0x03);
  Operation::decode_result(bufp, remainp);
}

const string OperationMoveRange::name() {
  if (m_state == OperationState::COMPLETE)
    return "OperationMoveRange";
  return format("OperationMoveRange %s %s[%s..%s] -> %s",
                m_params.source().c_str(), m_params.table().id,
                m_params.range_spec().start_row, m_params.range_spec().end_row,
                m_destination.c_str());
}

const string OperationMoveRange::label() {
  if (m_state == OperationState::COMPLETE)
    return "OperationMoveRange";
  return format("MoveRange %s %s[%s..%s] -> %s",
                m_params.source().c_str(), m_params.table().id,
                m_params.range_spec().start_row, m_params.range_spec().end_row,
                m_destination.c_str());
}

const string OperationMoveRange::graphviz_label() {
  string start_row = m_params.range_spec().start_row;
  string end_row = m_params.range_spec().end_row;

  if (start_row.length() > 20)
    start_row = start_row.substr(0, 10) + ".."
        + start_row.substr(start_row.length()-10, 10);

  if (!strcmp(end_row.c_str(), Key::END_ROW_MARKER))
    end_row = "END_ROW_MARKER";
  else if (end_row.length() > 20)
    end_row = end_row.substr(0, 10) + ".."
        + end_row.substr(end_row.length() - 10, 10);

  return format("MoveRange %s\\n%s\\n%s", m_params.table().id,
                start_row.c_str(), end_row.c_str());
}


int64_t OperationMoveRange::hash_code(const TableIdentifier &table,
                                      const RangeSpec &range,
                                      const std::string &source,
                                      int64_t range_id) {
  if (range_id)
    return Utility::range_hash_code(table, range, string("OperationMoveRange-") +
                                    source + ":" + range_id);
  return Utility::range_hash_code(table, range, string("OperationMoveRange-") +
                                  source);
}
