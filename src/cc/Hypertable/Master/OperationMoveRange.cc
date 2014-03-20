/*
 * Copyright (C) 2007-2013 Hypertable, Inc.
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
#include "Common/FailureInducer.h"
#include "Common/ScopeGuard.h"
#include "Common/Serialization.h"
#include "Common/System.h"
#include "Common/md5.h"

#include "AsyncComm/ResponseCallback.h"

#include "Hypertable/Lib/Key.h"

#include "OperationMoveRange.h"
#include "OperationProcessor.h"
#include "Utility.h"
#include "BalancePlanAuthority.h"

extern "C" {
#include <poll.h>
}

using namespace Hypertable;
using namespace Hyperspace;

OperationMoveRange::OperationMoveRange(ContextPtr &context, const String &source,
        const TableIdentifier &table, const RangeSpec &range,
        const String &transfer_log, uint64_t soft_limit, bool is_split)
  : Operation(context, MetaLog::EntityType::OPERATION_MOVE_RANGE),
    m_table(table), m_range(range), m_transfer_log(transfer_log),
    m_soft_limit(soft_limit), m_is_split(is_split), m_source(source) {
  m_range_name = format("%s[%s..%s]", m_table.id, m_range.start_row,
          m_range.end_row);
  initialize_dependencies();
  m_hash_code = Utility::range_hash_code(m_table, m_range,
          String("OperationMoveRange-") + m_source);
  set_remove_approval_mask(0x03);
}

OperationMoveRange::OperationMoveRange(ContextPtr &context,
        const MetaLog::EntityHeader &header_)
  : Operation(context, header_), m_source("UNKNOWN") {
}

OperationMoveRange::OperationMoveRange(ContextPtr &context, EventPtr &event)
  : Operation(context, event, MetaLog::EntityType::OPERATION_MOVE_RANGE) {
  const uint8_t *ptr = event->payload;
  size_t remaining = event->payload_len;
  decode_request(&ptr, &remaining);
  set_remove_approval_mask(0x03);
}

void OperationMoveRange::initialize_dependencies() {
  m_exclusivities.insert(Utility::range_hash_string(m_table, m_range,
              "OperationMoveRange"));

  m_dependencies.insert(Dependency::INIT);
  m_dependencies.insert(Dependency::SERVERS);
  m_dependencies.insert(Utility::range_hash_string(m_table, m_range, ""));

  if (!strcmp(m_table.id, TableIdentifier::METADATA_ID)) {
    m_dependencies.insert(Dependency::ROOT);
  }
  else if (!strncmp(m_table.id, "0/", 2)) {
    m_dependencies.insert(Dependency::ROOT);
    m_dependencies.insert(Dependency::METADATA);
  }
  else {
    m_dependencies.insert(Dependency::ROOT);
    m_dependencies.insert(Dependency::METADATA);
    m_dependencies.insert(Dependency::SYSTEM);
  }
  m_obstructions.insert(String("OperationMove ") + m_range_name);
  m_obstructions.insert(String(m_table.id) + " move range");
}

void OperationMoveRange::execute() {
  int32_t state = get_state();
  String old_destination = m_destination;
  BalancePlanAuthority *bpa = m_context->get_balance_plan_authority();

  HT_INFOF("Entering MoveRange-%lld %s state=%s",
          (Lld)header.id, m_range_name.c_str(),
          OperationState::get_text(state));

  switch (state) {

  case OperationState::INITIAL:

    if (!bpa->get_balance_destination(m_table, m_range, m_destination)) {
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
    m_context->mml_writer->record_state(this);
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
      RangeServerClient rsc(m_context->comm);
      CommAddress addr;
      RangeState range_state;
      TableIdentifier *table = &m_table;
      RangeSpec *range = &m_range;

      addr.set_proxy(m_destination);
      range_state.soft_limit = m_soft_limit;
      range_state.transfer_log = m_transfer_log.c_str();
      if (m_context->test_mode)
        HT_WARNF("Skipping %s::load_range() because in TEST MODE",
                m_destination.c_str());
      else
        rsc.load_range(addr, *table, *range, range_state, !m_is_split);
    }
    catch (Exception &e) {
      if (e.code() != Error::RANGESERVER_RANGE_ALREADY_LOADED) {

        // If not yet relinquished, wait a couple of seconds and try again
        if (e.code() == Error::RANGESERVER_RANGE_NOT_YET_RELINQUISHED) {
          HT_INFOF("%s - %s", Error::get_text(e.code()), e.what());
          poll(0, 0, 2000);
          return;
        }

        if (!Utility::table_exists(m_context, m_table.id)) {
          HT_WARNF("Aborting MoveRange %s because table no longer exists",
                   m_range_name.c_str());
          bpa->balance_move_complete(m_table, m_range);
          remove_approval_add(0x03);
          complete_ok(bpa);
          return;
        }

        // server might be down - go back to the initial state and pick a
        // new destination
        HT_WARNF("Problem moving range %s to %s: %s - %s",
                 m_range_name.c_str(), m_destination.c_str(),
                 Error::get_text(e.code()), e.what());
        poll(0, 0, 5000);
        set_state(OperationState::INITIAL);
        return;
      }
    }
    HT_MAYBE_FAIL("move-range-LOAD_RANGE");
    set_state(OperationState::ACKNOWLEDGE);
    m_context->mml_writer->record_state(this);

  case OperationState::ACKNOWLEDGE:
    {
      RangeServerClient rsc(m_context->comm);
      CommAddress addr;
      TableIdentifier *table = &m_table;
      RangeSpec *range = &m_range;

      addr.set_proxy(m_destination);
      if (m_context->test_mode) {
        HT_WARNF("Skipping %s::acknowledge_load() because in TEST MODE",
                m_destination.c_str());
      }
      else {
        try {
          QualifiedRangeSpec qrs(*table, *range);
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
          poll(0, 0, 5000);
          // Fetch new destination, if it changed, and then try again
          if (!bpa->get_balance_destination(m_table, m_range, m_destination))
            m_context->op->unblock(Dependency::SERVERS);
          return;
        }
      }
    }
    bpa->balance_move_complete(m_table, m_range);
    remove_approval_add(0x02);
    complete_ok(bpa);
    break;

  case OperationState::COMPLETE:
    break;

  default:
    HT_FATALF("Unrecognized state %d", state);
  }

  HT_INFOF("Leaving MoveRange-%lld %s -> %s",
          (Lld)header.id, m_range_name.c_str(), m_destination.c_str());
}

void OperationMoveRange::display_state(std::ostream &os) {
  os << " " << m_table << " " << m_range << " transfer-log='" << m_transfer_log;
  os << "' soft-limit=" << m_soft_limit << " is_split="
     << ((m_is_split) ? "true" : "false");
  os << "source='" << m_source << "' location='" << m_destination << "' ";
}

#define OPERATION_MOVE_RANGE_VERSION 1

uint16_t OperationMoveRange::encoding_version() const {
  return OPERATION_MOVE_RANGE_VERSION;
}

size_t OperationMoveRange::encoded_state_length() const {
  return Serialization::encoded_length_vstr(m_source)
      + m_table.encoded_length() + m_range.encoded_length()
      + Serialization::encoded_length_vstr(m_transfer_log) + 9
      + Serialization::encoded_length_vstr(m_destination);
}

void OperationMoveRange::encode_state(uint8_t **bufp) const {
  Serialization::encode_vstr(bufp, m_source);
  m_table.encode(bufp);
  m_range.encode(bufp);
  Serialization::encode_vstr(bufp, m_transfer_log);
  Serialization::encode_i64(bufp, m_soft_limit);
  Serialization::encode_bool(bufp, m_is_split);
  Serialization::encode_vstr(bufp, m_destination);
}

void OperationMoveRange::decode_state(const uint8_t **bufp, size_t *remainp) {
  set_remove_approval_mask(0x03);
  decode_request(bufp, remainp);
  m_destination = Serialization::decode_vstr(bufp, remainp);
}

void OperationMoveRange::decode_request(const uint8_t **bufp, size_t *remainp) {
  if (get_original_type() != MetaLog::EntityType::OLD_OPERATION_MOVE_RANGE)
    m_source = Serialization::decode_vstr(bufp, remainp);
  m_table.decode(bufp, remainp);
  m_range.decode(bufp, remainp);
  m_transfer_log = Serialization::decode_vstr(bufp, remainp);
  m_soft_limit = Serialization::decode_i64(bufp, remainp);
  m_is_split = Serialization::decode_bool(bufp, remainp);
  m_range_name = format("%s[%s..%s]", m_table.id, m_range.start_row,
          m_range.end_row);
  m_hash_code = Utility::range_hash_code(m_table, m_range,
          String("OperationMoveRange-") + m_source);
  initialize_dependencies();
}

void OperationMoveRange::decode_result(const uint8_t **bufp, size_t *remainp) {
  set_remove_approval_mask(0x03);
  Operation::decode_result(bufp, remainp);
}

const String OperationMoveRange::name() {
  if (m_state == OperationState::COMPLETE)
    return "OperationMoveRange";
  return format("OperationMoveRange %s %s[%s..%s] -> %s", m_source.c_str(),
                m_table.id, m_range.start_row, m_range.end_row, m_destination.c_str());
}

const String OperationMoveRange::label() {
  if (m_state == OperationState::COMPLETE)
    return "OperationMoveRange";
  return format("MoveRange %s %s[%s..%s] -> %s",
                m_source.c_str(), m_table.id, m_range.start_row, m_range.end_row, m_destination.c_str());
}

const String OperationMoveRange::graphviz_label() {
  String start_row = m_range.start_row;
  String end_row = m_range.end_row;

  if (start_row.length() > 20)
    start_row = start_row.substr(0, 10) + ".."
        + start_row.substr(start_row.length()-10, 10);

  if (!strcmp(end_row.c_str(), Key::END_ROW_MARKER))
    end_row = "END_ROW_MARKER";
  else if (end_row.length() > 20)
    end_row = end_row.substr(0, 10) + ".."
        + end_row.substr(end_row.length() - 10, 10);

  return format("MoveRange %s\\n%s\\n%s", m_table.id, start_row.c_str(),
          end_row.c_str());
}
