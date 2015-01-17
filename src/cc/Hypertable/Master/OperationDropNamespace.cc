/*
 * Copyright (C) 2007-2014 Hypertable, Inc.
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

#include "OperationDropNamespace.h"

#include <Hypertable/Lib/Master/NamespaceFlag.h>

#include <Common/Error.h>
#include <Common/FailureInducer.h>
#include <Common/ScopeGuard.h>
#include <Common/Serialization.h>

#include <boost/algorithm/string.hpp>

using namespace Hypertable;
using namespace Hypertable::Lib::Master;

OperationDropNamespace::OperationDropNamespace(ContextPtr &context,
                                               const String &name,
                                               int32_t flags)
  : Operation(context, MetaLog::EntityType::OPERATION_DROP_NAMESPACE),
    m_params(name, flags) {
  initialize_dependencies();
}

OperationDropNamespace::OperationDropNamespace(ContextPtr &context,
                                          const MetaLog::EntityHeader &header_)
  : Operation(context, header_) {
}

OperationDropNamespace::OperationDropNamespace(ContextPtr &context,
                                               EventPtr &event) 
  : Operation(context, event, MetaLog::EntityType::OPERATION_DROP_NAMESPACE) {
  const uint8_t *ptr = event->payload;
  size_t remaining = event->payload_len;
  m_params.decode(&ptr, &remaining);
  initialize_dependencies();
}

void OperationDropNamespace::initialize_dependencies() {
  m_exclusivities.insert(m_params.name());
  m_dependencies.insert(Dependency::INIT);
}


void OperationDropNamespace::execute() {
  bool is_namespace;
  String hyperspace_dir;
  int32_t state = get_state();

  HT_INFOF("Entering DropNamespace-%lld(%s, flags=%s) state=%s",
           (Lld)header.id, m_params.name().c_str(),
           NamespaceFlag::to_str(m_params.flags()).c_str(),
           OperationState::get_text(state));

  switch (state) {

  case OperationState::INITIAL:
    // Check to see if namespace exists
    if(m_context->namemap->name_to_id(m_params.name(), m_id, &is_namespace)) {
      if (!is_namespace) {
        complete_error(Error::BAD_NAMESPACE, format("%s is not a namespace", m_params.name().c_str()));
        return;
      }
      set_state(OperationState::STARTED);
      m_context->mml_writer->record_state(shared_from_this());
    }
    else {
      if (m_params.flags() & NamespaceFlag::IF_EXISTS)
        complete_ok();
      else
        complete_error(Error::NAMESPACE_DOES_NOT_EXIST, m_params.name());
      return;
    }
    HT_MAYBE_FAIL("drop-namespace-INITIAL");

  case OperationState::STARTED:
    try {
      m_context->namemap->drop_mapping(m_params.name());
      HT_MAYBE_FAIL("drop-namespace-STARTED-a");
      hyperspace_dir = m_context->toplevel_dir + "/tables/" + m_id;
      m_context->hyperspace->unlink(hyperspace_dir);
      HT_MAYBE_FAIL("drop-namespace-STARTED-b");
    }
    catch (Exception &e) {
      if (e.code() == Error::INDUCED_FAILURE)
        throw;
      if (e.code() != Error::HYPERSPACE_FILE_NOT_FOUND &&
          e.code() != Error::HYPERSPACE_BAD_PATHNAME) {
        HT_ERROR_OUT << e << HT_END;
        complete_error(e);
        return;
      }
    }
    complete_ok();
    break;

  default:
    HT_FATALF("Unrecognized state %d", state);
  }

  HT_INFOF("Leaving DropNamespace-%lld(%s, flags=%s) state=%s",
           (Lld)header.id, m_params.name().c_str(),
           NamespaceFlag::to_str(m_params.flags()).c_str(),
           OperationState::get_text(get_state()));

}


void OperationDropNamespace::display_state(std::ostream &os) {
  os << " name=" << m_params.name()
     << " flags=" << NamespaceFlag::to_str(m_params.flags())
     << " id=" << m_id << " ";
}

uint8_t OperationDropNamespace::encoding_version_state() const {
  return 1;
}

size_t OperationDropNamespace::encoded_length_state() const {
  return m_params.encoded_length() + Serialization::encoded_length_vstr(m_id);
}

void OperationDropNamespace::encode_state(uint8_t **bufp) const {
  m_params.encode(bufp);
  Serialization::encode_vstr(bufp, m_id);
}

void OperationDropNamespace::decode_state(uint8_t version, const uint8_t **bufp, size_t *remainp) {
  m_params.decode(bufp, remainp);
  m_id = Serialization::decode_vstr(bufp, remainp);
}

void OperationDropNamespace::decode_state_old(uint8_t version, const uint8_t **bufp, size_t *remainp) {
  string name = Serialization::decode_vstr(bufp, remainp);
  int32_t flags = Serialization::decode_i32(bufp, remainp);
  m_params = Lib::Master::Request::Parameters::DropNamespace(name, flags);
  m_id = Serialization::decode_vstr(bufp, remainp);
}

const String OperationDropNamespace::name() {
  return "OperationDropNamespace";
}

const String OperationDropNamespace::label() {
  return String("DropNamespace ") + m_params.name();
}

