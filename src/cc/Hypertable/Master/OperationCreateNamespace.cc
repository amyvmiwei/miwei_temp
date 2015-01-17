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

#include "OperationCreateNamespace.h"

#include <Hypertable/Lib/Master/NamespaceFlag.h>

#include <Common/Error.h>
#include <Common/FailureInducer.h>
#include <Common/Serialization.h>

using namespace Hypertable;
using namespace Hypertable::Lib::Master;
using namespace std;

OperationCreateNamespace::OperationCreateNamespace(ContextPtr &context,
                                                   const string &name,
                                                   int32_t flags)
  : Operation(context, MetaLog::EntityType::OPERATION_CREATE_NAMESPACE),
    m_params(name, flags) {
  initialize_dependencies();
}

OperationCreateNamespace::OperationCreateNamespace(ContextPtr &context,
                                                   const MetaLog::EntityHeader &header_)
  : Operation(context, header_) {
}

OperationCreateNamespace::OperationCreateNamespace(ContextPtr &context, EventPtr &event) 
  : Operation(context, event, MetaLog::EntityType::OPERATION_CREATE_NAMESPACE) {
  const uint8_t *ptr = event->payload;
  size_t remaining = event->payload_len;
  m_params.decode(&ptr, &remaining);
  initialize_dependencies();
}

void OperationCreateNamespace::initialize_dependencies() {
  m_exclusivities.insert(m_params.name());
  if (m_params.name() != "/sys" && m_params.name() != "/tmp")
    m_dependencies.insert(Dependency::INIT);
}

void OperationCreateNamespace::execute() {
  bool is_namespace;
  int nsflags = NameIdMapper::IS_NAMESPACE;
  string hyperspace_dir;
  int32_t state = get_state();

  HT_INFOF("Entering CreateNamespace-%lld('%s', flags=%s, '%s') state=%s",
           (Lld)header.id, m_params.name().c_str(),
           NamespaceFlag::to_str(m_params.flags()).c_str(),
           m_id.c_str(), OperationState::get_text(state));

  switch (state) {

  case OperationState::INITIAL:
    // Check to see if namespace exists
    if (m_context->namemap->exists_mapping(m_params.name(), &is_namespace)) {
      if (m_params.flags() & NamespaceFlag::IF_NOT_EXISTS)
        complete_ok();
      else if (is_namespace)
        complete_error(Error::NAMESPACE_EXISTS, "");
      else
        complete_error(Error::NAME_ALREADY_IN_USE, "");
      return;
    }
    set_state(OperationState::ASSIGN_ID);
    m_context->mml_writer->record_state(shared_from_this());
    HT_MAYBE_FAIL("create-namespace-INITIAL");

  case OperationState::ASSIGN_ID:
    if (m_params.flags() & NamespaceFlag::CREATE_INTERMEDIATE)
      nsflags |= NameIdMapper::CREATE_INTERMEDIATE;
    try {
      m_context->namemap->add_mapping(m_params.name(), m_id, nsflags, true);
    }
    catch (Exception &e) {
      if (e.code() == Error::INDUCED_FAILURE)
        throw;
      if (e.code() != Error::NAMESPACE_DOES_NOT_EXIST)
        HT_ERROR_OUT << e << HT_END;
      complete_error(e);
      return;
    }
    HT_MAYBE_FAIL("create-namespace-ASSIGN_ID-a");
    hyperspace_dir = m_context->toplevel_dir + "/tables" + m_id;
    try {
      if (m_params.flags() & NamespaceFlag::CREATE_INTERMEDIATE)
        m_context->hyperspace->mkdirs(hyperspace_dir);
      else
        m_context->hyperspace->mkdir(hyperspace_dir);
    }
    catch (Exception &e) {
      if (e.code() != Error::HYPERSPACE_FILE_EXISTS)
        HT_THROW2F(e.code(), e, "CreateNamespace %s -> %s", m_params.name().c_str(), m_id.c_str());
    }
    HT_MAYBE_FAIL("create-namespace-ASSIGN_ID-b");
    complete_ok();
    HT_MAYBE_FAIL("create-namespace-ASSIGN_ID-c");
    break;

  default:
    HT_FATALF("Unrecognized state %d", state);
  }

  HT_INFOF("Leaving CreateNamespace-%lld('%s', flags=%s, '%s') state=%s",
           (Lld)header.id, m_params.name().c_str(),
           NamespaceFlag::to_str(m_params.flags()).c_str(), m_id.c_str(),
           OperationState::get_text(get_state()));

}

void OperationCreateNamespace::display_state(std::ostream &os) {
  os << " name=" << m_params.name() << " flags=" << NamespaceFlag::to_str(m_params.flags());
  if (m_id != "")
    os << " (id=" << m_id << ")";
}

uint8_t OperationCreateNamespace::encoding_version_state() const {
  return 1;
}

size_t OperationCreateNamespace::encoded_length_state() const {
  return m_params.encoded_length() + Serialization::encoded_length_vstr(m_id);
}

void OperationCreateNamespace::encode_state(uint8_t **bufp) const {
  m_params.encode(bufp);
  Serialization::encode_vstr(bufp, m_id);
}

void OperationCreateNamespace::decode_state(uint8_t version,
                                            const uint8_t **bufp,
                                            size_t *remainp) {
  m_params.decode(bufp, remainp);
  m_id = Serialization::decode_vstr(bufp, remainp);
}

void OperationCreateNamespace::decode_state_old(uint8_t version,
                                                const uint8_t **bufp,
                                                size_t *remainp) {
  string name = Serialization::decode_vstr(bufp, remainp);
  int32_t flags = Serialization::decode_i32(bufp, remainp);
  m_params = Lib::Master::Request::Parameters::CreateNamespace(name, flags);
  m_id = Serialization::decode_vstr(bufp, remainp);
}

const string OperationCreateNamespace::name() {
  return "OperationCreateNamespace";
}

const string OperationCreateNamespace::label() {
  return string("CreateNamespace ") + m_params.name();
}

