/*
 * Copyright (C) 2007-2015 Hypertable, Inc.
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
 * Declarations for ResponseManager.
 * This file contains declarations for ResponseManager, a class for managing
 * the sending operation results back to requesting clients.
 */

#include <Common/Compat.h>

#include "ResponseManager.h"

#include <chrono>

using namespace Hypertable;
using namespace std;

void ResponseManager::operator()() {
  ResponseManagerContext::OperationExpirationTimeIndex &op_expiration_time_index = m_context->expirable_ops.get<1>();
  ResponseManagerContext::OperationExpirationTimeIndex::iterator op_iter;
  ResponseManagerContext::DeliveryExpirationTimeIndex &delivery_expiration_time_index = m_context->delivery_list.get<1>();
  ResponseManagerContext::DeliveryExpirationTimeIndex::iterator delivery_iter;
  ClockT::time_point now, expire_time;
  bool timed_wait;
  bool shutdown = false;
  std::vector<OperationPtr> operations;
  std::vector<MetaLog::EntityPtr> entities;

  try {

    while (!shutdown) {

      {
        unique_lock<mutex> lock(m_context->mutex);

        timed_wait = false;
        if (!op_expiration_time_index.empty()) {
          expire_time = op_expiration_time_index.begin()->expiration_time();
          timed_wait = true;
        }

        if (!delivery_expiration_time_index.empty()) {
          if (!timed_wait || delivery_expiration_time_index.begin()->expiration_time < expire_time)
            expire_time = delivery_expiration_time_index.begin()->expiration_time;
          timed_wait = true;
        }

        if (timed_wait)
          m_context->cond.wait_until(lock, expire_time);
        else
          m_context->cond.wait(lock);

        shutdown = m_context->shutdown;

        now = ClockT::now();

        op_iter = op_expiration_time_index.begin();
        while (op_iter != op_expiration_time_index.end()) {
          if (op_iter->expiration_time() < now) {
            m_context->removal_queue.push_back(op_iter->op);
            op_iter = op_expiration_time_index.erase(op_iter);
          }
          else
            break;
        }

        delivery_iter = delivery_expiration_time_index.begin();
        while (delivery_iter != delivery_expiration_time_index.end()) {
          if (delivery_iter->expiration_time < now)
            delivery_iter = delivery_expiration_time_index.erase(delivery_iter);
          else
            break;
        }

        operations.clear();
        entities.clear();
        if (!m_context->removal_queue.empty()) {
          for (auto & operation : m_context->removal_queue) {
            if (!operation->ephemeral()) {
              HT_ASSERT(m_context->mml_writer);
              operations.push_back(operation);
              entities.push_back(operation);
            }
          }
          m_context->removal_queue.clear();
        }
      }

      if (!entities.empty())
        m_context->mml_writer->record_removal(entities);

    }
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
  }
  
}


void ResponseManager::add_delivery_info(int64_t operation_id, EventPtr &event) {
  lock_guard<mutex> lock(m_context->mutex);
  ResponseManagerContext::OperationIdentifierIndex &operation_identifier_index = m_context->expirable_ops.get<2>();
  ResponseManagerContext::OperationIdentifierIndex::iterator iter;
  ResponseManagerContext::DeliveryRec delivery_rec;

  delivery_rec.id = operation_id;
  if ((iter = operation_identifier_index.find(delivery_rec.id)) == operation_identifier_index.end()) {
    delivery_rec.event = event;
    delivery_rec.expiration_time = ClockT::now() +
      chrono::milliseconds(event->header.timeout_ms);
    m_context->delivery_list.push_back(delivery_rec);
  }
  else {
    int error = Error::OK;
    CommHeader header;
    header.initialize_from_request_header(event->header);
    CommBufPtr cbp(new CommBuf(header, iter->op->encoded_result_length()));
    iter->op->encode_result( cbp->get_data_ptr_address() );
    error = m_context->comm->send_response(event->addr, cbp);
    if (error != Error::OK)
      HT_ERRORF("Problem sending ID response back for %s operation (id=%lld) - %s",
                iter->op->label().c_str(), (Lld)iter->op->id(), Error::get_text(error));
    m_context->removal_queue.push_back(iter->op);
    operation_identifier_index.erase(iter);
    m_context->cond.notify_all();
  }
}


void ResponseManager::add_operation(OperationPtr &operation) {
  lock_guard<mutex> lock(m_context->mutex);
  ResponseManagerContext::DeliveryIdentifierIndex &delivery_identifier_index = m_context->delivery_list.get<2>();
  ResponseManagerContext::DeliveryIdentifierIndex::iterator iter;

  HT_ASSERT(operation->get_remove_approval_mask() == 0);

  if ((iter = delivery_identifier_index.find(operation->id())) == delivery_identifier_index.end())
    m_context->expirable_ops.push_back(ResponseManagerContext::OperationRec(operation));
  else {
    int error = Error::OK;
    CommHeader header;
    header.initialize_from_request_header(iter->event->header);
    CommBufPtr cbp(new CommBuf(header, operation->encoded_result_length()));
    operation->encode_result( cbp->get_data_ptr_address() );
    error = m_context->comm->send_response(iter->event->addr, cbp);
    if (error != Error::OK)
      HT_ERRORF("Problem sending ID response back for %s operation (id=%lld) - %s",
                operation->label().c_str(), (Lld)operation->id(), Error::get_text(error));
    m_context->removal_queue.push_back(operation);
    delivery_identifier_index.erase(iter);
    m_context->cond.notify_all();
  }
}


void ResponseManager::shutdown() {
  lock_guard<mutex> lock(m_context->mutex);
  m_context->shutdown = true;
  m_context->cond.notify_all();
}
