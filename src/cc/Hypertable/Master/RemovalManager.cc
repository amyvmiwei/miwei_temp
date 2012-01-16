/** -*- c++ -*-
 * Copyright (C) 2007-2012 Hypertable, Inc.
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

#include "Common/Compat.h"

#include "RemovalManager.h"

using namespace Hypertable;


RemovalManager::RemovalManager(MetaLog::WriterPtr &mml_writer) {
  m_ctx = new RemovalManagerContext();
  m_ctx->mml_writer = mml_writer;
  m_thread = new Thread(*this);
}

void RemovalManager::operator()() {
  std::vector<MetaLog::Entity *> entities;
  std::vector<OperationPtr> operations;

  while (!m_ctx->shutdown) {

    {
      ScopedLock lock(m_ctx->mutex);

      while (m_ctx->removal_queue.empty() && !m_ctx->shutdown)
	m_ctx->cond.wait(lock);

      if (!m_ctx->removal_queue.empty()) {
	for (std::list<OperationPtr>::iterator iter=m_ctx->removal_queue.begin();
	     iter != m_ctx->removal_queue.end(); ++iter) {
	  operations.push_back(*iter);
	  entities.push_back(iter->get());
	}
	m_ctx->removal_queue.clear();
      }
    }

    if (!entities.empty()) {
      m_ctx->mml_writer->record_removal(entities);
      entities.clear();
      operations.clear();
    }
  }
}

void RemovalManager::shutdown() {
  {
    ScopedLock lock(m_ctx->mutex);
    m_ctx->shutdown = true;
    m_ctx->cond.notify_all();
  }
  m_thread->join();
  delete m_ctx;
}


bool RemovalManager::add_operation(Operation *operation) {
  ScopedLock lock(m_ctx->mutex);

  if (m_ctx->map.find(operation->hash_code()) != m_ctx->map.end())
    return false;

  m_ctx->map[operation->hash_code()] = RemovalManagerContext::RemovalRec(operation, operation->remove_approvals_required());
  return true;
}

void RemovalManager::approve_removal(int64_t hash_code) {
  ScopedLock lock(m_ctx->mutex);
  RemovalManagerContext::RemovalMapT::iterator iter = m_ctx->map.find(hash_code);
  HT_ASSERT(iter != m_ctx->map.end());
  HT_ASSERT((*iter).second.approvals_remaining > 0);
  (*iter).second.approvals_remaining--;
  if ((*iter).second.approvals_remaining == 0) {
    m_ctx->removal_queue.push_back((*iter).second.op);
    m_ctx->map.erase(iter);
    m_ctx->cond.notify_all();
  }
}
