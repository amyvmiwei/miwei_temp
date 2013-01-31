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
#include "AsyncComm/Protocol.h"

#include "Common/Error.h"
#include "Common/Logger.h"

#include "TableMutatorAsyncHandler.h"
#include "TableMutatorAsync.h"
#include "TableMutatorAsyncScatterBuffer.h"

using namespace Hypertable;
using namespace Serialization;


/**
 *
 */
TableMutatorAsyncHandler::TableMutatorAsyncHandler(TableMutatorAsync *mutator,
    uint32_t scatter_buffer) : ApplicationHandler(0),
    m_mutator(mutator), m_scatter_buffer(scatter_buffer) {
}

/**
 *
 */
void TableMutatorAsyncHandler::run() {
  // The scatter buffer will get destroyed when the TableMutator releases it
  // and this method goes out of scope
  TableMutatorAsyncScatterBufferPtr buffer =
      m_mutator->get_outstanding_buffer(m_scatter_buffer);
  if (!buffer)
    HT_FATALF("Unable to locate buffer %d from mutator %p", (int)m_scatter_buffer, (void *)m_mutator);
  buffer->finish();
}

