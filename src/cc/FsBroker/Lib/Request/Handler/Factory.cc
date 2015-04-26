/* -*- c++ -*-
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

/// @file
/// Definitions for %FsBroker request handler Factory.
/// This file contains definitions for Factory, a factory class for creating
/// request handler for file system brokers.

#include <Common/Compat.h>

#include "Factory.h"

#include "Append.h"
#include "Close.h"
#include "Create.h"
#include "Debug.h"
#include "Exists.h"
#include "Factory.h"
#include "Flush.h"
#include "Length.h"
#include "Mkdirs.h"
#include "Open.h"
#include "Pread.h"
#include "Readdir.h"
#include "Read.h"
#include "Remove.h"
#include "Rename.h"
#include "Rmdir.h"
#include "Seek.h"
#include "Status.h"
#include "Sync.h"

#include <Common/Logger.h>
#include <Common/Error.h>

using namespace Hypertable;
using namespace Hypertable::FsBroker::Lib::Request::Handler;

ApplicationHandler *Factory::create(Comm *comm, Broker *broker,
                                    EventPtr &event) {

  switch (event->header.command) {

  case FUNCTION_OPEN:
    return new Open(comm, broker, event);
  case FUNCTION_CREATE:
    return new Create(comm, broker, event);
  case FUNCTION_CLOSE:
    return new Close(comm, broker, event);
  case FUNCTION_READ:
    return new Read(comm, broker, event);
  case FUNCTION_APPEND:
    return new Append(comm, broker, event);
  case FUNCTION_SEEK:
    return new Seek(comm, broker, event);
  case FUNCTION_REMOVE:
    return new Remove(comm, broker, event);
  case FUNCTION_LENGTH:
    return new Length(comm, broker, event);
  case FUNCTION_PREAD:
    return new Pread(comm, broker, event);
  case FUNCTION_MKDIRS:
    return new Mkdirs(comm, broker, event);
  case FUNCTION_STATUS:
    return new Status(comm, broker, event);
  case FUNCTION_FLUSH:
    return new Flush(comm, broker, event);
  case FUNCTION_RMDIR:
    return new Rmdir(comm, broker, event);
  case FUNCTION_READDIR:
    return new Readdir(comm, broker, event);
  case FUNCTION_EXISTS:
    return new Exists(comm, broker, event);
  case FUNCTION_RENAME:
    return new Rename(comm, broker, event);
  case FUNCTION_DEBUG:
    return new Debug(comm, broker, event);
  case FUNCTION_SYNC:
    return new Sync(comm, broker, event);
  default:
    HT_THROWF(Error::INVALID_METHOD_IDENTIFIER,
              "%d", (int)event->header.command);
  }

  return nullptr;
}
