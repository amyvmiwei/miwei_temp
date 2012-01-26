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
#include "Common/Logger.h"

#include "UpdateThread.h"

using namespace Hypertable;

void UpdateThread::operator()() {
  
  try {
    switch (m_sequence_number) {
    case 0:
      m_range_server->update_qualify_and_transform();
      break;
    case 1:
      m_range_server->update_add_and_respond();
      break;
    default:
      m_range_server->update_commit();
    }
  }
  catch (Exception &e) {
    HT_FATAL_OUT << e << HT_END;
  }
  catch (std::exception &e) {
    HT_FATALF("caught std::exception: %s", e.what());
  }
  catch (...) {
    HT_FATAL("caught unknown exception here");
  }

}
