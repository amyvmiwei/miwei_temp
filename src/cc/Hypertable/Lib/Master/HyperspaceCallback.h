/* -*- c++ -*-
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

#ifndef Hypertable_Lib_Master_HyperspaceCallback_h
#define Hypertable_Lib_Master_HyperspaceCallback_h

#include "Client.h"

#include <Hyperspace/HandleCallback.h>

#include <AsyncComm/ApplicationQueueInterface.h>

namespace Hypertable {
namespace Lib {
namespace Master {

  using namespace Hyperspace;

  /// @addtogroup libHypertableMaster
  /// @{

  class HyperspaceCallback : public Hyperspace::HandleCallback {
  public:
    HyperspaceCallback(Client *client,
                                 ApplicationQueueInterfacePtr &app_queue)
      : HandleCallback(EVENT_MASK_ATTR_SET), m_client(client),
        m_app_queue(app_queue) { }

    virtual void attr_set(const std::string &name);
    Client *m_client;
    ApplicationQueueInterfacePtr m_app_queue;
  };

  /// @}

}}}

#endif // Hypertable_Lib_Master_HyperspaceCallback_h
