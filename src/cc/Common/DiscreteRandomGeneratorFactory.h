/*
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

/** @file
 * Factory for Discrete Random Generators.
 * This file contains a factory class for the various discrete random
 * generators.
 */

#ifndef HYPERTABLE_DISCRETERANDOMGENERATORFACTORY_H
#define HYPERTABLE_DISCRETERANDOMGENERATORFACTORY_H

#include "Common/String.h"

#include "DiscreteRandomGenerator.h"

namespace Hypertable {

  /** @addtogroup Common
   *  @{
   */

  /**
   * Static factory class for discrete random generators
   */
  class DiscreteRandomGeneratorFactory {
    public:
      /**
       * Creates a new DiscreteRandomGenerator instance.
       *
       * spec is one of the following:
       *   "uniform"
       *   "zipf"
       *   "zipf\t--s=<S>"  (where `S` is the seed)
       *
       * An unknown or invalid spec will cause termination of the application.
       *
       * @param spec The DiscreteRandomGenerator specification, as described
       *        above
       * @return Pointer to a newly created DiscreteRandomGenerator object
       */
      static DiscreteRandomGenerator *create(const String &spec);
  };

  /** @}*/

}

#endif // HYPERTABLE_DISCRETERANDOMGENERATORFACTORY_H
