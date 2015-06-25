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
 * Discreet Random Generator creating uniform distributed numbers
 */

#ifndef Common_DiscreteRandomGeneratorUniform_h
#define Common_DiscreteRandomGeneratorUniform_h

#include "DiscreteRandomGenerator.h"

namespace Hypertable {

/** @addtogroup Common
 *  @{
 */

  /**
   * Generate samples from Uniform distribution
   */
  class DiscreteRandomGeneratorUniform: public DiscreteRandomGenerator {
  public:
    DiscreteRandomGeneratorUniform() : DiscreteRandomGenerator() { }

    virtual uint64_t get_sample() { 
      auto rval = std::uniform_int_distribution<uint64_t>()(m_random_engine);
      if (m_pool_max != 0)
        rval = m_pool_min + (rval % (m_pool_max - m_pool_min));
      return rval;
    }
  };

/** @}*/

}

#endif // Common_DiscreteRandomGeneratorUniform_h
