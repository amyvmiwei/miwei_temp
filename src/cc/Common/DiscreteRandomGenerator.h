/*
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

/** @file
 * Discrete Random Generator.
 * This is a base class implementing a discrete random generator.
 */

#ifndef HYPERTABLE_DISCRETERANDOMGENERATOR_H
#define HYPERTABLE_DISCRETERANDOMGENERATOR_H

#include <boost/random.hpp>
#include <boost/random/uniform_01.hpp>

#include "Common/ReferenceCount.h"

namespace Hypertable {

  /** @addtogroup Common
   *  @{
   */

  /**
   * Generates samples from a discrete probability distribution
   * in the range [0, max_val] by transforming a
   * uniform [0, 1] distribution into the desired distribution
   */
  class DiscreteRandomGenerator : public ReferenceCount {

  public:
    /** Default constructor; sets up a random number generator with a
     * constant seed value of 1. */
    DiscreteRandomGenerator();

    /** Destructor - cleans up allocated resources */
    virtual ~DiscreteRandomGenerator() { delete [] m_cmf; };

    /** Sets the seed for the random number generator
     *
     * @param s The seed value for the random number generator
     */
    void set_seed(uint32_t s) {
      m_rng.seed(s);
      m_u01 = boost::uniform_01<boost::mt19937>(m_rng);
    }

    /** Sets the size of the generated range
     * 
     * @param value_count The number of values in the range
     */
    void set_value_count(uint64_t value_count) {
      m_value_count = value_count;
      delete [] m_cmf;
      m_cmf = 0;
      delete [] m_numbers;
      m_numbers = 0;
    }

    /** Sets the lowest value of the desired distribution
     *
     * @param pool_min The lower bound of the range
     */
    void set_pool_min(uint64_t pool_min) {
      m_pool_min = pool_min;
      delete [] m_cmf;
      m_cmf = 0;
      delete [] m_numbers;
      m_numbers = 0;
    }

    /** Sets the highest value of the desired distribution
     *
     * @param pool_max The upper bound of the range
     */
    void set_pool_max(uint64_t pool_max) {
      m_pool_max = pool_max;
      delete [] m_cmf;
      m_cmf = 0;
      delete [] m_numbers;
      m_numbers = 0;
    }

    /** Returns a random sample from the distribution */
    virtual uint64_t get_sample();

  protected:
    /** Generate the cumulative mass function for the distribution */
    virtual void generate_cmf();

    /**
     * Returns the probability of drawing a specific value from the
     * distribution
     *
     * @param val value to be generated
     * @return probability of generating this value
     */
    virtual double pmf(uint64_t val) { return 1.0 / (double)m_value_count; }

    /** The random number generator */
    boost::mt19937 m_rng;

    /** The uniform distribution */
    boost::uniform_01<boost::mt19937> m_u01;

    /** Number of values in the range */
    uint64_t m_value_count;

    /** Lower bound of the range */
    uint64_t m_pool_min;

    /** Upper bound of the range */
    uint64_t m_pool_max;

    /** Array with the random samples */
    uint64_t *m_numbers;

    /** The cumulative mass of the distribution */
    double *m_cmf;
  };

  typedef boost::intrusive_ptr<DiscreteRandomGenerator>
            DiscreteRandomGeneratorPtr;

  /** @}*/

} // namespace Hypertable

#endif // HYPERTABLE_DISCRETERANDOMGENERATOR_H
