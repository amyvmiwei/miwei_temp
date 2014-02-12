/* -*- c++ -*-
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
 * The FailureInducer simulates errors.
 * The FailureInducer parses a list of specs (usually set in a
 * test) and returns errors, throws exceptions or terminates the application
 * depending on the these arguments.
 *
 * There are a few helper macros that will check for induced failures:
 *
 *   Will induce a failure if "label" is active
 *      HT_MAYBE_FAIL(label)
 *
 *   Will induce a failure if "label" is active and "test" is true
 *      HT_MAYBE_FAIL_X(label, test)
 *
 *   Allows usage in an "if" block: if (HT_FAILURE_SIGNALLED("lbl")) { }
 *      HT_FAILURE_SIGNALLED(label) { }
 *
 *   A FailureInducer spec has the following layout:
 *      label:action<(parameter)>:iteration
 *
 *   Examples:
 *      label:exit:0
 *      label:signal:10
 *      label:pause(<milli-sec>):5
 *      label:throw(<decimal-code>):0
 *      label:throw(0x<hexadecimal-code>):0
 */

#ifndef HYPERTABLE_FAILUREINDUCER_H
#define HYPERTABLE_FAILUREINDUCER_H

#include <Common/Mutex.h>
#include <Common/String.h>
#include <Common/StringExt.h>

#include <unordered_map>

namespace Hypertable {

  /** @addtogroup Common
   *  @{
   */

  class FailureInducer {
  public:
    /** This is a singleton class
     *
     * When initializing an application, the FailureInducer is usually
     * instantiated like this:
     *
     *     if (FailureInducer::instance == 0)
     *       FailureInducer::instance = new FailureInducer();
     */
    static FailureInducer *instance;

    /** Returns true if the FailureInducer is enabled (= if an instance was
     * allocated)
     */
    static bool enabled() { return instance != 0; }

    /** Parses a spec string (as explained above) and stores it in an
     * internal structure. There can be multiple calls to parse_options,
     * and multiple failure inducer specs can be concatenated by ';'.
     *
     * @param spec The spec string
     */
    void parse_option(String spec);

    /** Tests and executes the induced failures. This function is usually
     * not invoked directly; use HT_MAYBE_FAIL and HT_MAYBE_FAIL_X
     * instead */
    void maybe_fail(const String &label);

    /** Returns true if a failure was signalled. This function is usually
     * not invoked directly; use HT_FAILURE_SIGNALLED instead */
    bool failure_signalled(const String &label);

    /** Clears the failure inducer */
    void clear();

  private:
    /** Helper function to parse a single option */
    void parse_option_single(String option);

    /** Internal structure to store a single failure setting */
    struct failure_inducer_state {
      /** Current iteration of the failure */
      uint32_t iteration;
      /** Number of iterations after which the failure is triggered */
      uint32_t trigger_iteration;
      /** The failure type; an enum in FailureInducer.cc */
      int failure_type;
      /** The error code which is thrown (if @a type is FAILURE_TYPE_THROW) */
      int error_code;
      /** Milliseconds to pause (if @a type is FAILURE_TYPE_PAUSE) */
      int pause_millis;
    };

    typedef std::unordered_map<String, failure_inducer_state *> StateMap;

    /** A mutex to serialize access */
    Mutex m_mutex;

    /** A list of all failure settings */
    StateMap m_state_map;
  };

}

/** Convenience macro which tests if a failure should be induced; if yes then an
 * error is thrown/returned or the application pauses/exits
 */
#define HT_MAYBE_FAIL(_label_) \
  if (Hypertable::FailureInducer::enabled()) { \
    Hypertable::FailureInducer::instance->maybe_fail(_label_); \
  }

/** Convenience macro which tests if a failure should be induced induced;
 * if yes then an error is thrown/returned or the application pauses/exits
 */
#define HT_MAYBE_FAIL_X(_label_, _exp_) \
  if (Hypertable::FailureInducer::enabled() && (_exp_)) { \
    Hypertable::FailureInducer::instance->maybe_fail(_label_); \
  }

/** Convenience macro for executing arbitrary code if a failure is induced */
#define HT_FAILURE_SIGNALLED(_label_) \
  Hypertable::FailureInducer::enabled() && \
    Hypertable::FailureInducer::instance->failure_signalled(_label_)

#endif // HYPERTABLE_FAILUREINDUCER_H
