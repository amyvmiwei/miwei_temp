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
 * Atomic Operations.
 * Atomic operations that C can't guarantee us. Useful i.e. for resource
 * counting. Operations contain atomic additions, substractions, tests etc.
 */

#ifndef HYPERTABLE_ATOMIC_H
#define HYPERTABLE_ATOMIC_H

/** @addtogroup Common
 *  @{
 */

#define LOCK "lock ; "

/**
 * An atomic type.
 * Internally, this is just a plain integer value. It's declared volatile
 * to avoid gcc optimizations: we need to use _exactly_ the address the user
 * gave us, not some alias that contains the same information.
 */
typedef struct {
  volatile int counter;
} atomic_t;

/**
 * Initialization macro for atomic variables; 
 * use like this:
 *
 *   atomic_t atom = ATOMIC_INIT(0);
 *   
 * @param i The initialization value
 */
#define ATOMIC_INIT(i)          { (i) }

/**
 * Atomically reads the value of v.
 *
 * @param v Pointer to an atomic_t variable
 * @return The integer value of the variable
 */
#define atomic_read(v)          ((v)->counter)

/**
 * Atomically sets the value of v to i.
 *
 * @param v Pointer to an atomic_t variable
 * @param i The new value of the variable
 */
#define atomic_set(v, i)        (((v)->counter) = (i))

/**
 * Adds an integer to an atomic variable
 *
 * @param i The integer value to add
 * @param v Pointer to an atomic_t variable
 */
static __inline__ void atomic_add(int i, atomic_t *v)
{
  __asm__ __volatile__(
          LOCK "addl %1,%0"
          :"=m" (v->counter)
          :"ir" (i), "m" (v->counter));
}

/**
 * Subtracts an integer from an atomic variable
 *
 * @param i The integer value to substract
 * @param v Pointer to an atomic_t variable
 */
static __inline__ void atomic_sub(int i, atomic_t *v)
{
  __asm__ __volatile__(
          LOCK "subl %1,%0"
          :"=m" (v->counter)
          :"ir" (i), "m" (v->counter));
}

/**
 * Atomically adds i to v and returns i + v
 *
 * @param i The integer value to add
 * @param v Pointer to an atomic_t variable
 * @return The sum of i and v
 */
static __inline__ int atomic_add_return(int i, atomic_t *v)
{
  int __i;
  /* Modern 486+ processor */
  __i = i;
  __asm__ __volatile__(
                LOCK "xaddl %0, %1"
                :"+r" (i), "+m" (v->counter)
                : : "memory");
  return i + __i;
}

/**
 * Atomically substracts i from v and returns v - i
 *
 * @param i The integer value to substract
 * @param v Pointer to an atomic_t variable
 * @return The sum of -i and v
 */
static __inline__ int atomic_sub_return(int i, atomic_t *v)
{
  return atomic_add_return(-i, v);
}

/**
 * Atomically subtracts i from v and returns true if the result is zero,
 * or false for all other cases.
 * 
 * @param i The integer value to subtract
 * @param v Pointer to an atomic_t variable
 * @return true if result is zero, otherwise false
 */
static __inline__ int atomic_sub_and_test(int i, atomic_t *v)
{
  unsigned char c;

  __asm__ __volatile__(
          LOCK "subl %2,%0; sete %1"
          :"=m" (v->counter), "=qm" (c)
          :"ir" (i), "m" (v->counter) : "memory");
  return c;
}

/**
 * Atomically increments v by 1.
 *
 * @param v Pointer to an atomic_t variable
 */
static __inline__ void atomic_inc(atomic_t *v)
{
  __asm__ __volatile__(
          LOCK "incl %0"
          :"=m" (v->counter)
          :"m" (v->counter));
}

/**
 * Atomically decrements v by 1.
 *
 * @param v Pointer to an atomic_t variable
 */
static __inline__ void atomic_dec(atomic_t *v)
{
  __asm__ __volatile__(
          LOCK "decl %0"
          :"=m" (v->counter)
          :"m" (v->counter));
}

/**
 * Atomically decrements v by 1 and returns true if the result is 0,
 * or false for all other cases.
 *
 * @param v Pointer to an atomic_t variable
 * @return true if result is zero, otherwise false
 */
static __inline__ int atomic_dec_and_test(atomic_t *v)
{
  unsigned char c;

  __asm__ __volatile__(
          LOCK "decl %0; sete %1"
          :"=m" (v->counter), "=qm" (c)
          :"m" (v->counter) : "memory");
  return c != 0;
}

/**
 * Atomically increments v by 1 and returns true if the result is zero,
 * or false for all other cases.
 *
 * @param v Pointer to an atomic_t variable
 * @return true if result is zero, otherwise false
 */
static __inline__ int atomic_inc_and_test(atomic_t *v)
{
  unsigned char c;

  __asm__ __volatile__(
          LOCK "incl %0; sete %1"
          :"=m" (v->counter), "=qm" (c)
          :"m" (v->counter) : "memory");
  return c != 0;
}

/**
 * Atomically adds i to v and returns true if the result is negative, or
 * false when result is greater than or equal to zero.
 *
 * @param i The value added to the atomic variable
 * @param v Pointer to an atomic_t variable
 * @return true if result is negative, otherwise false
 */
static __inline__ int atomic_add_negative(int i, atomic_t *v)
{
  unsigned char c;

  __asm__ __volatile__(
          LOCK "addl %2,%0; sets %1"
          :"=m" (v->counter), "=qm" (c)
          :"ir" (i), "m" (v->counter) : "memory");
  return c;
}

/**
 * Atomically increments v and returns the new value
 *
 * @param v Pointer to an atomic_t variable
 * @return The new, incremented value
 */
#define atomic_inc_return(v)  (atomic_add_return(1, v))

/**
 * Atomically decrements v and returns the new value
 *
 * @param v Pointer to an atomic_t variable
 * @return The new, decremented value
 */
#define atomic_dec_return(v)  (atomic_sub_return(1, v))

/* These are x86-specific, used by some header files */
#define atomic_clear_mask(mask, addr)                       \
                __asm__ __volatile__(LOCK "andl %0,%1"      \
                : : "r" (~(mask)),"m" (*addr) : "memory")

#define atomic_set_mask(mask, addr)                         \
                __asm__ __volatile__(LOCK "orl %0,%1"       \
                : : "r" (mask),"m" (*(addr)) : "memory")

/** @}*/

#endif // HYPERTABLE_ATOMIC_H

