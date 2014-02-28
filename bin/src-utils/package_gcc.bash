#!/bin/bash

######################################################################
#
# (C) Copyright 2014 Hypertable, Inc.
#
# File Name     : package_gcc.bash
# Version       : $Revision$
# Library       : 
# Project       : 
# Author        : John Gilson (Email: jag@acm.org)
# Others        : 
# Created       : 2014/02/15 12:22:34 Pacific Standard Time
# Last Modified : $Date$ by $Author$
# Description   : Bash script to download, unpack, configure, build,
#                 test and install GCC and any packages this depends
#                 upon
#
######################################################################

# External API functions
#
# This bash script is composed solely of functions arranged in a
# bottom-up fashion, that is, the external API functions described
# here are defined at the bottom of this file.
#
# This file should first be sourced:
#
# . ./package_gcc.bash
#
# To install from scratch the following packages in dependency order
# (see function definitions below for description of arguments)
#
# package_tcl [base directory pathname] [create release-specific directory?]
# package_expect [base directory pathname] [create release-specific directory?]
# package_dejagnu [base directory pathname] [create release-specific directory?]
# package_gcc [base directory pathname] [create release-specific directory?]
#
# Or to do all of this in one fell swoop:
#
# package_gcc_all [base directory pathname] [create release-specific directory?]
#
# The base directory pathname argument is optional and defaults to
# /usr/local.  The second argument is also optional and defaults to
# "false".
#
# Examples:
#
# $ package_gcc_all /opt true
#
# will build and install the latest version of GCC and all its
# dependencies under release-specific directories under /opt.
#
# $ package_gcc_all ~
#
# will build and install the latest version of GCC and all its
# dependencies under the user's home directory, NOT in
# release-specific directories.
#
# $ package_gcc_all
#
# will build and install the latest version of GCC and all its
# dependencies under /usr/local, NOT in release-specific directories.
#
# This script will build under 32-bit or 64-bit flavors of Linux.  A
# package will not be built if it is already installed.  The following
# variables could be set but need not be as they default to current
# latest versions:
#
# GCC_VERSION
# ISL_VERSION
# CLOOG_VERSION
# TCL_VERSION
# EXPECT_VERSION
# DEJAGNU_VERSION
#
# They are each set in one place in the code below.  For example, to
# install gcc 4.9 when it comes out, one would either change the value
# of the global variable GCC_VERSION set below in set_gcc_release_vars
# or do the following:
#
# . ./package_gcc.bash
# GCC_VERSION=4.9
#
# and then call the appropriate function, e.g., package_gcc_all.  Of
# course, one should also check to see if ISL_VERSION and
# CLOOG_VERSION need to be updated as well (see
# http://gcc.gnu.org/install/prerequisites.html) and those global
# variables could either be set in this script or externally prior to
# calling the appropriate build and install function.  The versions of
# ISL and CLooG that a particular version of GCC uses is not
# necessarily the latest versions of these libraries so one must
# consult the web page and set the global variables appropriately.
#
# To uninstall the following packages in dependency order (see
# function definitions below for description of arguments)
#
# unpackage_gcc [base directory pathname] [create release-specific directory?]
# unpackage_dejagnu  [base directory pathname]
#                    [create release-specific directory?]
# unpackage_expect [base directory pathname]
#                  [create release-specific directory?]
# unpackage_tcl [base directory pathname]
#               [create release-specific directory?]
#
# Or to do all of this in one fell swoop:
#
# unpackage_gcc_all [base directory pathname]
#                   [create release-specific directory?]

# Utilities

# pushd command with a descriptive message
function push_dir()
{
  if pushd $1
  then
    echo "cd to $1."
  else
    echo "Failed to cd to $1."
    exit 1
  fi
}

# If directory does not exist first make it, then pushd along with
# descriptive message
function push_dir_mk_if_needed()
{
  if [ ! -d $1 ]
  then
    if mkdir -p $1
    then
      echo "Directory $1 created."
    else
      echo "Directory $1 does not exist and cannot be created."
      exit 1
    fi
  fi
  push_dir $1
}

# popd command with a descriptive message
function pop_dir()
{
  local prev_dir
  prev_dir=$(dirs +1)
  if popd
  then
    echo "cd back to $prev_dir."
  else
    echo "Could not cd back to $prev_dir."
    exit 1
  fi
}

# Add directory to beginning of PATH env var if not currently in PATH
function path_cons()
{
  local d=$(readlink -m $1)
  if [ -d "$d" ] && [[ ":$PATH:" != *":$d:"* ]]
  then
    PATH="${PATH:+"$d:$PATH"}"
    echo "Added $d to front of PATH environment variable."
  fi
}

# Functions to set variables

# Set GCC release variables

function set_gcc_release_vars()
{
  GCC_NAME=gcc
  # The following variable specifies the GCC version
  GCC_VERSION=${GCC_VERSION:=4.8.2}
  # The following two variables specify versions of support libraries
  # used in building GCC.  These should be checked for any new version
  # of GCC at http://gcc.gnu.org/install/prerequisites.html.
  ISL_VERSION=${ISL_VERSION:=0.12.2}
  CLOOG_VERSION=${CLOOG_VERSION:=0.18.1}

  ISL=isl-$ISL_VERSION
  CLOOG=cloog-$CLOOG_VERSION

  GCC_RELEASE=$GCC_NAME-$GCC_VERSION
}

# Function sets other variables for the GCC package.  Function is
# passed two arguments, both required:
#
# 1. Base directory pathname, empty string if unprovided.  If
# unprovided then the base directory pathname defaults to /usr/local.
#
# 2. Release directory name, empty string if unprovided.  If
# unprovided then only the base directory pathname is used with no
# release directory within used.

function set_gcc_vars()
{
  local prefix
  if [ "$1" = "" ]
  then
    prefix=/usr/local
  else
    prefix=$(readlink -m $1)
  fi

  if [ -d $prefix ]
  then
    GCC_PREFIX_PATH=$prefix
    echo "Prefix path set to $prefix."
  else
    echo "Prefix path is not a directory: $prefix."
    exit 1
  fi

  if [ "$2" = "" ]
  then
    push_dir $GCC_PREFIX_PATH
  else
    GCC_PREFIX_PATH=$GCC_PREFIX_PATH/$2
    push_dir_mk_if_needed $GCC_PREFIX_PATH
    echo "Prefix path set to $GCC_PREFIX_PATH."
  fi

  GCC_SRC_DIR=src
  GCC_SRC_PATH=$GCC_PREFIX_PATH/$GCC_SRC_DIR
  GCC_INCLUDE_DIR=include
  GCC_INCLUDE_PATH=$GCC_PREFIX_PATH/$GCC_INCLUDE_DIR
  GCC_LIB_DIR=lib
  GCC_LIB_PATH=$GCC_PREFIX_PATH/$GCC_LIB_DIR
  GCC_BIN_DIR=bin
  GCC_BIN_PATH=$GCC_PREFIX_PATH/$GCC_BIN_DIR

  GCC_RELEASE_SRC_PATH=$GCC_SRC_PATH/$GCC_RELEASE
  GCC_TARBALL=$GCC_RELEASE.tar
  GCC_COMPRESSED_TARBALL=$GCC_TARBALL.bz2
  GCC_URL=ftp://ftp.gnu.org/gnu/$GCC_NAME/$GCC_RELEASE/$GCC_COMPRESSED_TARBALL
  GCC_BUILD_DIR=build-$GCC_RELEASE
  GCC_BUILD_PATH=$GCC_PREFIX_PATH/$GCC_BUILD_DIR

  pop_dir
}

# Set Tcl release variables

function set_tcl_release_vars()
{
  TCL_NAME=tcl
  # The following variable specifies the Tcl version
  TCL_VERSION=${TCL_VERSION:=8.6.1}

  TCL_MAJOR_VERSION=${TCL_VERSION%.*}

  TCL_RELEASE=$TCL_NAME$TCL_VERSION
}

# Function sets other variables for the Tcl package.  Function is
# passed two arguments, both required:
#
# 1. Base directory pathname, empty string if unprovided.  If
# unprovided then the base directory pathname defaults to /usr/local.
#
# 2. Release directory name, empty string if unprovided.  If
# unprovided then only the base directory pathname is used with no
# release directory within used.

function set_tcl_vars()
{
  local prefix
  if [ "$1" = "" ]
  then
    prefix=/usr/local
  else
    prefix=$(readlink -m $1)
  fi

  if [ -d $prefix ]
  then
    TCL_PREFIX_PATH=$prefix
    echo "Prefix path set to $prefix."
  else
    echo "Prefix path is not a directory: $prefix."
    exit 1
  fi

  if [ "$2" = "" ]
  then
    push_dir $TCL_PREFIX_PATH
  else
    TCL_PREFIX_PATH=$TCL_PREFIX_PATH/$2
    push_dir_mk_if_needed $TCL_PREFIX_PATH
    echo "Prefix path set to $TCL_PREFIX_PATH."
  fi

  TCL_SRC_DIR=src
  TCL_SRC_PATH=$TCL_PREFIX_PATH/$TCL_SRC_DIR
  TCL_INCLUDE_DIR=include
  TCL_INCLUDE_PATH=$TCL_PREFIX_PATH/$TCL_INCLUDE_DIR
  TCL_LIB_DIR=lib
  TCL_LIB_PATH=$TCL_PREFIX_PATH/$TCL_LIB_DIR
  TCL_BIN_DIR=bin
  TCL_BIN_PATH=$TCL_PREFIX_PATH/$TCL_BIN_DIR

  TCL_RELEASE_SRC_PATH=$TCL_SRC_PATH/$TCL_RELEASE
  TCL_TARBALL=${TCL_RELEASE}-src.tar
  TCL_COMPRESSED_TARBALL=${TCL_TARBALL}.gz
  TCL_URL=http://prdownloads.sourceforge.net/$TCL_NAME/$TCL_COMPRESSED_TARBALL
  TCL_BUILD_DIR=unix
  TCL_BUILD_PATH=$TCL_RELEASE_SRC_PATH/$TCL_BUILD_DIR

  pop_dir
}

# Set Expect release variables

function set_expect_release_vars()
{
  EXPECT_NAME=expect
  # The following variable specifies the Expect version
  EXPECT_VERSION=${EXPECT_VERSION:=5.45}

  EXPECT_RELEASE=$EXPECT_NAME$EXPECT_VERSION
}

# Function sets other variables for the Expect package.  Function is
# passed two arguments, both required:
#
# 1. Base directory pathname, empty string if unprovided.  If
# unprovided then the base directory pathname defaults to /usr/local.
#
# 2. Release directory name, empty string if unprovided.  If
# unprovided then only the base directory pathname is used with no
# release directory within used.

function set_expect_vars()
{
  local prefix
  if [ "$1" = "" ]
  then
    prefix=/usr/local
  else
    prefix=$(readlink -m $1)
  fi

  if [ -d $prefix ]
  then
    EXPECT_PREFIX_PATH=$prefix
    echo "Prefix path set to $prefix."
  else
    echo "Prefix path is not a directory: $prefix."
    exit 1
  fi

  if [ "$2" = "" ]
  then
    push_dir $EXPECT_PREFIX_PATH
  else
    EXPECT_PREFIX_PATH=$EXPECT_PREFIX_PATH/$2
    push_dir_mk_if_needed $EXPECT_PREFIX_PATH
    echo "Prefix path set to $EXPECT_PREFIX_PATH."
  fi

  EXPECT_SRC_DIR=src
  EXPECT_SRC_PATH=$EXPECT_PREFIX_PATH/$EXPECT_SRC_DIR
  EXPECT_INCLUDE_DIR=include
  EXPECT_INCLUDE_PATH=$EXPECT_PREFIX_PATH/$EXPECT_INCLUDE_DIR
  EXPECT_LIB_DIR=lib
  EXPECT_LIB_PATH=$EXPECT_PREFIX_PATH/$EXPECT_LIB_DIR
  EXPECT_BIN_DIR=bin
  EXPECT_BIN_PATH=$EXPECT_PREFIX_PATH/$EXPECT_BIN_DIR

  EXPECT_RELEASE_SRC_PATH=$EXPECT_SRC_PATH/$EXPECT_RELEASE
  EXPECT_TARBALL=${EXPECT_RELEASE}.tar
  EXPECT_COMPRESSED_TARBALL=${EXPECT_TARBALL}.gz
  EXPECT_URL=http://sourceforge.net/projects/expect/files/Expect/$EXPECT_VERSION/$EXPECT_COMPRESSED_TARBALL
  EXPECT_BUILD_PATH=$EXPECT_RELEASE_SRC_PATH

  pop_dir
}

# Set DejaGnu release variables

function set_dejagnu_release_vars()
{
  DEJAGNU_NAME=dejagnu
  # The following variable specifies the DejaGnu version
  DEJAGNU_VERSION=${DEJAGNU_VERSION:=1.5.1}

  DEJAGNU_RELEASE=$DEJAGNU_NAME-$DEJAGNU_VERSION
}

# Function sets other variables for the DejaGnu package.  Function is
# passed two arguments, both required:
#
# 1. Base directory pathname, empty string if unprovided.  If
# unprovided then the base directory pathname defaults to /usr/local.
#
# 2. Release directory name, empty string if unprovided.  If
# unprovided then only the base directory pathname is used with no
# release directory within used.

function set_dejagnu_vars()
{
  local prefix
  if [ "$1" = "" ]
  then
    prefix=/usr/local
  else
    prefix=$(readlink -m $1)
  fi

  if [ -d $prefix ]
  then
    DEJAGNU_PREFIX_PATH=$prefix
    echo "Prefix path set to $prefix."
  else
    echo "Prefix path is not a directory: $prefix."
    exit 1
  fi

  if [ "$2" = "" ]
  then
    push_dir $DEJAGNU_PREFIX_PATH
  else
    DEJAGNU_PREFIX_PATH=$DEJAGNU_PREFIX_PATH/$2
    push_dir_mk_if_needed $DEJAGNU_PREFIX_PATH
    echo "Prefix path set to $DEJAGNU_PREFIX_PATH."
  fi

  DEJAGNU_SRC_DIR=src
  DEJAGNU_SRC_PATH=$DEJAGNU_PREFIX_PATH/$DEJAGNU_SRC_DIR
  DEJAGNU_INCLUDE_DIR=include
  DEJAGNU_INCLUDE_PATH=$DEJAGNU_PREFIX_PATH/$DEJAGNU_INCLUDE_DIR
  DEJAGNU_LIB_DIR=lib
  DEJAGNU_LIB_PATH=$DEJAGNU_PREFIX_PATH/$DEJAGNU_LIB_DIR
  DEJAGNU_BIN_DIR=bin
  DEJAGNU_BIN_PATH=$DEJAGNU_PREFIX_PATH/$DEJAGNU_BIN_DIR

  DEJAGNU_RELEASE_SRC_PATH=$DEJAGNU_SRC_PATH/$DEJAGNU_RELEASE
  DEJAGNU_TARBALL=${DEJAGNU_RELEASE}.tar
  DEJAGNU_COMPRESSED_TARBALL=${DEJAGNU_TARBALL}.gz
  DEJAGNU_URL=ftp://ftp.gnu.org/gnu/$DEJAGNU_NAME/$DEJAGNU_COMPRESSED_TARBALL
  DEJAGNU_BUILD_PATH=$DEJAGNU_RELEASE_SRC_PATH

  pop_dir
}

######################################################################
#
# Functions for GCC
#
######################################################################

# Download GCC tarball
function get_gcc()
{
  push_dir_mk_if_needed $GCC_SRC_PATH
  if [ -f $GCC_COMPRESSED_TARBALL ]
  then
    echo "$GCC_COMPRESSED_TARBALL compressed tarball already exists."
    pop_dir
    return 0
  elif [ -f $GCC_TARBALL ]
  then
    echo "$GCC_TARBALL tarball already exists."
    pop_dir
    return 0
  else
    echo "Downloading GCC $GCC_VERSION compressed tarball..."
    if wget $GCC_URL
    then
      echo "$GCC_COMPRESSED_TARBALL downloaded."
      pop_dir
      return 0
    else
      echo "Failed download of GCC $GCC_VERSION compressed tarball."
      pop_dir
      exit 1
    fi
  fi
}

# Undo the download of the GCC tarball
function undo_get_gcc()
{
  push_dir $GCC_SRC_PATH
  if [ -f $GCC_COMPRESSED_TARBALL ]
  then
    if rm -f $GCC_COMPRESSED_TARBALL
    then
      echo "$GCC_COMPRESSED_TARBALL compressed tarball deleted."
      pop_dir
    else
      echo "$GCC_COMPRESSED_TARBALL compressed tarball not deleted."
      pop_dir
      exit 1
    fi
  elif [ -f $GCC_TARBALL ]
  then
    if rm -f $GCC_TARBALL
    then
      echo "$GCC_TARBALL tarball deleted."
      pop_dir
    else
      echo "$GCC_TARBALL tarball not deleted."
      pop_dir
      exit 1
    fi
  else
    echo "GCC $GCC_VERSION tarball does not exist."
    pop_dir
  fi
}

# Unpack the GCC tarball
function unpack_gcc()
{
  push_dir $GCC_SRC_PATH
  if [ -d $GCC_RELEASE ]
  then
    echo "GCC $GCC_VERSION already unpacked."
    pop_dir
    return 0
  fi

  if [ -f $GCC_COMPRESSED_TARBALL ]
  then
    echo "Uncompress and extract $GCC_COMPRESSED_TARBALL..."
    if tar xjf $GCC_COMPRESSED_TARBALL
    then
      echo "$GCC_COMPRESSED_TARBALL uncompressed and extracted into" \
           "the directory $GCC_RELEASE_SRC_PATH."
      pop_dir
    else
      echo "Uncompression and extraction of $GCC_COMPRESSED_TARBALL failed."
      pop_dir
      exit 1
    fi
  elif [ -f $GCC_TARBALL ]
  then
    echo "Extract $GCC_TARBALL..."
    if tar xf $GCC_TARBALL
    then
      echo "$GCC_TARBALL extracted into the directory $GCC_RELEASE_SRC_PATH."
      pop_dir
    else
      echo "Extraction of $GCC_TARBALL failed."
      pop_dir
      exit 1
    fi
  else
    echo "GCC $GCC_VERSION tarball not present."
    pop_dir
    exit 1
  fi
}

# Undo the unpacking of the GCC tarball
function undo_unpack_gcc()
{
  push_dir $GCC_SRC_PATH
  if [ -d $GCC_RELEASE ]
  then
    if rm -rf $GCC_RELEASE
    then
      echo "$GCC_RELEASE_SRC_PATH directory deleted."
      pop_dir
      return 0
    else
      echo "$GCC_RELEASE_SRC_PATH directory not deleted."
      pop_dir
      exit 1
    fi
  else
    echo "$GCC_RELEASE_SRC_PATH directory does not exist."
    pop_dir
  fi
}

# Download and unpack libs needed to build GCC
function get_gcc_support_libs()
{
  push_dir $GCC_RELEASE_SRC_PATH
  if [ -L mpfr ] && [ -L gmp ] && [ -L mpc ]
  then
    echo "GCC $GCC_VERSION support libs MPFR, GMP and MPC already present."
    pop_dir
    return 0
  fi
  if ./contrib/download_prerequisites
  then
    echo "GCC $GCC_VERSION support libs MPFR, GMP and MPC successfully" \
         "downloaded and unpacked to $GCC_RELEASE_SRC_PATH."
  else
    echo "GCC $GCC_VERSION support libs MPFR, GMP and MPC unsuccessfully" \
         "downloaded and unpacked to $GCC_RELEASE_SRC_PATH."
    pop_dir
    exit 1
  fi

  # These libs are used to build gcc with Graphite loop optimizations.
  # They are not required to build gcc.
  if [ -L isl ] && [ -L cloog ]
  then
    echo "GCC $GCC_VERSION support libs ISL and CLooG already present."
    pop_dir
    return 0
  fi

  if [ ! -L isl ]
  then
    if wget ftp://gcc.gnu.org/pub/gcc/infrastructure/$ISL.tar.bz2
    then
      echo "$ISL.tar.bz2 GCC $GCC_VERSION support lib successfully downloaded."
      if tar xjf $ISL.tar.bz2
      then
        echo "$ISL.tar.bz2 GCC $GCC_VERSION support lib successfully unpacked."
        if ln -sf $ISL isl
        then
          echo "ISL support lib available to build GCC $GCC_VERSION."
        else
          echo "ISL support lib unavailable to build GCC $GCC_VERSION."
          pop_dir
          return 0
        fi
      else
        echo "$ISL.tar.bz2 support lib unsuccessfully unpacked and therefore" \
             "unavailable to build GCC $GCC_VERSION."
        pop_dir
        return 0
      fi
    else
      echo "$ISL.tar.bz2 unsuccessfully downloaded and therefore unavailable" \
           "to build GCC $GCC_VERSION."
      pop_dir
      return 0
    fi
  else
    echo "GCC $GCC_VERSION ISL support lib already present."
  fi

  if [ ! -L cloog ]
  then
    if wget ftp://gcc.gnu.org/pub/gcc/infrastructure/$CLOOG.tar.gz
    then
      echo "$CLOOG.tar.gz GCC $GCC_VERSION support lib successfully" \
           "downloaded."
      if tar xzf $CLOOG.tar.gz
      then
        echo "$CLOOG.tar.gz GCC $GCC_VERSION support lib successfully" \
             "unpacked."
        if ln -sf $CLOOG cloog
        then
          echo "CLooG support lib available to build GCC $GCC_VERSION."
        else
          echo "CLooG support lib unavailable to build GCC $GCC_VERSION."
          if [ -L isl ]
          then
            rm -f isl
          fi
          pop_dir
          return 0
        fi
      else
        echo "$CLOOG.tar.gz support lib unsuccessfully unpacked and" \
             "therefore unavailable to build GCC $GCC_VERSION."
        if [ -L isl ]
        then
          rm -f isl
        fi
        pop_dir
        return 0
      fi
    else
      echo "$CLOOG.tar.gz unsuccessfully downloaded and therefore" \
           "unavailable to build GCC $GCC_VERSION."
      if [ -L isl ]
      then
        rm -f isl
      fi
      pop_dir
      return 0
    fi
  else
    echo "GCC $GCC_VERSION CLooG support lib already present."
  fi
  pop_dir
  return 0
}

# Delete libs needed to build GCC
function undo_get_gcc_support_libs
{
  push_dir $GCC_RELEASE_SRC_PATH
  if [ -L mpfr ]
  then
    if rm -f mpfr
    then
      echo "$GCC_RELEASE_SRC_PATH/mpfr deleted."
    else
      echo "$GCC_RELEASE_SRC_PATH/mpfr not deleted."
    fi
  else
    echo "$GCC_RELEASE_SRC_PATH/mpfr does not exist."
  fi

  if [ -L gmp ]
  then
    if rm -f gmp
    then
      echo "$GCC_RELEASE_SRC_PATH/gmp deleted."
    else
      echo "$GCC_RELEASE_SRC_PATH/gmp not deleted."
    fi
  else
    echo "$GCC_RELEASE_SRC_PATH/gmp does not exist."
  fi

  if [ -L mpc ]
  then
    if rm -f mpc
    then
      echo "$GCC_RELEASE_SRC_PATH/mpc deleted."
    else
      echo "$GCC_RELEASE_SRC_PATH/mpc not deleted."
    fi
  else
    echo "$GCC_RELEASE_SRC_PATH/mpc does not exist."
  fi

  if [ -L isl ]
  then
    if rm -f isl
    then
      echo "$GCC_RELEASE_SRC_PATH/isl deleted."
    else
      echo "$GCC_RELEASE_SRC_PATH/isl not deleted."
    fi
  else
    echo "$GCC_RELEASE_SRC_PATH/isl does not exist."
  fi

  if [ -L cloog ]
  then
    if rm -f cloog
    then
      echo "$GCC_RELEASE_SRC_PATH/cloog deleted."
    else
      echo "$GCC_RELEASE_SRC_PATH/cloog not deleted."
    fi
  else
    echo "$GCC_RELEASE_SRC_PATH/cloog does not exist."
  fi

  pop_dir
}

# Make the directory that GCC will be built in
function make_gcc_build_dir()
{
  if [ -d $GCC_BUILD_PATH ]
  then
    echo "Directory $GCC_BUILD_PATH exists to build GCC $GCC_VERSION within."
  elif mkdir -p $GCC_BUILD_PATH
  then
    echo "Make directory $GCC_BUILD_PATH to build GCC $GCC_VERSION within."
  else
    echo "Build directory $GCC_BUILD_PATH to build GCC $GCC_VERSION within" \
         "does not exist and cannot be created."
    exit 1;
  fi
}

# Delete GCC build directory
function undo_make_gcc_build_dir()
{
  if [ -d $GCC_BUILD_PATH ]
  then
    if rm -rf $GCC_BUILD_PATH
    then
      echo "Directory $GCC_BUILD_PATH deleted."
    else
      echo "Directory $GCC_BUILD_PATH could not be deleted."
      exit 1
    fi
  else
    echo "Directory $GCC_BUILD_PATH does not exist."
  fi
}

# Configure build of GCC
function configure_gcc()
{
  push_dir $GCC_BUILD_PATH
  if [ -d $GCC_RELEASE_SRC_PATH ]
  then
    echo "Begin configuration of build of GCC $GCC_VERSION..."
    if $GCC_RELEASE_SRC_PATH/configure -v --prefix=$GCC_PREFIX_PATH \
       --enable-bootstrap \
       --enable-version-specific-runtime-libs --enable-shared \
       --enable-__cxa_atexit --enable-languages=c,c++ \
       --enable-threads=posix --with-tune=generic \
       --enable-checking=release --with-system-zlib \
       --disable-isl-version-check \
       --disable-multilib --enable-clocale=gnu --enable-lto --enable-plugin
    then
      echo "Configuration of build of GCC $GCC_VERSION successful."
      pop_dir
      return 0
    else
      echo "Configuration of build of GCC $GCC_VERSION unsuccessful."
      pop_dir
      exit 1
    fi
  else
    echo "Directory $GCC_RELEASE_SRC_PATH with GCC $GCC_VERSION source does not exist" \
         "so configuration of build cannot occur."
    pop_dir
    exit 1
  fi
}

# Build GCC
function build_gcc()
{
  push_dir $GCC_BUILD_PATH
  if [ -d $GCC_RELEASE_SRC_PATH ]
  then
    echo "Begin building GCC $GCC_VERSION..."
    if make -j $(expr 2 \* $(nproc))
    then
      echo "GCC $GCC_VERSION successfully built."
      pop_dir
      return 0
    else
      echo "GCC $GCC_VERSION unsuccessfully built."
      pop_dir
      exit 1
    fi
  else
    echo "Directory $GCC_RELEASE_SRC_PATH with GCC $GCC_VERSION source does not exist" \
         "so build cannot occur."
    pop_dir
    exit 1
  fi
}

######################################################################
#
# Ensure that Tcl is installed to run GCC tests
#
######################################################################

function get_tcl()
{
  push_dir_mk_if_needed $TCL_SRC_PATH
  if [ -f $TCL_COMPRESSED_TARBALL ]
  then
    echo "$TCL_COMPRESSED_TARBALL compressed tarball already exists."
    pop_dir
    return 0
  elif [ -f $TCL_TARBALL ]
  then
    echo "$TCL_TARBALL tarball already exists."
    pop_dir
    return 0
  else
    echo "Downloading Tcl $TCL_VERSION compressed tarball..."
    if wget $TCL_URL
    then
      echo "$TCL_COMPRESSED_TARBALL downloaded."
      pop_dir
      return 0
    else
      echo "Failed download of Tcl $TCL_VERSION compressed tarball."
      pop_dir
      exit 1
    fi
  fi
}

# Undo the download of the Tcl tarball
function undo_get_tcl()
{
  push_dir $TCL_SRC_PATH
  if [ -f $TCL_COMPRESSED_TARBALL ]
  then
    if rm -f $TCL_COMPRESSED_TARBALL
    then
      echo "$TCL_COMPRESSED_TARBALL compressed tarball deleted."
      pop_dir
    else
      echo "$TCL_COMPRESSED_TARBALL compressed tarball not deleted."
      pop_dir
      exit 1
    fi
  elif [ -f $TCL_TARBALL ]
  then
    if rm -f $TCL_TARBALL
    then
      echo "$TCL_TARBALL tarball deleted."
      pop_dir
    else
      echo "$TCL_TARBALL tarball not deleted."
      pop_dir
      exit 1
    fi
  else
    echo "Tcl $TCL_VERSION tarball does not exist."
    pop_dir
  fi
}

# Unpack the Tcl tarball
function unpack_tcl()
{
  push_dir $TCL_SRC_PATH
  if [ -d $TCL_RELEASE ]
  then
    echo "Tcl $TCL_VERSION already unpacked."
    pop_dir
    return 0
  fi

  if [ -f $TCL_COMPRESSED_TARBALL ]
  then
    echo "Uncompress and extract $TCL_COMPRESSED_TARBALL..."
    if tar xzf $TCL_COMPRESSED_TARBALL
    then
      echo "$TCL_COMPRESSED_TARBALL uncompressed and extracted into" \
           "the directory $TCL_RELEASE_SRC_PATH."
      pop_dir
    else
      echo "Uncompression and extraction of $TCL_COMPRESSED_TARBALL failed."
      pop_dir
      exit 1
    fi
  elif [ -f $TCL_TARBALL ]
  then
    echo "Extract $TCL_TARBALL..."
    if tar xf $TCL_TARBALL
    then
      echo "$TCL_TARBALL extracted into the directory $TCL_RELEASE_SRC_PATH."
      pop_dir
    else
      echo "Extraction of $TCL_TARBALL failed."
      pop_dir
      exit 1
    fi
  else
    echo "Tcl $TCL_VERSION tarball not present."
    pop_dir
    exit 1
  fi
}

# Undo the unpacking of the Tcl tarball
function undo_unpack_tcl()
{
  push_dir $TCL_SRC_PATH
  if [ -d $TCL_RELEASE ]
  then
    if rm -rf $TCL_RELEASE
    then
      echo "$TCL_RELEASE_SRC_PATH directory deleted."
      pop_dir
      return 0
    else
      echo "$TCL_RELEASE_SRC_PATH directory not deleted."
      pop_dir
      exit 1
    fi
  else
    echo "$TCL_RELEASE_SRC_PATH directory does not exist."
    pop_dir
  fi
}

# Configure build of Tcl
function configure_tcl()
{
  push_dir $TCL_BUILD_PATH
  echo "Begin configuration of build of Tcl $TCL_VERSION..."
  local word_size=$(getconf LONG_BIT)
  if [ $word_size -eq 32 ]
  then
    if ./configure -v --prefix=$TCL_PREFIX_PATH
    then
      echo "Configuration of build of 32-bit Tcl $TCL_VERSION successful."
      pop_dir
      return 0
    else
      echo "Configuration of build of 32-bit Tcl $TCL_VERSION unsuccessful."
      pop_dir
      exit 1
    fi
  elif [ $word_size -eq 64 ]
  then
    if ./configure -v --prefix=$TCL_PREFIX_PATH --enable-64bit
    then
      echo "Configuration of build of 64-bit Tcl $TCL_VERSION successful."
      pop_dir
      return 0
    else
      echo "Configuration of build of 64-bit Tcl $TCL_VERSION unsuccessful."
      pop_dir
      exit 1
    fi
  else
    echo "Configuration of build of Tcl $TCL_VERSION unsuccessful because word size of $word_size bits not handled."
    pop_dir
    exit 1
  fi
}

# Build Tcl
function build_tcl()
{
  push_dir $TCL_BUILD_PATH
  echo "Begin building Tcl $TCL_VERSION..."
  if make -j $(expr 2 \* $(nproc))
  then
    echo "Tcl $TCL_VERSION successfully built."
    pop_dir
    return 0
  else
    echo "Tcl $TCL_VERSION unsuccessfully built."
    pop_dir
    exit 1
  fi
}

# Test Tcl
function test_tcl()
{
  push_dir $TCL_BUILD_PATH
  echo "Begin running tests for Tcl $TCL_VERSION..."
  if make -j $(expr 2 \* $(nproc)) test
  then
    echo "Tests for Tcl $TCL_VERSION successfully completed."
    pop_dir
    return 0
  else
    echo "Tests for Tcl $TCL_VERSION did not all successfully complete."
    pop_dir
    exit 1
  fi
}

# Install Tcl
function install_tcl()
{
  push_dir $TCL_BUILD_PATH
  echo "Begin installation of Tcl $TCL_VERSION..."
  if make -j $(expr 2 \* $(nproc)) install
  then
    path_cons $TCL_BIN_PATH
    ln -sf $TCL_BIN_PATH/tclsh${TCL_MAJOR_VERSION} $TCL_BIN_PATH/tclsh
    echo "Successful installation of Tcl $TCL_VERSION."
    pop_dir
    return 0
  else
    echo "Unsuccessful installation of Tcl $TCL_VERSION."
    pop_dir
    exit 1
  fi
}

######################################################################

######################################################################
#
# Ensure that Expect is installed to run GCC tests
#
######################################################################

function get_expect()
{
  push_dir_mk_if_needed $EXPECT_SRC_PATH
  if [ -f $EXPECT_COMPRESSED_TARBALL ]
  then
    echo "$EXPECT_COMPRESSED_TARBALL compressed tarball already exists."
    pop_dir
    return 0
  elif [ -f $EXPECT_TARBALL ]
  then
    echo "$EXPECT_TARBALL tarball already exists."
    pop_dir
    return 0
  else
    echo "Downloading Expect $EXPECT_VERSION compressed tarball..."
    if wget $EXPECT_URL
    then
      echo "$EXPECT_COMPRESSED_TARBALL downloaded."
      pop_dir
      return 0
    else
      echo "Failed download of Expect $EXPECT_VERSION compressed tarball."
      pop_dir
      exit 1
    fi
  fi
}

# Undo the download of the Expect tarball
function undo_get_expect()
{
  push_dir $EXPECT_SRC_PATH
  if [ -f $EXPECT_COMPRESSED_TARBALL ]
  then
    if rm -f $EXPECT_COMPRESSED_TARBALL
    then
      echo "$EXPECT_COMPRESSED_TARBALL compressed tarball deleted."
      pop_dir
    else
      echo "$EXPECT_COMPRESSED_TARBALL compressed tarball not deleted."
      pop_dir
      exit 1
    fi
  elif [ -f $EXPECT_TARBALL ]
  then
    if rm -f $EXPECT_TARBALL
    then
      echo "$EXPECT_TARBALL tarball deleted."
      pop_dir
    else
      echo "$EXPECT_TARBALL tarball not deleted."
      pop_dir
      exit 1
    fi
  else
    echo "Expect $EXPECT_VERSION tarball does not exist."
    pop_dir
  fi
}

# Unpack the Expect tarball
function unpack_expect()
{
  push_dir $EXPECT_SRC_PATH
  if [ -d $EXPECT_RELEASE ]
  then
    echo "Expect $EXPECT_VERSION already unpacked."
    pop_dir
    return 0
  fi

  if [ -f $EXPECT_COMPRESSED_TARBALL ]
  then
    echo "Uncompress and extract $EXPECT_COMPRESSED_TARBALL..."
    if tar xzf $EXPECT_COMPRESSED_TARBALL
    then
      echo "$EXPECT_COMPRESSED_TARBALL uncompressed and extracted into" \
           "the directory $EXPECT_RELEASE_SRC_PATH."
      pop_dir
    else
      echo "Uncompression and extraction of $EXPECT_COMPRESSED_TARBALL failed."
      pop_dir
      exit 1
    fi
  elif [ -f $EXPECT_TARBALL ]
  then
    echo "Extract $EXPECT_TARBALL..."
    if tar xf $EXPECT_TARBALL
    then
      echo "$EXPECT_TARBALL extracted into the directory $EXPECT_RELEASE_SRC_PATH."
      pop_dir
    else
      echo "Extraction of $EXPECT_TARBALL failed."
      pop_dir
      exit 1
    fi
  else
    echo "Expect $EXPECT_VERSION tarball not present."
    pop_dir
    exit 1
  fi
}

# Undo the unpacking of the Expect tarball
function undo_unpack_expect()
{
  push_dir $EXPECT_SRC_PATH
  if [ -d $EXPECT_RELEASE ]
  then
    if rm -rf $EXPECT_RELEASE
    then
      echo "$EXPECT_RELEASE_SRC_PATH directory deleted."
      pop_dir
      return 0
    else
      echo "$EXPECT_RELEASE_SRC_PATH directory not deleted."
      pop_dir
      exit 1
    fi
  else
    echo "$EXPECT_RELEASE_SRC_PATH directory does not exist."
    pop_dir
  fi
}

# Configure build of Expect
function configure_expect()
{
  push_dir $EXPECT_BUILD_PATH
  echo "Begin configuration of build of Expect $EXPECT_VERSION..."
  local word_size=$(getconf LONG_BIT)
  if [ $word_size -eq 32 ]
  then
    if ./configure -v --prefix=$EXPECT_PREFIX_PATH --enable-threads \
       --with-tcl=$TCL_LIB_PATH --with-tclinclude=$TCL_INCLUDE_PATH
    then
      echo "Configuration of build of 32-bit Expect $EXPECT_VERSION successful."
      pop_dir
      return 0
    else
      echo "Configuration of build of 32-bit Expect $EXPECT_VERSION unsuccessful."
      pop_dir
      exit 1
    fi
  elif [ $word_size -eq 64 ]
  then
    if ./configure -v --prefix=$EXPECT_PREFIX_PATH --enable-threads \
       --enable-64bit --with-tcl=$TCL_LIB_PATH --with-tclinclude=$TCL_INCLUDE_PATH
    then
      echo "Configuration of build of 64-bit Expect $EXPECT_VERSION successful."
      pop_dir
      return 0
    else
      echo "Configuration of build of 64-bit Expect $EXPECT_VERSION unsuccessful."
      pop_dir
      exit 1
    fi
  else
    echo "Configuration of build of Expect $EXPECT_VERSION unsuccessful because word size of $word_size bits not handled."
    pop_dir
    exit 1
  fi
}

# Build Expect
function build_expect()
{
  push_dir $EXPECT_BUILD_PATH
  echo "Begin building Expect $EXPECT_VERSION..."
  if make -j $(expr 2 \* $(nproc))
  then
    echo "Expect $EXPECT_VERSION successfully built."
    pop_dir
    return 0
  else
    echo "Expect $EXPECT_VERSION unsuccessfully built."
    pop_dir
    exit 1
  fi
}

# Test Expect
function test_expect()
{
  push_dir $EXPECT_BUILD_PATH
  echo "Begin running tests for Expect $EXPECT_VERSION..."
  if make -j $(expr 2 \* $(nproc)) test
  then
    echo "Tests for Expect $EXPECT_VERSION successfully completed."
    pop_dir
    return 0
  else
    echo "Tests for Expect $EXPECT_VERSION did not all successfully complete."
    pop_dir
    exit 1
  fi
}

# Install Expect
function install_expect()
{
  push_dir $EXPECT_BUILD_PATH
  echo "Begin installation of Expect $EXPECT_VERSION..."
  if make -j $(expr 2 \* $(nproc)) install
  then
    path_cons $EXPECT_BIN_PATH
    echo "Successful installation of Expect $EXPECT_VERSION."
    pop_dir
    return 0
  else
    echo "Unsuccessful installation of Expect $EXPECT_VERSION."
    pop_dir
    exit 1
  fi
}

######################################################################

######################################################################
#
# Ensure that DejaGnu is installed to run GCC tests
#
######################################################################

function get_dejagnu()
{
  push_dir_mk_if_needed $DEJAGNU_SRC_PATH
  if [ -f $DEJAGNU_COMPRESSED_TARBALL ]
  then
    echo "$DEJAGNU_COMPRESSED_TARBALL compressed tarball already exists."
    pop_dir
    return 0
  elif [ -f $DEJAGNU_TARBALL ]
  then
    echo "$DEJAGNU_TARBALL tarball already exists."
    pop_dir
    return 0
  else
    echo "Downloading DejaGnu $DEJAGNU_VERSION compressed tarball..."
    if wget $DEJAGNU_URL
    then
      echo "$DEJAGNU_COMPRESSED_TARBALL downloaded."
      pop_dir
      return 0
    else
      echo "Failed download of DejaGnu $DEJAGNU_VERSION compressed tarball."
      pop_dir
      exit 1
    fi
  fi
}

# Undo the download of the DejaGnu tarball
function undo_get_dejagnu()
{
  push_dir $DEJAGNU_SRC_PATH
  if [ -f $DEJAGNU_COMPRESSED_TARBALL ]
  then
    if rm -f $DEJAGNU_COMPRESSED_TARBALL
    then
      echo "$DEJAGNU_COMPRESSED_TARBALL compressed tarball deleted."
      pop_dir
    else
      echo "$DEJAGNU_COMPRESSED_TARBALL compressed tarball not deleted."
      pop_dir
      exit 1
    fi
  elif [ -f $DEJAGNU_TARBALL ]
  then
    if rm -f $DEJAGNU_TARBALL
    then
      echo "$DEJAGNU_TARBALL tarball deleted."
      pop_dir
    else
      echo "$DEJAGNU_TARBALL tarball not deleted."
      pop_dir
      exit 1
    fi
  else
    echo "DejaGnu $DEJAGNU_VERSION tarball does not exist."
    pop_dir
  fi
}

# Unpack the DejaGnu tarball
function unpack_dejagnu()
{
  push_dir $DEJAGNU_SRC_PATH
  if [ -d $DEJAGNU_RELEASE ]
  then
    echo "DejaGnu $DEJAGNU_VERSION already unpacked."
    pop_dir
    return 0
  fi

  if [ -f $DEJAGNU_COMPRESSED_TARBALL ]
  then
    echo "Uncompress and extract $DEJAGNU_COMPRESSED_TARBALL..."
    if tar xzf $DEJAGNU_COMPRESSED_TARBALL
    then
      echo "$DEJAGNU_COMPRESSED_TARBALL uncompressed and extracted into" \
           "the directory $DEJAGNU_RELEASE_SRC_PATH."
      pop_dir
    else
      echo "Uncompression and extraction of $DEJAGNU_COMPRESSED_TARBALL failed."
      pop_dir
      exit 1
    fi
  elif [ -f $DEJAGNU_TARBALL ]
  then
    echo "Extract $DEJAGNU_TARBALL..."
    if tar xf $DEJAGNU_TARBALL
    then
      echo "$DEJAGNU_TARBALL extracted into the directory $DEJAGNU_RELEASE_SRC_PATH."
      pop_dir
    else
      echo "Extraction of $DEJAGNU_TARBALL failed."
      pop_dir
      exit 1
    fi
  else
    echo "DejaGnu $DEJAGNU_VERSION tarball not present."
    pop_dir
    exit 1
  fi
}

# Undo the unpacking of the DejaGnu tarball
function undo_unpack_dejagnu()
{
  push_dir $DEJAGNU_SRC_PATH
  if [ -d $DEJAGNU_RELEASE ]
  then
    if rm -rf $DEJAGNU_RELEASE
    then
      echo "$DEJAGNU_RELEASE_SRC_PATH directory deleted."
      pop_dir
      return 0
    else
      echo "$DEJAGNU_RELEASE_SRC_PATH directory not deleted."
      pop_dir
      exit 1
    fi
  else
    echo "$DEJAGNU_RELEASE_SRC_PATH directory does not exist."
    pop_dir
  fi
}

# Configure build of DejaGnu
function configure_dejagnu()
{
  push_dir $DEJAGNU_BUILD_PATH
  echo "Begin configuration of build of DejaGnu $DEJAGNU_VERSION..."
  if ./configure -v --prefix=$DEJAGNU_PREFIX_PATH
  then
    echo "Configuration of build of DejaGnu $DEJAGNU_VERSION successful."
    pop_dir
    return 0
  else
    echo "Configuration of build of DejaGnu $DEJAGNU_VERSION unsuccessful."
    pop_dir
    exit 1
  fi
}

# Build DejaGnu
function build_dejagnu()
{
  push_dir $DEJAGNU_BUILD_PATH
  echo "Begin building DejaGnu $DEJAGNU_VERSION..."
  if make -j $(expr 2 \* $(nproc))
  then
    echo "DejaGnu $DEJAGNU_VERSION successfully built."
    pop_dir
    return 0
  else
    echo "DejaGnu $DEJAGNU_VERSION unsuccessfully built."
    pop_dir
    exit 1
  fi
}

# Test DejaGnu
function test_dejagnu()
{
  push_dir $DEJAGNU_BUILD_PATH
  echo "Begin running tests for DejaGnu $DEJAGNU_VERSION..."
  if make -j $(expr 2 \* $(nproc)) check
  then
    echo "Tests for DejaGnu $DEJAGNU_VERSION successfully completed."
    pop_dir
    return 0
  else
    echo "Tests for DejaGnu $DEJAGNU_VERSION did not all successfully complete."
    pop_dir
    exit 1
  fi
}

# Install DejaGnu
function install_dejagnu()
{
  push_dir $DEJAGNU_BUILD_PATH
  echo "Begin installation of DejaGnu $DEJAGNU_VERSION..."
  if make -j $(expr 2 \* $(nproc)) install
  then
    path_cons $DEJAGNU_BIN_PATH
    echo "Successful installation of DejaGnu $DEJAGNU_VERSION."
    pop_dir
    return 0
  else
    echo "Unsuccessful installation of DejaGnu $DEJAGNU_VERSION."
    pop_dir
    exit 1
  fi
}

######################################################################

# Run GCC test suite
function test_gcc()
{
  push_dir $GCC_BUILD_PATH
  if [ -d $GCC_RELEASE_SRC_PATH ]
  then
    echo "Begin running tests for GCC $GCC_VERSION..."
    ulimit -s 32768 # Set stack space so tests do not run out
    make -k -j $(expr 2 \* $(nproc)) check # Run test suite even if errors occur
    echo "Completed running tests for GCC $GCC_VERSION."
    echo "Summary of test results follow:"
    $GCC_RELEASE_SRC_PATH/contrib/test_summary
    pop_dir
    return 0
  else
    echo "Directory $GCC_RELEASE_SRC_PATH with GCC $GCC_VERSION source does not exist" \
         "so tests cannot be run."
    pop_dir
    exit 1
  fi
}

# Install GCC
function install_gcc()
{
  push_dir $GCC_BUILD_PATH
  echo "Begin installation of GCC $GCC_VERSION..."
  if make -j $(expr 2 \* $(nproc)) install
  then
    path_cons $GCC_BIN_PATH
    local word_size=$(getconf LONG_BIT)
    if [ $word_size -eq 64 ]
    then
      local build_os=`$GCC_RELEASE_SRC_PATH/config.guess`
      local gcc_os_version_dependent_lib_path=$GCC_LIB_PATH/$GCC_NAME/$build_os/$GCC_VERSION
      local gcc_os_64bit_dependent_lib_path=$GCC_LIB_PATH/$GCC_NAME/$build_os/lib64
      ln -sf $gcc_os_64bit_dependent_lib_path/libgcc_s.so \
             $gcc_os_version_dependent_lib_path/libgcc_s.so
    fi
    echo "Successful installation of GCC $GCC_VERSION."
    pop_dir
    return 0
  else
    echo "Unsuccessful installation of GCC $GCC_VERSION."
    pop_dir
    exit 1
  fi
}

# External API

# GCC package
#
# Can be passed at most two arguments:
#
# Argument 1 - This is an optional argument specifying the base
# directory pathname in which all building and installation occurs
# within.  If not provided then the default value is the directory
# pathname /usr/local.  The directory pathname must already exist.
#
# Argument 2 - This is an optional boolean argument indicating whether
# a release-specific directory should be created under the base
# directory pathname (argument 1) where all building and installation
# actually occurs.  If not provided then the default value is false
# indicating no release-specific directory will be created.  If true
# then the release-specific directory will be created under the base
# directory pathname with a name appropriate for the package and
# release, e.g., for GCC version 4.8.2 the directory name is
# gcc-4.8.2.  If this directory does not exist it is created.

function package_gcc()
{
  set_gcc_release_vars

  # Argument processing

  local base_pathname
  if [ "$1" != "" ]
  then
    base_pathname=$(readlink -m $1)
  fi

  local release_dir
  if [ "$2" = true ]
  then
    release_dir=$GCC_RELEASE
  fi

  set_gcc_vars $base_pathname $release_dir

  if [ -f $GCC_BIN_PATH/gcc ]
  then
    echo "Package GCC $GCC_VERSION will not be installed as a version of GCC is already present in $GCC_PREFIX_PATH."
    path_cons $GCC_BIN_PATH
    return 0
  fi

  echo "Adding GCC $GCC_VERSION package..."
  if get_gcc && unpack_gcc && get_gcc_support_libs && \
     make_gcc_build_dir && configure_gcc && build_gcc && test_gcc && install_gcc
  then
    echo "Installation of GCC $GCC_VERSION complete."
    return 0
  else
    echo "Installation of GCC $GCC_VERSION failed."
    exit 1
  fi
}

# GCC unpackage
#
# Arguments as described above

function unpackage_gcc()
{
  set_gcc_release_vars

  # Argument processing

  local base_pathname
  if [ "$1" != "" ]
  then
    base_pathname=$(readlink -m $1)
  fi

  local release_dir
  if [ "$2" = true ]
  then
    release_dir=$GCC_RELEASE
  fi

  set_gcc_vars $base_pathname $release_dir

  echo "Removing GCC $GCC_VERSION package..."
  push_dir $GCC_BUILD_PATH
  if make clean
  then
    pop_dir
    if undo_make_gcc_build_dir && undo_get_gcc_support_libs && \
       undo_unpack_gcc && undo_get_gcc
    then
      echo "Removed GCC $GCC_VERSION package."
      return 0
    fi
  else
    pop_dir
  fi
  echo "Failed to completely remove GCC $GCC_VERSION package."
  exit 1
}

# Tcl package
#
# Can be passed at most two arguments:
#
# Argument 1 - This is an optional argument specifying the base
# directory pathname in which all building and installation occurs
# within.  If not provided then the default value is the directory
# pathname /usr/local.  The directory pathname must already exist.
#
# Argument 2 - This is an optional boolean argument indicating whether
# a release-specific directory should be created under the base
# directory pathname (argument 1) where all building and installation
# actually occurs.  If not provided then the default value is false
# indicating no release-specific directory will be created.  If true
# then the release-specific directory will be created under the base
# directory pathname with a name appropriate for the package and
# release, e.g., for Tcl version 8.6.1 the directory name is tcl8.6.1.
# If this directory does not exist it is created.

function package_tcl()
{
  set_tcl_release_vars

  # Argument processing

  local base_pathname
  if [ "$1" != "" ]
  then
    base_pathname=$(readlink -m $1)
  fi

  local release_dir
  if [ "$2" = true ]
  then
    release_dir=$TCL_RELEASE
  fi

  set_tcl_vars $base_pathname $release_dir

  if [ -f $TCL_INCLUDE_PATH/tcl.h ] && ls $TCL_LIB_PATH/libtcl* &> /dev/null && ls $TCL_BIN_PATH/tclsh* &> /dev/null
  then
    echo "Package Tcl $TCL_VERSION will not be installed as a version of Tcl is already present in $TCL_PREFIX_PATH."
    path_cons $TCL_BIN_PATH
    return 0
  fi

  echo "Adding Tcl $TCL_VERSION package..."
  if get_tcl && unpack_tcl && configure_tcl && build_tcl \
     && test_tcl && install_tcl
  then
    echo "Installation of Tcl $TCL_VERSION complete."
    return 0
  else
    echo "Installation of Tcl $TCL_VERSION failed."
    exit 1
  fi
}

# Tcl unpackage
#
# Arguments as described above

function unpackage_tcl()
{
  set_tcl_release_vars

  # Argument processing

  local base_pathname
  if [ "$1" != "" ]
  then
    base_pathname=$(readlink -m $1)
  fi

  local release_dir
  if [ "$2" = true ]
  then
    release_dir=$TCL_RELEASE
  fi

  set_tcl_vars $base_pathname $release_dir

  echo "Removing Tcl $TCL_VERSION package..."
  push_dir $TCL_BUILD_PATH
  if make clean
  then
    pop_dir
    if undo_unpack_tcl && undo_get_tcl
    then
      echo "Removed Tcl $TCL_VERSION package."
      return 0
    fi
  else
    pop_dir
  fi
  echo "Failed to completely remove Tcl $TCL_VERSION package."
  exit 1
}

# Expect package
#
# Can be passed at most two arguments:
#
# Argument 1 - This is an optional argument specifying the base
# directory pathname in which all building and installation occurs
# within.  If not provided then the default value is the directory
# pathname /usr/local.  The directory pathname must already exist.
#
# Argument 2 - This is an optional boolean argument indicating whether
# a release-specific directory should be created under the base
# directory pathname (argument 1) where all building and installation
# actually occurs.  If not provided then the default value is false
# indicating no release-specific directory will be created.  If true
# then the release-specific directory will be created under the base
# directory pathname with a name appropriate for the package and
# release, e.g., for Expect version 5.45 the directory name is
# expect5.45.  If this directory does not exist it is created.

function package_expect()
{
  set_tcl_release_vars
  set_expect_release_vars

  # Argument processing

  local base_pathname
  if [ "$1" != "" ]
  then
    base_pathname=$(readlink -m $1)
  fi

  local tcl_release_dir
  local expect_release_dir
  if [ "$2" = true ]
  then
    tcl_release_dir=$TCL_RELEASE
    expect_release_dir=$EXPECT_RELEASE
  fi

  set_tcl_vars $base_pathname $tcl_release_dir
  set_expect_vars $base_pathname $expect_release_dir

  if [ -f $EXPECT_INCLUDE_PATH/expect.h ] && [ -f $TCL_BIN_PATH/expect ]
  then
    echo "Package Expect $EXPECT_VERSION will not be installed as a version of Expect is already present in $EXPECT_PREFIX_PATH."
    path_cons $EXPECT_BIN_PATH
    return 0
  fi

  echo "Adding Expect $EXPECT_VERSION package..."
  if get_expect && unpack_expect && configure_expect \
     && build_expect && test_expect && install_expect
  then
    echo "Installation of Expect $EXPECT_VERSION complete."
    return 0
  else
    echo "Installation of Expect $EXPECT_VERSION failed."
    exit 1
  fi
}

# Expect unpackage
#
# Arguments as described above

function unpackage_expect()
{
  set_tcl_release_vars
  set_expect_release_vars

  # Argument processing

  local base_pathname
  if [ "$1" != "" ]
  then
    base_pathname=$(readlink -m $1)
  fi

  local tcl_release_dir
  local expect_release_dir
  if [ "$2" = true ]
  then
    tcl_release_dir=$TCL_RELEASE
    expect_release_dir=$EXPECT_RELEASE
  fi

  set_tcl_vars $base_pathname $tcl_release_dir
  set_expect_vars $base_pathname $expect_release_dir

  echo "Removing Expect $EXPECT_VERSION package..."
  push_dir $EXPECT_BUILD_PATH
  if make clean
  then
    pop_dir
    if undo_unpack_expect && undo_get_expect
    then
      echo "Removed Expect $EXPECT_VERSION package."
      return 0
    fi
  else
    pop_dir
  fi
  echo "Failed to completely remove Expect $EXPECT_VERSION package."
  exit 1
}

# DejaGnu package
#
# Can be passed at most two arguments:
#
# Argument 1 - This is an optional argument specifying the base
# directory pathname in which all building and installation occurs
# within.  If not provided then the default value is the directory
# pathname /usr/local.  The directory pathname must already exist.
#
# Argument 2 - This is an optional boolean argument indicating whether
# a release-specific directory should be created under the base
# directory pathname (argument 1) where all building and installation
# actually occurs.  If not provided then the default value is false
# indicating no release-specific directory will be created.  If true
# then the release-specific directory will be created under the base
# directory pathname with a name appropriate for the package and
# release, e.g., for DejaGnu version 1.5.1 the directory name is
# dejagnu-1.5.1.  If this directory does not exist it is created.

function package_dejagnu()
{
  set_dejagnu_release_vars

  # Argument processing

  local base_pathname
  if [ "$1" != "" ]
  then
    base_pathname=$(readlink -m $1)
  fi

  local release_dir
  if [ "$2" = true ]
  then
    release_dir=$DEJAGNU_RELEASE
  fi

  set_dejagnu_vars $base_pathname $release_dir

  if [ -f $DEJAGNU_INCLUDE_PATH/dejagnu.h ]
  then
    echo "Package DejaGnu $DEJAGNU_VERSION will not be installed as a version of DejaGnu is already present in $DEJAGNU_PREFIX_PATH."
    path_cons $DEJAGNU_BIN_PATH
    return 0
  fi

  echo "Adding DejaGnu $DEJAGNU_VERSION package..."
  if get_dejagnu && unpack_dejagnu && configure_dejagnu \
     && build_dejagnu && test_dejagnu && install_dejagnu
  then
    echo "Installation of DejaGnu $DEJAGNU_VERSION complete."
    return 0
  else
    echo "Installation of DejaGnu $DEJAGNU_VERSION failed."
    exit 1
  fi
}

# DejaGnu unpackage
#
# Arguments as described above

function unpackage_dejagnu()
{
  set_dejagnu_release_vars

  # Argument processing

  local base_pathname
  if [ "$1" != "" ]
  then
    base_pathname=$(readlink -m $1)
  fi

  local release_dir
  if [ "$2" = true ]
  then
    release_dir=$DEJAGNU_RELEASE
  fi

  set_dejagnu_vars $base_pathname $release_dir

  echo "Removing DejaGnu $DEJAGNU_VERSION package..."
  push_dir $DEJAGNU_BUILD_PATH
  if make clean
  then
    pop_dir
    if undo_unpack_dejagnu && undo_get_dejagnu
    then
      echo "Removed DejaGnu $DEJAGNU_VERSION package."
      return 0
    fi
  else
    pop_dir
  fi
  echo "Failed to completely remove DejaGnu $DEJAGNU_VERSION package."
  exit 1
}

# Make GCC and all dependencies
#
# Can be passed at most two arguments:
#
# Argument 1 - This is an optional argument specifying the base
# directory pathname in which all building and installation occurs
# within.  If not provided then the default value is the directory
# pathname /usr/local.  The directory pathname must already exist.
#
# Argument 2 - This is an optional boolean argument indicating whether
# a release-specific directory should be created under the base
# directory pathname (argument 1) where all building and installation
# actually occurs.  If not provided then the default value is false
# indicating no release-specific directory will be created.  If true
# then the release-specific directory will be created under the base
# directory pathname with a name appropriate for the package and
# release.

function package_gcc_all()
{
  echo "Adding GCC and all dependent packages..."
  if package_tcl $1 $2 && package_expect $1 $2 && package_dejagnu $1 $2 \
     && package_gcc $1 $2
  then
    echo "Installation of GCC and all dependent packages complete."
    return 0
  else
    echo "Installation of GCC and all dependent packages failed."
    exit 1
  fi
}

# Remove GCC and all dependencies
#
# Arguments as described above

function unpackage_gcc_all()
{
  echo "Removing GCC and all dependent packages..."
  if unpackage_gcc $1 $2 && unpackage_dejagnu $1 $2 && unpackage_expect $1 $2 \
     unpackage_tcl $1 $2
  then
    echo "Removal of GCC and all dependent packages complete."
    return 0
  else
    echo "Removal of GCC and all dependent packages failed."
    exit 1
  fi
}
