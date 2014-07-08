#!/bin/bash

if [ $# -ne 2 ]; then
  echo "usage: $0 <output-dir> <file>"
  exit 1
fi

thrift -r --gen perl -o $1 $2

for f in `find $1/gen-perl -name '*.pm'` ; do
    cat $f | awk '{print} /package .*Exception;/ { print "use base qw(Error);";}' > tmp.pm
    mv tmp.pm $f
done
