#!/usr/bin/perl

my $dir=shift;

print join(":", split(/\s/, `ls $dir/lib/java/hypertable-*.jar`));
