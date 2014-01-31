#!/usr/bin/perl

$filename = shift;
die "filename is missing" unless $filename;
$qualifier = shift;
die "qualifier is missing" unless defined $qualifier;

open(FH, "< $filename") or die "Couldn't open $filename: $!\n";

while (<FH>) {
  if ($qualifier) {
    /^(\d+)-(\d+)-(\d+) (\d+):(\d+):(\d+)\.(\d+)\t(\w+)\t(\w+):(\w+)\t(.*)$/;
    #print "line: $_\n";
    #print "year: $1-$2-$3\n";
    #print "time: $4-$5-$6.$7\n";
    #print "row:  $8\n";
    #print "col:  $9\n";
    #print "qual: $10\n";
    #print "cell: $11\n";
    $select="SELECT $9:$10 FROM IndexTest WHERE Exists($9:$10) AND TIMESTAMP = \"$1-$2-$3 $4:$5:$6:$7\" DISPLAY_TIMESTAMPS;";
  }
  else {
    /^(\d+)-(\d+)-(\d+) (\d+):(\d+):(\d+)\.(\d+)\t(\w+)\t(\w+)\t(.*)$/;
    #print "year: $1-$2-$3\n";
    #print "time: $4-$5-$6.$7\n";
    #print "row:  $8\n";
    #print "col:  $9\n";
    #print "cell: $10\n";
    $select="SELECT $9 FROM IndexTest WHERE $9 = \"$10\" AND TIMESTAMP = \"$1-$2-$3 $4:$5:$6:$7\" DISPLAY_TIMESTAMPS;";
  }
  print "$select\n";
}
close(FH);
