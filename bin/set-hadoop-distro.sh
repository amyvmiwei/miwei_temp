#!/bin/bash

export HT_HOME=$(cd `dirname "$0"`/.. && pwd)

declare -a Distros=('apache1' 'apache2' 'cdh3' 'cdh4' 'hdp2');

usage() {
  echo ""
  echo "usage: set-hadoop-distro.sh <distro>"
  echo ""
  echo "This script will copy the jar files for <distro> into the primary jar"
  echo "directory ($HT_HOME/lib/java)"
  echo "so that Hypertable will run on top of a Hadoop installation of type"
  echo "<distro>.  Supported values for <distro> are:  ${Distros[@]}."
  echo "The default distribution is cdh4."
  echo ""
}

if [ "$#" -eq 0 ]; then
  usage
  exit 1
fi

DISTRO=
for distro in ${Distros[@]}; do
    if [ $1 == $distro ]; then
        DISTRO=$1
        shift
        break;
    fi
done

# Check for valid distro
if [ -z $DISTRO ]; then
    usage
    echo "error: unrecognized distro '$1'"
    exit 1;
fi

\rm -f $HT_HOME/lib/java/*.jar
\cp -f $HT_HOME/lib/java/common/*.jar $HT_HOME/lib/java
\cp -f $HT_HOME/lib/java/$DISTRO/*.jar $HT_HOME/lib/java

if [ $DISTRO == "cdh4" ]; then
    \cp $HT_HOME/lib/java/apache2/hypertable-*.jar $HT_HOME/lib/java
    \cp $HT_HOME/lib/java/specific/guava-11.0.2.jar $HT_HOME/lib/java
    \cp $HT_HOME/lib/java/specific/protobuf-java-2.4.0a.jar $HT_HOME/lib/java
elif [ $DISTRO == "hdp2" ]; then
    \cp $HT_HOME/lib/java/apache2/*.jar $HT_HOME/lib/java
    \cp $HT_HOME/lib/java/specific/guava-11.0.2.jar $HT_HOME/lib/java
    \cp $HT_HOME/lib/java/specific/protobuf-java-2.5.0.jar $HT_HOME/lib/java
elif [ $DISTRO == "apache2" ]; then
    \cp $HT_HOME/lib/java/specific/guava-11.0.2.jar $HT_HOME/lib/java
    \cp $HT_HOME/lib/java/specific/protobuf-java-2.5.0.jar $HT_HOME/lib/java
fi

echo $DISTRO > $HT_HOME/conf/hadoop-distro

echo "Hypertable successfully configured for Hadoop $DISTRO"

exit 0
