#!/usr/bin/env bash

echo "========================================================================"
echo "HyperPython: SerizalizedCellsWriter test"
echo "========================================================================"

SCRIPT_DIR=`dirname $0`

echo "SCRIPT_DIR is $SCRIPT_DIR"
echo "PYTHONPATH is $PYTHONPATH"

python $SCRIPT_DIR/writer.py > test-writer.txt
diff test-writer.txt $SCRIPT_DIR/test-writer.golden
if [ $? != 0 ]
then
  echo "golden file differs"
  exit 1
fi
