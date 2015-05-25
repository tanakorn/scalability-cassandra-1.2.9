#!/bin/sh

if [ ! $1 ]; then
  echo "Please enter total number of nodes"
  exit 1
fi

numnode=$1
rm machinelist 2> /dev/null
i=1
while [ $i -le $numnode ]; do
  echo $i node-$i.scale.ucare >> machinelist
  i=`expr $i + 1`
done
