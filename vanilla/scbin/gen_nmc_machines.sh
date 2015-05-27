#!/bin/sh

if [ ! $1 ]; then
  echo "Please enter total number of nodes"
  exit 1
fi

numnode=$1
rm machinelist 2> /dev/null
i=1
while [ $i -le $numnode ]; do
  #echo $i node-$i.scale.ucare >> machinelist
  ip=`nslookup node-$i.scale.ucare | awk 'BEGIN { flag = 0 } /Name/ { flag = 1 } /Address/ { if (flag == 1) print $2 }'`
  echo $i $ip >> machinelist
  i=`expr $i + 1`
done
