#!/bin/sh

if [ ! $1 ]; then
  echo "Please enter total number of nodes"
  exit 1
fi

down=13

numnode=$1
rm machinelist 2> /dev/null
i=1
j=1
while [ $i -le $numnode ]; do
  #echo $i node-$i.scale.ucare >> machinelist
  if [ $j -ne $down ]; then
    ip=`nslookup node-$j.scale.ucare | awk 'BEGIN { flag = 0 } /Name/ { flag = 1 } /Address/ { if (flag == 1) print $2 }'`
    echo $i $ip >> machinelist
    i=`expr $i + 1`
  fi
  j=`expr $j + 1`
done
