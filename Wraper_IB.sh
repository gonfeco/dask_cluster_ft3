#!/bin/sh

IB=$(ip -4 -o addr | grep -m 1 'ib0' | sed -n 's/^.*inet //p' | sed -n 's/\/.*//p')

echo $IB
