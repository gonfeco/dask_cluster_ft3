#!/bin/sh

NoIB=$(ip -4 -o addr | grep -m 1 'bond0' | sed -n 's/^.*inet //p'| sed -n 's/\/.*//p')
echo $NoIB
