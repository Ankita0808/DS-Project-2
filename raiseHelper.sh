#!/bin/bash

#for i in {1..$1}
for (( i=0; i<$1; i++ ))
do
	CMD="./helper&"
	eval "$CMD"
done

