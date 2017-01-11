#!/bin/sh

if [ $# -eq 0 ]; then
    python source.py -a localhost -p 12345 -d ./tmp/ ;

elif [ $# -eq 3 ]; then
    python source.py -a $1 -p $2 -d $3 ;
fi
