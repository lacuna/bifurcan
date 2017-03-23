#!/bin/bash

cd gnuplot
for x in `ls`; do gnuplot $x; done
cd ..