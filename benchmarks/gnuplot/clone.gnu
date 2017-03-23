load "common.gnu"

data = "../data/clone.csv"
set output dir."clone".ext
set title "cloning mutable collections"

plot data using 1:2, for [i=3:21] '' using 1:i
