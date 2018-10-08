load "common.gnu"

data = "../data/sorted_map_construct.csv"
set output dir."sorted_map_construct".ext
set title "constructing sorted maps"

plot data using 1:2, for [i=3:21] '' using 1:i
