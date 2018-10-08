load "common.gnu"

data = "../data/sorted_map_iterate.csv"
set output dir."sorted_map_iterate".ext
set title "iterating over sorted maps"

plot data using 1:2, for [i=3:21] '' using 1:i
