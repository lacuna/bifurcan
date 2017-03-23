load "common.gnu"

data = "../data/map_iterate.csv"
set output dir."map_iterate".ext
set title "iterating over maps"

plot data using 1:2, for [i=3:21] '' using 1:i
