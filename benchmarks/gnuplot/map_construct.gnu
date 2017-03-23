load "common.gnu"

data = "../data/map_construct.csv"
set output dir."map_construct".ext
set title "constructing maps"

plot data using 1:2, for [i=3:21] '' using 1:i
