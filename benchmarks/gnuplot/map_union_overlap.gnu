load "common.gnu"

data = "../data/map_union_overlap.csv"
set output dir."map_union_overlap".ext
set title "map overlapping union"

plot data using 1:2, for [i=3:21] '' using 1:i
