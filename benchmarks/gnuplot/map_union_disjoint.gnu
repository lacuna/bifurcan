load "common.gnu"

data = "../data/map_union_disjoint.csv"
set output dir."map_union_disjoint".ext
set title "map disjoint union"

plot data using 1:2, for [i=3:21] '' using 1:i
