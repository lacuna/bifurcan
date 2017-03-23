load "common.gnu"

data = "../data/map_intersection_overlap.csv"
set output dir."map_intersection_overlap".ext
set title "map overlapping intersection"

plot data using 1:2, for [i=3:21] '' using 1:i
