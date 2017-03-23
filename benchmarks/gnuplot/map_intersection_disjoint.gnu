load "common.gnu"

data = "../data/map_intersection_disjoint.csv"
set output dir."map_intersection_disjoint".ext
set title "map disjoint intersections"

plot data using 1:2, for [i=3:21] '' using 1:i
