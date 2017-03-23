load "common.gnu"

data = "../data/set_intersection_disjoint.csv"
set output dir."set_intersection_disjoint".ext
set title "set disjoint intersections"

plot data using 1:2, for [i=3:21] '' using 1:i
