load "common.gnu"

data = "../data/set_intersection_overlap.csv"
set output dir."set_intersection_overlap".ext
set title "set overlapping intersection"

plot data using 1:2, for [i=3:21] '' using 1:i
