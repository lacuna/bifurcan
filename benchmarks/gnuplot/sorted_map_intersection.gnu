load "common.gnu"

data = "../data/sorted_map_intersection.csv"
set output dir."sorted_map_intersection".ext
set title "sorted map intersection"

plot data using 1:2, for [i=3:21] '' using 1:i
