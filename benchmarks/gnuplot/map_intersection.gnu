load "common.gnu"

data = "../data/map_intersection.csv"
set output dir."map_intersection".ext
set title "map intersection"

plot data using 1:2, for [i=3:21] '' using 1:i
