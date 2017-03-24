load "common.gnu"

data = "../data/map_union.csv"
set output dir."map_union".ext
set title "map union"

plot data using 1:2, for [i=3:21] '' using 1:i
