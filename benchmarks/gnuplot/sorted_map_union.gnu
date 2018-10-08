load "common.gnu"

data = "../data/sorted_map_union.csv"
set output dir."sorted_map_union".ext
set title "sorted map union"

plot data using 1:2, for [i=3:21] '' using 1:i
