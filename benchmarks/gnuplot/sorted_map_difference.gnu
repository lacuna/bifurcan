load "common.gnu"

data = "../data/sorted_map_difference.csv"
set output dir."sorted_map_difference".ext
set title "sorted map difference"

plot data using 1:2, for [i=3:21] '' using 1:i
