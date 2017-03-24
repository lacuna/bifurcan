load "common.gnu"

data = "../data/map_difference.csv"
set output dir."map_difference".ext
set title "map difference"

plot data using 1:2, for [i=3:21] '' using 1:i
