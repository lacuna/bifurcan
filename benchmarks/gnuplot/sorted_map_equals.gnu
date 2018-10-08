load "common.gnu"

data = "../data/sorted_map_equals.csv"
set output dir."sorted_map_equals".ext
set title "sorted map equality"

plot data using 1:2, for [i=3:21] '' using 1:i
