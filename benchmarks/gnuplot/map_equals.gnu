load "common.gnu"

data = "../data/map_equals.csv"
set output dir."map_equals".ext
set title "map equality"

plot data using 1:2, for [i=3:21] '' using 1:i
