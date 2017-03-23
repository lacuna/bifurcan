load "common.gnu"

data = "../data/map_lookup.csv"
set output dir."map_lookup".ext
set title "random lookup on maps"

plot data using 1:2, for [i=3:21] '' using 1:i
