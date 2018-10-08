load "common.gnu"

data = "../data/sorted_map_lookup.csv"
set output dir."sorted_map_lookup".ext
set title "random lookup on sorted maps"

plot data using 1:2, for [i=3:21] '' using 1:i
