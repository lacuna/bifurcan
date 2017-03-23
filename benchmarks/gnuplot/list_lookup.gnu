load "common.gnu"

data = "../data/list_lookup.csv"
set output dir."list_lookup".ext
set title "random lookup on lists"

plot data using 1:2, for [i=3:21] '' using 1:i
