load "common.gnu"

data = "../data/set_lookup.csv"
set output dir."set_lookup".ext
set title "random lookup on sets"

plot data using 1:2, for [i=3:21] '' using 1:i
