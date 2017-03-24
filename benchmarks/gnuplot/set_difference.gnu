load "common.gnu"

data = "../data/set_difference.csv"
set output dir."set_difference".ext
set title "set difference"

plot data using 1:2, for [i=3:21] '' using 1:i
