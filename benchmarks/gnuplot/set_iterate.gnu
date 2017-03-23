load "common.gnu"

data = "../data/set_iterate.csv"
set output dir."set_iterate".ext
set title "iterating over sets"

plot data using 1:2, for [i=3:21] '' using 1:i
