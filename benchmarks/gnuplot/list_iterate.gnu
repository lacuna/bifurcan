load "common.gnu"

set format y "%.1fns"

data = "../data/list_iterate.csv"
set output dir."list_iterate".ext
set title "iterating over lists"

plot data using 1:2, for [i=3:21] '' using 1:i
