load "common.gnu"

data = "../data/list_construct.csv"
set output dir."list_construct".ext
set title "constructing lists"

plot data using 1:2, for [i=3:21] '' using 1:i
