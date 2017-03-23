load "common.gnu"

data = "../data/set_construct.csv"
set output dir."set_construct".ext
set title "constructing sets"

plot data using 1:2, for [i=3:21] '' using 1:i
