load "common.gnu"

data = "../data/set_union.csv"
set output dir."set_union".ext
set title "set union"

plot data using 1:2, for [i=3:21] '' using 1:i
