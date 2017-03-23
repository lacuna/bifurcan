load "common.gnu"

data = "../data/set_union_overlap.csv"
set output dir."set_union_overlap".ext
set title "set overlapping union"

plot data using 1:2, for [i=3:21] '' using 1:i
