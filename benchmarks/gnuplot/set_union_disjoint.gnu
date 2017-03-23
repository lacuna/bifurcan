load "common.gnu"

data = "../data/set_union_disjoint.csv"
set output dir."set_union_disjoint".ext
set title "set disjoint union"

plot data using 1:2, for [i=3:21] '' using 1:i
