load "common.gnu"

data = "../data/set_difference_disjoint.csv"
set output dir."set_difference_disjoint".ext
set title "set disjoint differences"

plot data using 1:2, for [i=3:21] '' using 1:i
