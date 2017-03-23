load "common.gnu"

data = "../data/set_difference_overlap.csv"
set output dir."set_difference_overlap".ext
set title "set overlapping differences"

plot data using 1:2, for [i=3:21] '' using 1:i
