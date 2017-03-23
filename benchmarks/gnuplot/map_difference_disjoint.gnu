load "common.gnu"

data = "../data/map_difference_disjoint.csv"
set output dir."map_difference_disjoint".ext
set title "map disjoint differences"

plot data using 1:2, for [i=3:21] '' using 1:i
