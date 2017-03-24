load "common.gnu"

data = "../data/set_intersection.csv"
set output dir."set_intersection".ext
set title "set intersection"

plot data using 1:2, for [i=3:21] '' using 1:i
