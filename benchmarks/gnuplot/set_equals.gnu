load "common.gnu"

data = "../data/set_equals.csv"
set output dir."set_equals".ext
set title "set equality"

plot data using 1:2, for [i=3:21] '' using 1:i
