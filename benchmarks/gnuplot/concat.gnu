load "common.gnu"

data = "../data/concat.csv"
set output dir."concat".ext
set title "concatenating lists"

plot data using 1:2, for [i=3:21] '' using 1:i
