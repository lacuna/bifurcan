load "common.gnu"

data = "../data/string_concat.csv"
set output dir."string_concat".ext
set title "string concat"

plot data using 1:2, for [i=3:21] '' using 1:i
