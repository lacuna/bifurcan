load "common.gnu"

data = "../data/string_remove.csv"
set output dir."string_remove".ext
set title "string remove"

plot data using 1:2, for [i=3:21] '' using 1:i
