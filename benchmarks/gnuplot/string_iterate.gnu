load "common.gnu"

data = "../data/string_iterate.csv"
set output dir."string_iterate".ext
set title "string iterate"

plot data using 1:2, for [i=3:21] '' using 1:i
