load "common.gnu"

data = "../data/string_lookup.csv"
set output dir."string_lookup".ext
set title "string lookup"

plot data using 1:2, for [i=3:21] '' using 1:i
