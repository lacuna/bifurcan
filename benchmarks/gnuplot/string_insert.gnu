load "common.gnu"

data = "../data/string_insert.csv"
set output dir."string_insert".ext
set title "string insert"

plot data using 1:2, for [i=3:21] '' using 1:i
