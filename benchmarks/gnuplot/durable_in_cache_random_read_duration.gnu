load "durable_common.gnu"

data = "../data/durable_in_cache_random_read_duration.csv"
set output dir."durable_in_cache_random_read_duration".ext
set title "in-cache random read duration"
set ylabel "microseconds per entry read"
set format y "%.1fus"

plot data using 1:2, for [i=3:21] '' using 1:i
