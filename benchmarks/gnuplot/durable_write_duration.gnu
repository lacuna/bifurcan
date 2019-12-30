load "durable_common.gnu"

data = "../data/durable_write_duration.csv"
set output dir."durable_write_duration".ext
set title "database construction duration"
set ylabel "microseconds per entry stored"
set format y "%.1fus"

plot data using 1:2, for [i=3:21] '' using 1:i
