load "durable_common.gnu"

data = "../data/durable_random_read_amplification.csv"
set output dir."durable_random_read".ext
set title "random reads"
set ylabel "read amplification factor"
set format y "%.2fx"

plot data using 1:2, for [i=3:21] '' using 1:i
