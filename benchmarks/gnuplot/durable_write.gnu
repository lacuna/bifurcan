load "durable_common.gnu"

data = "../data/durable_write_amplification.csv"
set output dir."durable_write".ext
set title "database construction"
set ylabel "amplification factor"
set format y "%.1fx"

plot data using 1:2, for [i=3:21] '' using 1:i
