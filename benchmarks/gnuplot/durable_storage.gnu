load "durable_common.gnu"

data = "../data/durable_storage_amplification.csv"
set output dir."durable_storage".ext
set title "storage overhead"
set ylabel "storage amplification factor"
set format y "%.2fx"

plot data using 1:2, for [i=3:21] '' using 1:i
