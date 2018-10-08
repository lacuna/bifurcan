set terminal pngcairo enhanced font 'arial,22' fontscale 1.0 size 1500, 1000 linewidth 2
set xtics nomirror 
set key autotitle columnhead outside right
set style data linespoints 
set title font 'arial,36'
set xlabel "number of elements"
set ylabel "time per element, in nanoseconds"
# set logscale y
set logscale x
set yrange [0:]
set format y "%.0fns"
set format x "%.0s%c"
set datafile separator ','

dir = "../images/"
ext = ".png"
