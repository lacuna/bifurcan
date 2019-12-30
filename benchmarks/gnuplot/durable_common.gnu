set terminal pngcairo enhanced font 'helvetica,14' fontscale 1.0 size 1500, 1000 linewidth 2
set xtics nomirror 
set key autotitle columnhead outside right
set style data linespoints 
set title font 'helvetica,26'
set xlabel "dataset size"
# set logscale y
set logscale x 2
set yrange [0:]
# set format y "%.0fx"
set format x "%.1fgb"
set datafile separator ','

dir = "../images/"
ext = ".png"
