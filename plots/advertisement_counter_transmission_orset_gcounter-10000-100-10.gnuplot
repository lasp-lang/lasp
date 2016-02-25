# Note you need gnuplot 4.4 for the pdfcairo terminal.

set terminal pdfcairo font "Gill Sans,9" linewidth 4 rounded fontscale 1.0

# Line style for axes
set style line 80 lt rgb "#808080"

# Line style for grid
set style line 81 lt 0  # dashed
set style line 81 lt rgb "#808080"  # grey

set grid back linestyle 81
set border 3 back linestyle 80 # Remove border on top and right.  These
             # borders are useless and make it harder
             # to see plotted lines near the border.
    # Also, put it in grey; no need for so much emphasis on a border.
set xtics nomirror
set ytics nomirror

#set log x
#set mxtics 10    # Makes logscale look good.

# Line styles: try to pick pleasing colors, rather
# than strictly primary colors or hard-to-see colors
# like gnuplot's default yellow.  Make the lines thick
# so they're easy to see in small plots in papers.
set style line 1 lt rgb "#A00000" lw 2 pt 1
set style line 2 lt rgb "#00A000" lw 2 pt 6
set style line 3 lt rgb "#5060D0" lw 2 pt 2
set style line 4 lt rgb "#F25900" lw 2 pt 9

set output "advertisement_counter_transmission_orset_gcounter-10000-100-10.pdf"
set xlabel "Time in Seconds"
set ylabel "MB/client Transmitted"

set key top right

set datafile missing '0'

# log scale on the y axis
set logscale y 2
# set format y ""
# set ytics 1e-6, 10, 1
# set ytics add ("1" 1, ".1" 0.1, ".01" 0.01, "10^-3" 0.001, \
#                "10^-4" 0.0001)
set mytics 10

# set xrange [0:60]
set yrange [0.1:16]

# Input file contains comma-separated values fields
set datafile separator ","

# plot "/tmp/lasp_transmission_instrumentation-lasp_orset.csv" using 1:2 title 'OR-Set' with linespoints
#
# plot "/tmp/lasp_transmission_instrumentation-lasp_orset-lasp_gcounter-10000-100-10.csv" using 1:3 title "10000 events, 100 clients, 10 second sync" with linespoints, \
#      "/tmp/lasp_transmission_instrumentation-lasp_orset-lasp_gcounter-10000-500-10.csv" using 1:3 title "10000 events, 500 clients, 10 second sync" with linespoints, \
#      "/tmp/lasp_transmission_instrumentation-lasp_orset-lasp_gcounter-10000-1000-10.csv" using 1:3 title "10000 events, 1000 clients, 10 second sync" with linespoints
#
plot "../apps/lasp/priv/lasp_transmission_instrumentation-client-lasp_orset-lasp_gcounter-10000-100-10.csv" using 1:3 title "Client-to-Server" with linespoints, \
     "../apps/lasp/priv/lasp_transmission_instrumentation-client-lasp_orset-lasp_gcounter-10000-100-10.csv" using 1:3 title "Server-to-Server" with linespoints
