start_time=`date +%s`

python3 -u -m extractor -t -a -r 2
extractor_time=`date +%s`
echo extraction time was `expr $extractor_time - $start_time` s.

python3 -u -m aggregator -t -a -r 2
aggregator_time=`date +%s`
echo aggregation time was `expr $aggregator_time - $extractor_time` s.

python3 -u -m combiner -r 90
combiner_time=`date +%s`
echo combining time was `expr $combiner_time - $aggregator_time` s.

sudo shutdown -h now
