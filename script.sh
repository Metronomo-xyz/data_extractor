start_time = `date +%s`

python3 -u -m extractor -t -a -r 2
extractor_time = `date +%s`
echo extraction time was `expr $exrtactor_time - $start_time` s.

python3 -u -m aggregator -r 2
aggregator_time = `date +%s`
echo aggregation time was `expr $aggregator_time - $exrtactor_time` s.

python3 -u -m combiner
combiner_time = `date +%s`
echo combining time was `expr $combiner_time - $aggregator_time` s.

sudo shutdown -h now
