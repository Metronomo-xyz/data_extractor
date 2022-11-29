nohup python3 -u -m extractor -t -a -s 29112022 -r 2 > extractor_log.txt 2> extractor_error.txt &
nohup python3 -u -m aggregator -r 2 > agg_logs.txt 2> agg_error.txt &
nohup python3 -u -m combiner > combiner_logs.txt 2> combiner_errors.txt &
