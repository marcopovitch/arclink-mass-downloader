grep ERROR arclink_recuperator.log | cut -d']' -f2 | cut -d'/' -f1  | cut -d':' -f1 | sort | uniq -c
