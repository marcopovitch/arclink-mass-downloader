grep UNSET $1 | cut -f4- -d':' | sed 's/ //g' | awk -F',' '{print "{\"waveform_id\": \""$1"\", \"from_date\": \""$2"\", \"to_date\": \""$3"\"}"}' 
