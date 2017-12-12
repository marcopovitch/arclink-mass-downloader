# Arclink Mass Downloader

This is an **Arclink Mass Downloader** which uses a master/slave paradigm
(ie. parallel programming) to download huge amount of seismological waveforms using arclink protocol. 

It relies heavily on
[obspy](https://github.com/obspy/obspy) (arclink client).

## Usage

Use the `client.py` python script as a sample program to fetch your data. 

For the moment *arclink-mass-downloader* can only use :

1.  a list of channel/timespan (and other paramters) to be defined in `client.py` (using `get_waveforms_master(waveforms_ids, from_date,
                               to_date, freq, nbprocs)` method);
1.  or a file containing a json formated list of requests to fetch the data (using `get_waveforms_from_json_file('my_rqts.json', nbprocs)` method), such as :

	<pre>
	{"waveform_id": "FR.CIEL.*.HHZ", "from_date": "2017-07-01T04:00:00.000000Z", 	"to_date": "2017-07-01T05:00:00.000000Z"}
	</pre>

But, it is planed to use command line options and station inventory (dataless/xml) to drive the requests.


## Note
##### UNSET arclink status
Sometimes the arclink server sends back an 'UNSET' status to a request (related to `max_status_requests`). It doesn't mean the data is not avalaible (or the opposite) ... but the request should be sent again.

You can get all those 'UNSET' requests, from the logfile `logfile.txt`, in a json formated file using :
	
	script/get_unset.sh logfile.txt 

and then go to Usage.2 section. 

##### Number of processes
Do not try to set `nbprocs` too high ( > 6 ) since most arclink servers will
reject your data requests.


## Todo
* use station inventory 
* use command line arguments to set :
	* `nbprocs`
	* `host:port`
	* `route`
	* `max_status_requests`
* use command line arguments to use 
	* json requests file 
	* station inventory (dataless/stationxml) 
	* selected station/timespan.
* exhaust all requests leading to 'UNSET' status (*ie.* send them again until "no-more-UNSET")