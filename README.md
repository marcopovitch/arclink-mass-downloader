# Arclink Mass Downloader

This is an **Arclink Mass Downloader** which uses a master/slave paradigm
(ie. parallel programming). It relies heavily on
[obspy](https://github.com/obspy/obspy) (arclink client).

Do not try to set `nbprocs` too high ( > 8 ) since most arclink servers will
reject your data requests.
