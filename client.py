#!/usr/bin/env python

from itertools import product
from arclink_mass_downloader import ArclinkMassDownloader
from obspy import UTCDateTime


if __name__ == "__main__":
    ''' Todo: use station inventory to drive the requests.'''

    network = 'FR'
    stations = ['STR', 'ISO', 'CHMF']
    location = '*'
    channels = ['HHE', 'HHN', 'HHZ']

    from_date = UTCDateTime("2016-11-19 00:00:00")
    to_date = UTCDateTime("2016-11-20 00:00:00")
    freq = '6H'

    host = 'renass-fw.u-strasbg.fr'
    port = 18001
    user = 'marc'
    data_dir = '.'
    log_file = 'downloader.log'
    nbprocs = 6

    waveforms_ids = product([network], stations, [location], channels)

    recup = ArclinkMassDownloader(user=user, host=host, port=port,
                                  route=False,
                                  data_dir=data_dir, log_file=log_file)

    # Read a file which contains one request by line json formated
    # recup.get_waveforms_from_json_file('my_rqts.json', nbprocs)

    # Just a hack to write one request by line (json formated)
    # from waveforms_ids, from_date, to_date and freq
    # recup.save_rqt(waveforms_ids, from_date, to_date, freq)

    # Perform arclink requests using master/slave mode
    recup.get_waveforms_master(waveforms_ids, from_date,
                               to_date, freq, nbprocs)
