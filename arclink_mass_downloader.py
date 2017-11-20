#!/usr/bin/env python

import sys
import os
import logging
from itertools import product
from multiprocessing import Process, Queue
import json
import pandas as pd
from dateutil.parser import parse

from obspy import UTCDateTime, read
from obspy.clients.arclink.client import Client


class ArclinkMassDownloader(object):
    ''' Fetch waveforms using Arclink protocol.

        User must provide:
        - user (optional) but required for some datasets (as credentail)
        - waveform_id,
        - a time window ([startdate, enddate])
        - a time slice
        - a directory to store mseed/seed files

        All messages, errors are handled by a console and file logger.
    '''
    def __init__(self, user,
                 host='webdc.eu', port=18002,
                 route='True',
                 data_dir='.', log_file='amdl.log',
                 client_debug=False):
        '''
            :param user: user (used as credential)
            :param host: arclink server
            :type host: string:type user: string
            :param data_dir: data directory path
            :param port: server port number
            :param route: enable arclink routing
            :type route: boolean
            :type port: int
            :type data_dir: string
            :param log_file: logger file
            :type log_file: string
        '''
        self._setup_logger(log_file)
        self.host = host
        self.port = port
        self.user = user
        self.route = route
        self.data_dir = data_dir
        self.log_file = log_file
        self.client_debug = client_debug
        self.logger.debug('user={}, data_dir={}, '
                          'log_file={}'.format(user, data_dir, log_file))

    def _connect(self):
        ''' setup arclink client/connection

            :return: arclink client instance
        '''
        self.logger.info('Connecting to {}@{}:{}'.format(self.user,
                                                         self.host,
                                                         self.port))
        try:
            client = Client(host=self.host, user=self.user,
                            port=self.port, debug=self.client_debug)
        except Exception as e:
            self.logger.error(e)
            sys.exit(1)
        client.max_status_requests = 200
        return client

    def _setup_logger(self, log_file,
                      console_level=logging.INFO,
                      file_level=logging.DEBUG,
                      file_mode='a'):
        ''' Setup logger for error and messages

            :param console_level: logging level
            :param file_level: loging level
            :param file_mode: 'a' append or 'w' to overwrite
        '''
        logger = logging.getLogger('ArclinkRecuperator')
        logger.setLevel(logging.DEBUG)

        console_handler = logging.StreamHandler()
        logfile_handler = logging.FileHandler(log_file, mode=file_mode)

        console_handler.setLevel(console_level)
        logfile_handler.setLevel(file_level)

        formatter = logging.Formatter('[%(asctime)s]'
                                      ' %(levelname)s %(message)s')

        console_handler.setFormatter(formatter)
        logfile_handler.setFormatter(formatter)

        logger.addHandler(console_handler)
        logger.addHandler(logfile_handler)

        self.logger = logger

    def _get_dates_interval(self, from_date, to_date, freq):
        ''' Slice time interval into dates, bounds are included.

            :param from_date: start date
            :type from_date: UTCDateTime
            :param to_date: end date
            :type to_date: UTCDateTime
            :param freq: time slice
            :type freq: Frequency strings ('1d', '1H')
            :return: UTCDateTime array
        '''
        if from_date >= to_date:
            self.logger.error('Check your time range ! ['
                              '{} - {}]'.format(from_date, to_date))
            return []
        dates = [i for i in pd.date_range(start=from_date.datetime,
                                          end=to_date.datetime,
                                          freq=freq).to_pydatetime()]
        dates = map(UTCDateTime, dates)
        if dates[-1] != to_date:
            dates.append(to_date)
        return dates

    def get_waveforms(self, waveform_id, from_date, to_date, freq):
        ''' Fetch waveform using multiple Arclink requests sliced by time.

                Sequential version.

            :param waveform_id: NET.STA.LOC.CHAN
            :type waveform_id: string
            :param from_date: start date
            :type from_date: UTCDateTime
            :param to_date: end date
            :type to_date: UTCDateTime
            :param freq: time slice
            :type freq: Frequency strings ('1d', '1H')
        '''
        dates = self._get_dates_interval(from_date, to_date, freq)
        for i in range(len(dates)-1):
            d1 = dates[i]
            d2 = dates[i+1]
            self.get_waveforms_simple(waveform_id, d1, d2, freq)

    def get_waveforms_simple(self, waveform_id, d1, d2, proc_id=None):
        ''' Send ArcLink request to server and fetch/save waveforms

            :param waveform_id: NET.STA.LOC.CHAN
            :type waveform_id: string
            :param d1: start date
            :type d1: UTCDateTime
            :param d2: end date
            :type d2: UTCDateTime
            :param proc_id: process rank only used in master/slave mode
            :type proc_id: int
        '''
        if proc_id is not None:
            logprefix = 'process[{}] '.format(proc_id)
        else:
            logprefix = ''

        net, sta, loc, chan = waveform_id.split('.')
        self.logger.debug(logprefix+waveform_id)

        # path and filename
        path = os.path.join(self.data_dir, str(d1.year), net, sta, chan)
        try:
            os.makedirs(path)
        except OSError as e:
            if e.errno != os.errno.EEXIST:
                raise
        filename = os.path.join(path,
                                '-'.join((waveform_id, str(d1), str(d2))))

        # check if rqt was previously done
        if os.path.isfile(filename) and os.path.getsize(filename) > 0:
            self.logger.info(logprefix +
                             'Job already done for {}'.format(filename))
            return

        # arclink rqt
        self.logger.info(logprefix +
                         'Fetching {} from '
                         '[{}-{}]'.format(waveform_id, d1, d2))

        # No sure if Arclink client is thread safe
        # so let instanciate it here.
        client = self._connect()
        try:
            client.save_waveforms(filename,
                                  net, sta, loc, chan,
                                  d1, d2,
                                  format='FSEED',
                                  route=self.route)
        except Exception as e:
            self.logger.error(logprefix + '{}: {}, {}, '
                              '{}'.format(e, waveform_id, d1, d2))
        else:
            # try to read it
            try:
                st = read(filename)
                self.logger.debug(logprefix + st.__str__())
            except Exception as e:
                self.logger.error(logprefix + '{}: {}, '
                                  '{}, {}'.format(e, waveform_id, d1, d2))
                # more action here ?

    def get_waveforms_master(self, waveforms_ids,
                             from_date, to_date, freq,
                             nbprocs):
        ''' Fetch waveform using Arclink.

                Parallel version using Master / Slave paradigm.

            :param waveforms_ids: (NET.STA.LOC.CHAN, ...)
            :type waveform_id: list of string (ie. waveform_id)
            :param from_date: start date
            :type from_date: UTCDateTime
            :param to_date: end date
            :type to_date: UTCDateTime
            :param freq: time slice
            :type freq: Frequency strings ('1d', '1H')
            :parm nbprocs: nb of slaves to use
            :type nbprocs: int
        '''
        slave_pool = []
        rqt_queue = Queue()

        self.logger.info('Master started with {} processes'.format(nbprocs))

        # slaves
        for proc_id in range(nbprocs):
            slave_pool.append(Process(target=self.get_waveforms_chunk,
                                      args=(proc_id, rqt_queue)))

        # start the slaves
        for slave in slave_pool:
            slave.start()

        # populate the queue with all rqt
        dates = self._get_dates_interval(from_date, to_date, freq)
        for nslc in waveforms_ids:
            waveform_id = '.'.join(nslc)
            for i in range(len(dates)-1):
                d1 = dates[i]
                d2 = dates[i+1]
                rqt = {'waveform_id': waveform_id, 'from_date': d1,
                       'to_date': d2, 'freq': freq}
                rqt_queue.put(rqt)

        # send signal termination to slave
        for i in range(0, nbprocs):
            rqt_queue.put(None)

        # wait for the slaves to finish processing
        for slave in slave_pool:
            slave.join()

    def get_waveforms_chunk(self, proc_id, rqt_queue):
        ''' Slave process to handle arclink request

            :param proc_id: process rank
            :type proc_id: int
            :param rqt_queue: queue to handle requests
            :type rqt_queue: dict()
        '''
        self.logger.info('Process[{}] started.'.format(proc_id))
        while True:
            try:
                rqt = rqt_queue.get()
            except Exception as e:
                self.logger.error(e)
                return

            # self.logger.info('process[{}] {}'.format(proc_id, rqt))
            if rqt is None:
                self.logger.info('process[{}] completed.'.format(proc_id))
                return
            self.get_waveforms_simple(rqt['waveform_id'], rqt['from_date'],
                                      rqt['to_date'], proc_id)

    def get_waveforms_from_json_file(self, filename, nbprocs):
        ''' Fetch waveform from json file

            Parallel version using Master / Slave paradigm.
            Each line is json formated.

            :param filename: rqt filename
            :type filename: string (filename path)
            :parm nbprocs: nb of slaves to use
            :type nbprocs: int
        '''
        slave_pool = []
        rqt_queue = Queue()

        self.logger.info('Master started with {} processes'.format(nbprocs))

        # slaves
        for proc_id in range(nbprocs):
            slave_pool.append(Process(target=self.get_waveforms_chunk,
                                      args=(proc_id, rqt_queue)))

        # start the slaves
        for slave in slave_pool:
            slave.start()

        # populate the queue with all requests
        with open(filename, 'r') as f:
            for line in f:
                rqt = json.loads(line)
                rqt['from_date'] = UTCDateTime(parse(rqt['from_date']))
                rqt['to_date'] = UTCDateTime(parse(rqt['to_date']))
                if 'freq' in rqt.keys():
                    del rqt['freq']
                rqt_queue.put(rqt)

        # send signal termination to slave
        for i in range(0, nbprocs):
            rqt_queue.put(None)

        # wait for the slaves to finish processing
        for slave in slave_pool:
            slave.join()

    def save_rqt(self, waveforms_ids, from_date, to_date, freq):
        ''' Save Arclink requests to (kind of) json files.

            Caution:
            Each line (ie. request)
            is json formated but not the whole file !
        '''
        dates = self._get_dates_interval(from_date, to_date, freq)
        for nslc in waveforms_ids:
            waveform_id = '.'.join(nslc)
            for i in range(len(dates)-1):
                d1 = dates[i]
                d2 = dates[i+1]
                rqt = {'waveform_id': waveform_id, 'from_date': d1,
                       'to_date': d2, 'freq': freq}
                print json.dumps(rqt, default=str)


if __name__ == "__main__":
    ''' Todo: use station inventory to drive the requests.'''

    network = 'FR'
    stations = ['CIEL', 'STR']
    location = '*'
    channels = ['HHE', 'HHN', 'HHZ']

    from_date = UTCDateTime("2017-07-01 00:00:00")
    to_date = UTCDateTime("2017-07-01 06:00:00")
    freq = '1H'

    host = 'renass-fw.u-strasbg.fr'
    port = 18001
    user = 'marc'
    data_dir = '.'
    log_file = 'downloader.log'
    # arclink servers usually allow very few concurrent processes
    # keep it < 8
    nbprocs = 1

    waveforms_ids = product([network], stations, [location], channels)

    # Perform arclink requests using master/slave mode
    recup = ArclinkMassDownloader(user=user, host=host, port=port,
                                  route=False,
                                  data_dir=data_dir, log_file=log_file)
    recup.get_waveforms_master(waveforms_ids,
                               from_date, to_date, freq, nbprocs)
