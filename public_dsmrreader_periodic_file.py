"""
    https://dsmr-reader.readthedocs.io/en/latest/installation/datalogger.html
    Installation:
        pip3 install pyserial==3.4 requests==2.22.0
"""
import datetime
import time
import os
import json
import serial
from influxdb import InfluxDBClient
import configparser
import logging
import logging.config
# import requests

from threading import Thread
"""
telegram = 
    from first line starting with '/'
    till last line -included- starting with '!'
buffer telegrams in memory,
write every minute the memory to txt file;
txt file name : dsmr-telegramlogger-TST.txt
   TXT = 
to txt file
start new txt file every minute

TODO:

V3_00: make use of multithreading
    writing xxx.json files
   seperate .py program:    if xxx.txt files available: first ingest files, sorted by (date), oldest first
            keep xxx.tmp files also in memory; ingest this data after the xxx.txt files
            keep writing to tmp as backup in case real time ingest fails; 
            if data of tmp ingested successfuly: change tmp-file to .txp (text file processed) as kind of backup

"""


def timestamp_from_telegram(raw_time_dst_str):    # 200306224224W
    """
    raw_time_dst_str: format 200306224224W : yymmddHHMMSS + W (winter time) or S (summer time)
    return: ['2020-03-06T21:42:24Z', True, '200306224224'] : [iso-time-format, boolean, string]
    """
    # tst : timestamp
    raw_time = raw_time_dst_str[0:-1]
    tst_Y = int(raw_time_dst_str[0:2]) + 2000
    tst_m = int(raw_time_dst_str[2:4])
    tst_d = int(raw_time_dst_str[4:6])
    tst_H = int(raw_time_dst_str[6:8])
    tst_M = int(raw_time_dst_str[8:10])
    tst_S = int(raw_time_dst_str[10:12])
    dst_str = raw_time_dst_str[-1]
    if dst_str == 'S':
        dst = -2
    elif dst_str == 'W':
        dst = -1
    else:
        dst = 0
        # dst code not recognized (must be 'S' or 'W' for summer or winter time)
    try:
        tst_time = datetime.datetime(tst_Y, tst_m, tst_d, tst_H, tst_M, tst_S) # tst in datetime format
        tst_time = tst_time + datetime.timedelta(hours=dst)   # tst corrected with DST
        tst_time_dt = tst_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        # tst_time = tst_time.timestamp()   # tst in unix format
        tst_time_valid  = True
    except:
        tst_time_dt = ''
        tst_time_valid = False
    return [tst_time_dt, tst_time_valid, raw_time]

def listOfLines_to_dict(listOfLines):
    # try:
    #     # Make sure weird characters are converted properly.
    #     listOfLines = str(listOfLines, 'utf-8')
    # except TypeError:
    #     pass
    # print('starting "def listOfLines_to_dict(listOfLines)" now. len(listOfLines): ', len(listOfLines))
    logger.debug(f'starting "def listOfLines_to_dict(listOfLines)" now. len(listOfLines):  {len(listOfLines)}')
    # print(listOfLines[:4])

    telegram_dev00_01_dict = {}
    telegram_dev10_dict = {}
    telegram_dev00_01_listOfDict = []
    telegram_dev10_listOfDict = []

    for item in listOfLines:
        item = item.strip()
        value = '' # value of the key:value pair e.g. kWh or A
        obis = item[:item.find('(')]
        try:
            if item.startswith('/'):
                # print(f'if item.startswith: {item}')
                telegram_dev00_01_dict = {"measurement":"Electricity", "time":"", "fields":{}}
                telegram_dev10_dict = {"measurement":"Gas", "time":"", "fields":{}}
                electricity_timestamp = False
                gas_timestamp = False
                # continue
            elif item.startswith('!'):
                # check if timestamps are right format
                if electricity_timestamp:
                    if valid_logging_time(telegram_dev00_01_dict["time"], earliest_logging_date, latest_logging_date_now_plus_days):
                        telegram_dev00_01_listOfDict.append(telegram_dev00_01_dict)
                        telegram_dev00_01_dict = {"measurement":"Electricity", "time":"", "fields":{}}
                        electricity_timestamp = False
                if gas_timestamp:
                    if valid_logging_time(telegram_dev10_dict["time"], earliest_logging_date, latest_logging_date_now_plus_days):
                        telegram_dev10_listOfDict.append(telegram_dev10_dict)
                        telegram_dev10_dict = {"measurement":"Gas", "time":"", "fields":{}}
                        gas_timestamp = False
            elif obis == '0-0:96.1.4':    # 0-0:96.1.4(50213)
                value = str(item[11:16])
                telegram_dev00_01_dict["fields"]["Device_Id"] = value
            elif obis == '0-0:96.1.1':    # 0-0:96.1.1(3153414733313030303230353134)
                # from item[12] to next ) which is before last character of the line -max 96 chars-
                if len(item[12:])==1:
                    value = ""
                else:
                    value = str(item[12:-1])
                # value = item[11:39]
                # telegram_dev00_01_dict['fields']['Equipm_Id'] = value
            elif obis == '0-0:1.0.0' : # 0-0:1.0.0 : timestamp electricity :: 0-0:1.0.0(200306224224W)
                # 0-0:1.0.0(200306224224W); S:DST acitve; W: DST not active
                # DST: daylight savings time; tst: timestamp
                telegram_dev00_01_dict["fields"]["DST"] = item[22]
                # 0-0:1.0.0(200306224224W); S:DST acitve; W: DST not active
                # DST: daylight savings time; tst: timestamp
                # value = int(item[10:22])
                # telegram_dev00_01_dict['fields']['time_electr_orig'] = value   # timestamp as value identical to telegram
                raw_time_str = item[10:23]
                telegram_dev00_01_dict["time"] = timestamp_from_telegram(raw_time_str)[0]
                electricity_timestamp = timestamp_from_telegram(raw_time_str)[1]
                telegram_dev00_01_dict['fields']['raw_time'] = timestamp_from_telegram(raw_time_str)[2]
                telegram_dev00_01_dict['fields']['raw_time_float'] = float(timestamp_from_telegram(raw_time_str)[2])

            elif obis == '1-0:1.8.1':  # 1-0:1.8.1 : kWh afgenomen piek : afnameteller_kWh_piek 1-0:1.8.1(001255.196*kWh)
                value = float(item[10:20])
                telegram_dev00_01_dict['fields']['kWh_cons_T1'] = value
            elif obis == '1-0:1.8.2':   # 1-0:1.8.2 : kWh afgenomen dal : afnameteller_kWh_dal :: 1-0:1.8.2(002267.593*kWh)
                value = float(item[10:20])
                telegram_dev00_01_dict['fields']['kWh_cons_T2'] = value
            elif obis == '1-0:2.8.1':   # 1-0:2.8.1(000000.004*kWh)	Negative active energy (A+) in tariff T1 [kWh]
                value = float(item[10:20])
                telegram_dev00_01_dict['fields']['kWh_inj_T1'] = value
            elif obis == '1-0:2.8.2':   # 1-0:2.8.2(000000.175*kWh)	Negative active energy (A+) in tariff T2 [kWh]
                value = float(item[10:20])
                telegram_dev00_01_dict['fields']['kWh_inj_T2'] = value
            elif obis == '1.0:1.8.0':   # 1.0:1.8.0(000000.175*kWh)
                value = float(item[10:20])
                telegram_dev00_01_dict['fields']['kWh_cons'] = value
            elif obis == '1.0:2.8.0':   # 1.0:2.8.0(000000.175*kWh)
                value = float(item[10:20])
                telegram_dev00_01_dict['fields']['kWh_inj'] = value           
            elif obis == '0-0:96.14.0':   # 0-0:96.14.0(0002)
                value = int(item[12:16])
                telegram_dev00_01_dict['fields']['Tarif'] = value
            elif obis == '1-0:1.7.0':   # 1-0:1.7.0(02.138*kW)	Positive active instantaneous power (A+) [kW]
                value = float(item[10:16])
                telegram_dev00_01_dict['fields']['kW_A_cons'] = value 
            elif obis == '1-0:2.7.0':   # 1-0:2.7.0(00.000*kW)	Negative active instantaneous power (A-) [kW]
                value = float(item[10:16])
                telegram_dev00_01_dict['fields']['kW_A_inj'] = value 
            elif obis == '1-0:32.7.0':   # 1-0:32.7.0(225.7*V)	Instantaneous voltage (U) in phase L1 [V]
                value = float(item[11:16])
                telegram_dev00_01_dict['fields']['V_L1'] = value
            elif obis == '1-0:52.7.0':   # 1-0:52.7.0(225.6*V)	Instantaneous voltage (U) in phase L2 [V]
                value = float(item[11:16])
                telegram_dev00_01_dict['fields']['V_L2'] = value
            elif obis == '1-0:72.7.0':   # 1-0:72.7.0(229.9*V)	Instantaneous voltage (U) in phase L3 [V]
                value = float(item[11:16])
                telegram_dev00_01_dict['fields']['V_L3'] = value
            elif obis == '1-0:31.7.0':   # 1-0:31.7.0(003*A)	Instantaneous current (I) in phase L1 [A]
                value = float(item[11:14])
                telegram_dev00_01_dict['fields']['A_L1'] = value
            elif obis == '1-0:51.7.0':   # 1-0:51.7.0(003*A)	Instantaneous current (I) in phase L2 [A]
                value = float(item[11:14])
                telegram_dev00_01_dict['fields']['A_L2'] = value
            elif obis == '1-0:71.7.0':   # 1-0:71.7.0(003*A)	Instantaneous current (I) in phase L3 [A]
                value = float(item[11:14])
                telegram_dev00_01_dict['fields']['A_L3'] = value
            elif obis == '0-0:96.3.10':   # 0-0:96.3.10(1)	Breaker state 
                                        # (0) Disconnected, (1) Connected, (2) Ready_for_reconnection 
                value = int(item[12:13])
                telegram_dev00_01_dict['fields']['Breaker_state'] = value            
            elif obis == '0-0:17.0.0':   # 0-0:17.0.0(999.9*kW)	Limiter threshold 
                value = float(item[11:16])
                telegram_dev00_01_dict['fields']['Limiter_threshold'] = value
            elif obis == '1-0:31.4.0':   # 1-0:31.4.0(999*A)	Fuse supervision threshold L1 
                value = float(item[11:14])
                telegram_dev00_01_dict['fields']['Fuse_threshold_L1_A'] = value
            elif obis == '0-0:96.13.0':   # 0-0:96.13.0()	Messages
                # from item[12] to next ) which is before last character of the line
                if item[12]==')':
                    value = "None"
                else:
                    value = str(item[12:-1])
                telegram_dev00_01_dict["fields"]["Message"] = value
            elif obis == '0-1:24.1.0':   # 0-1:24.1.0(003)	M-Bus Device ID 2 
                value = int(item[11:14])
                telegram_dev10_dict["fields"]["Gas_Id2"] = value 
            elif obis == '0-0:96.13.0':   # 0-1:96.1.1(37464C4F32313139303933393538)	Device Id (Mbus identifier)
                # from item[11] to next ) which is before last character of the line -max 96 chars-
                if item[11:]==')':
                    value = "None"
                else:
                    value = str(item[11:-1])
                telegram_dev10_dict["fields"]["Gas_Mbus_Id"] = value           
            elif obis == '0-1:24.4.0(1)':   # 0-1:24.4.0(1)	Valve state (0=disconnected, 1=connected)
                value = int(item[11:12])
                telegram_dev10_dict["fields"]["Gas_valve_state"] = value 
            elif obis == '0-1:24.2.3':   # 0-1:24.2.3 : TSTW m³ afgenomen : gasteller_m3 :: 0-1:24.2.3(200306223959W)(01837.351*m3)
                value = int(item[11:23])
                telegram_dev10_dict["fields"]["time_gas_orig"] = value   # timestamp as value identical to telegram // but dubbel value

                value = float(item[26:35])
                telegram_dev10_dict["fields"]["Gas_m3"] = value
                telegram_dev10_dict["fields"]["DST"] = item[23]
                raw_time_str = item[11:24]
                telegram_dev10_dict["time"] = timestamp_from_telegram(raw_time_str)[0]
                gas_timestamp = timestamp_from_telegram(raw_time_str)[1]
                telegram_dev10_dict['fields']['raw_time'] = timestamp_from_telegram(raw_time_str)[2]
                telegram_dev10_dict['fields']['raw_time_float'] = float(timestamp_from_telegram(raw_time_str)[2])
        except Exception as exceptionMessage:
            # print('wrong item: ', item, telegram_dev00_01_dict["time"], exceptionMessage)
            logger.debug('wrong item: ', item, telegram_dev00_01_dict["time"], exceptionMessage)
            continue
    print('listOfDicts: ', len(telegram_dev00_01_listOfDict), len(telegram_dev10_listOfDict))
    logger.debug('listOfDicts: ', len(telegram_dev00_01_listOfDict), len(telegram_dev10_listOfDict))
    return [telegram_dev00_01_listOfDict, telegram_dev10_listOfDict]

def delete_keys_with_unchanged_values(actual_dict, previous_dict):
    # print('actual_dict: ', actual_dict)
    # print('previous_dict: ', previous_dict)
    previous_dict_keys_list = previous_dict.keys()
    actual_dict_keys_delete_list = []
    for key, value in actual_dict.items():
        if key in previous_dict_keys_list:
            # print(f"key: {key}, previous_dict_keys_list: {previous_dict_keys_list}")
            if previous_dict[key] == value : 
                actual_dict_keys_delete_list.append(key)
            else:
                previous_dict[key] = value
                pass
        else:
            previous_dict[key] = value
    for key in actual_dict_keys_delete_list:
        del actual_dict[key]
    # print('actual_dict cleaned: ', actual_dict)
    # print('previous_dict: ', previous_dict)
    return actual_dict, previous_dict

def valid_logging_time(dict_time, earliest_logging_date, latest_logging_date_now_plus_days):
    """
    dict_time: type = string format yyyy-mm-ddThh:mm:ssZ
    earliest_logging_date, latest_logging_date: type = datetime
    earliest_logging_date <= dict_time <= latest_logging_date
    return: True, False
    """
    try:
        latest_logging_datetime = datetime.datetime.now() + datetime.timedelta(int(latest_logging_date_now_plus_days))
        # print(f'latest_logging_datetime, type(latest_logging_datetime) {latest_logging_datetime}, {type(latest_logging_datetime)}')
        earliest_logging_date_datetime = datetime.datetime.strptime(earliest_logging_date,'%Y-%m-%d %H:%M:%S')
        # print(f'earliest_logging_date_datetime, type(earliest_logging_date_datetime) {earliest_logging_date_datetime}, {type(earliest_logging_date_datetime)}')
        dict_time_datetime = datetime.datetime.strptime(dict_time,'%Y-%m-%dT%H:%M:%SZ')
        # print(f'dict_time_datetime, type(dict_time_datetime) {dict_time_datetime}, {type(dict_time_datetime)}')
    except Exception as exceptionMessage:
        # print('wrong date, one of: ', dict_time, earliest_logging_date, latest_logging_date_now_plus_days, exceptionMessage)
        logger.debug('wrong date, one of: ', dict_time, earliest_logging_date, latest_logging_date_now_plus_days, exceptionMessage)
        return False
    return earliest_logging_date_datetime < dict_time_datetime < latest_logging_datetime

def clean_listOfDicts(listOfDicts):
    previous_electricity_fields_dict = {}
    previous_Gas_fields_dict = {}
    previous_Water_fields_dict = {}

    for el in listOfDicts[:]:
        # print(el)
        if 'measurement' in el.keys():
            if 'Electricity' in el.values():
                previous_fields_dict = previous_electricity_fields_dict
            elif 'Gas' in el.values():
                previous_fields_dict = previous_Gas_fields_dict
            elif 'Water' in el.values():
                previous_fields_dict = previous_Water_fields_dict
        else:
            pass   # no measurement
        # print(f"el['fields']: {el['fields']}")
        el['fields'], previous_fields_dict =  delete_keys_with_unchanged_values(el['fields'], previous_fields_dict)
    listOfDicts_cleaned = []
    for el in listOfDicts:
        if el['fields']: listOfDicts_cleaned.append(el)
    return listOfDicts_cleaned  # cleaned from unchanged field-values and empty field-directory


            
os_name = os.name
config = configparser.ConfigParser()
if os_name == 'posix':
    config.read('/home/pi/Pyprograms/dsmrreader_periodic_file.ini')
    os_config = 'posix_config'
elif os_name == 'nt':
    # config.read('Training/os/dsmr_parse_to_influx.ini')
    config.read('OneDrive\Documents\PythonScripts\dsmr_reader_production\dev\dsmrreader_periodic_file.ini')
    os_config = 'nt_config'
else:
    # *** SEND ERROR MAIL ***
    raise SystemExit(f'Something wrong, unknown OS. Config only for posix or nt. This is os: {os_name}')

# print('os_config: ', os_config)

main_path = os.path.join(config[os_config]['main_path'])
data_path = os.path.join(config[os_config]['data_path'])
log_path = os.path.join(config[os_config]['log_path'])
backup_path = os.path.join(config[os_config]['backup_path'])
json_path = os.path.join(config[os_config]['json_path'])


### LOGGER
# logging.config.fileConfig(logger_config_path)
#--- LOGGER ---
# Create a logger
this_module = 'dsmrreader_periodic_file'
logger = logging.getLogger(this_module)
logger.setLevel(config['logger_config']['logger_level'])
logfileFullPath = os.path.join(log_path, config['logger_config']['filename'])
print(f"logfileFullPath: {logfileFullPath}")
mode = config['logger_config']['mode']
maxBytes = int(config['logger_config']['maxBytes'])
backupCount = int(config['logger_config']['backupCount'])
# Create a rotating file handler
handler = logging.handlers.RotatingFileHandler(
    filename = logfileFullPath, mode = mode, maxBytes=maxBytes, backupCount=backupCount)
# Set the formatter for the handler
# formatter = logging.Formatter('%(asctime)s | %(process)d | %(name)s | %(levelname)8s | %(module)s | %(lineno)d | %(message)s')
formatter = logging.Formatter(config['logger_config']['format'])
handler.setFormatter(formatter)
# Add the handler to the logger
logger.addHandler(handler)
# # Test the logger
# msg = 'message as test'
# logger.info(f'Info message: {msg}')
# logger.debug(f'Debug message: {msg}')
# logger.warning(f'Warning message: {msg}')
logger.debug(f"*** Start program *** '{this_module}'.")
logger.debug(f'os_name detected: {os_name}.')
logger.debug(f'os_config: "{os_config}"')
# logger.debug(f'logger_config_path: "{logger_config_path}".')
logger.debug(f'path to txt telegrams files: {data_path}')
logger.debug(f'path to backup_path : {backup_path}.')


### initialize working variables
telegram_start_seen = False
buffer = ''
telegrams = ''
# integer in seconds; writes every period the read data into buffer e.g. 60
periodicityBuffer = int(config['working_var']['periodicityBuffer'])
# integer of max number of buffers to be appended in one file
# before writing into a new file e.g. 60
numberOfBuffersInFile = int(config['working_var']['numberOfBuffersInFile'])
timestamp_last_write_to_file = datetime.datetime.utcnow()
number_of_writes = 0
# initiating earliest and latest logging date
earliest_logging_date = config['working_var']['earliest_logging_date']
latest_logging_date_now_plus_days= int(config['working_var']['latest_logging_date_now_plus_days'])
time_between_influx_parses_s = float(config['working_var']['time_between_influx_parses_s'])



# exit(_ExitCode = 'Exit defined by user - testing')

dynamic_telegram_sum_lst = []
###

def compose_telegram_txt_files_and_dynamic_telegram_sum_lst(periodicityBuffer=periodicityBuffer, numberOfBuffersInFile=numberOfBuffersInFile):
    global dynamic_telegram_sum_lst
    ### import serial
    SLEEP = 0.0
    SERIAL_PORT = '/dev/ttyUSB0' #'/dev/serial/by-id/usb-FTDI_FT232R_USB_UART_AK47CKUP-if00-port0'
    SERIAL_BAUDRATE = 115200

    port=SERIAL_PORT
    baudrate=SERIAL_BAUDRATE
    bytesize=serial.EIGHTBITS
    parity=serial.PARITY_NONE
    stopbits=serial.STOPBITS_ONE
    xonxoff=1
    rtscts=0

    serial_handle = serial.Serial(
            port=port,
            baudrate=baudrate,
            bytesize=bytesize,
            parity=parity,
            stopbits=stopbits,
            xonxoff=xonxoff,
            rtscts=rtscts,
            timeout=10,  # Max time to wait for data.
            )
    serial_handle.close()
    serial_handle.open()
    # print('sleep 0.5s to open serial communication')
    logger.debug(f'sleep 0.5s to open serial communication')
    time.sleep(0.5)    # allowing system to set up serial communication
    # print(f"serial_handle.is_open: {serial_handle.is_open}")
    logger.debug(f"serial_handle.is_open: {serial_handle.is_open}")
    ###
    ### initialize working variables
    buffer = ''
    telegrams = ''
    # integer in seconds; writes every period the read data into buffer e.g. 60
    periodicityBuffer = int(config['working_var']['periodicityBuffer'])
    # # integer of max number of buffers to be appended in one file
    # # before writing into a new file e.g. 60
    numberOfBuffersInFile = int(config['working_var']['numberOfBuffersInFile'])
    timestamp_last_write_to_file = datetime.datetime.utcnow()
    number_of_writes = 0
    data_path = os.path.join(config[os_config]['data_path'])


    def readTelegram():
        telegram_start_seen = False
        data = ''
        while True:
                try:
                    # We use an infinite datalogger loop and signals to break out of it. Serial
                    # operations however do not work well with interrupts, so we'll have to check for E-INTR error.
                    data = serial_handle.readline()
                except serial.SerialException as error:
                    if str(error) == 'read failed: [Errno 4] Interrupted system call':
                        # If we were signaled to stop, we still have to finish our loop.
                        continue

                    # Something else and unexpected failed.
                    raise

                try:
                    # Make sure weird characters are converted properly.
                    data = str(data, 'utf-8')
                except TypeError:
                    pass

                if data.startswith('/'):
                    telegram_start_seen = True
                    buffer = ''

                if telegram_start_seen:
                    buffer += data

                if data.startswith('!') and telegram_start_seen:
                    # Keep connection open.
                    yield buffer

    buffer = ''
    telegrams = ''
    for buffer in readTelegram():
        # type buffer is string, string is written to .tmp file and later read as readlines() to convert to list of lines
        telegrams +=buffer
        # convert string to list of lines to become similar to the readlines() of written .tmp file  
        dynamic_telegram_sum_lst += buffer.splitlines()   
        if datetime.datetime.utcnow() > timestamp_last_write_to_file + datetime.timedelta(seconds=periodicityBuffer):
            
            with open(data_path+'/dsmr_buffer.tmp', 'a') as fh:
                fh.write(telegrams)
                telegrams = ''
                timestamp_last_write_to_file = datetime.datetime.utcnow()
                number_of_writes +=1
                # print('number of writes: ', number_of_writes)
                if number_of_writes >= numberOfBuffersInFile:
                    fileNameSuffix = datetime.datetime.utcnow().strftime('%y%m%d%H%M%SZ')
                    os.rename(data_path+'/dsmr_buffer.tmp', data_path+'/dsmr_'+fileNameSuffix+'.txt')
                    logger.debug(f'Rename file "{data_path}/dsmr_buffer.tmp" to {data_path}/dsmr_{fileNameSuffix}.txt')
                    number_of_writes = 0
    serial_handle.close()


def write_dynamic_telegram_sum_lst_to_influx():
    global dynamic_telegram_sum_lst

    ### initialize working variables
    # integer in seconds; writes every period the read data into buffer e.g. 60
    batch_size = int(config['influxdb']['batch_size'])
    max_json_listOfDicts = int(config['working_var']['max_json_listOfDicts'])
    json_path = os.path.join(config[os_config]['json_path'])
    len_dtc = ''
    listOfDicts = []
    telegrams_write_to_influx = []
    frozen_dynamic_telegram_sum_lst = []
    time_between_influx_parses_s = float(config['working_var']['time_between_influx_parses_s'])


    # PREPARE INFLUXDBCLIENT
    try:
        # host = "3.127.228.69"  # AWS server 'Ubuntu-1'; user 'Ubuntu' / root no password
        # port = 8086
        # username = "ubuntu"
        # password = "ivo"
        # dbname = "dsmr"
        # precision = "s"

        host = config['influxdb']['host']             # "3.127.228.69"  # AWS server 'Ubuntu-1'; user 'Ubuntu' / root no password
        port = config.getint('influxdb','port')       # 8086
        username = config['influxdb']['user']         # "ubuntu"
        password = config['influxdb']['password']     # "ivo"
        dbname = config['influxdb']['dbname']         # "dsmr"
        precision = config['influxdb']['precision']   # "s"

        client = InfluxDBClient(host=host, port=port, username=username, password=password, database=dbname)
    except Exception as exceptionMessage:
        # *** SEND ERROR MAIL ***
        logger.error(f'Something wrong, no connection to influxDBClient.Exception: {exceptionMessage}')
        # raise SystemExit('Something wrong, no connection to influxDBClient.')


    while True:
        len_dtc = len(dynamic_telegram_sum_lst)
        frozen_dynamic_telegram_sum_lst += dynamic_telegram_sum_lst[:len_dtc]
        dynamic_telegram_sum_lst = dynamic_telegram_sum_lst[len_dtc:]
        logger.debug(f'length dynamic_telegram_sum_lst before: {len_dtc+1}; after: {len(dynamic_telegram_sum_lst)}; frozen length: {len(frozen_dynamic_telegram_sum_lst)}')
        # print(f'telegrams_buffer: {buffer.splitlines()}')
        # print(f'telegrams_write_to_influx: {telegrams_write_to_influx}')
        if frozen_dynamic_telegram_sum_lst:
            listOfDicts = listOfLines_to_dict(frozen_dynamic_telegram_sum_lst)
            logger.debug(f'len(listOfDicts): {len(listOfDicts)}, [0]: {len(listOfDicts[0])}; [1]: {len(listOfDicts[1])}')
            # client.write_points(listOfDicts[0][0:2], time_precision = 's', )  #json_body measurements must be list of dict [{},{}]
            # client.write_points(listOfDicts[1][0:2], time_precision = 's', )  #json_body measurements must be list of dict [{},{}]
            listOfDicts[0] = clean_listOfDicts(listOfDicts[0])
            logger.debug(f'after clean_listOfDicts len(listOfDicts[0]): {len(listOfDicts[0])}')
            # print("listOfDicts to ingest[0]: ", listOfDicts[0][:5])
            try:
                write_0_succes = client.write_points(listOfDicts[0], time_precision = precision, batch_size=batch_size)
                logger.debug(f'Wrote {len(listOfDicts[0])} points of measurement: {listOfDicts[0][0]["measurement"]} with time: {listOfDicts[0][0]["time"]}')
            except Exception as exceptionMessage:
                logger.debug(f'Wrote len(listOfDicts[0]): {len(listOfDicts[0])}: {exceptionMessage}')

            listOfDicts[1] = clean_listOfDicts(listOfDicts[1])
            logger.debug(f'after clean_listOfDicts len(listOfDicts[1]): {len(listOfDicts[1])}')
            # print("listOfDicts to ingest[1]: ", listOfDicts[1][:5])
            try:
                write_1_succes = client.write_points(listOfDicts[1], time_precision = precision, batch_size=batch_size)
                logger.debug(f'Wrote {len(listOfDicts[1])} points of measurement {listOfDicts[1][0]["measurement"]} with times: {listOfDicts[1][0]["time"]}')
            except Exception as exceptionMessage:
                logger.debug(f'Wrote len(listOfDicts[1]): {len(listOfDicts[1])}: {exceptionMessage}')

            if write_0_succes and write_1_succes:
                try:
                    logger.info(f'Wrote {len(listOfDicts[0]) + len(listOfDicts[1])} points of measurement {listOfDicts[0][0]["measurement"]} and {listOfDicts[1][0]["measurement"]}')
                except:
                    pass # do nothing, just in case listOfDicts out of index
                listOfDicts = []
                frozen_dynamic_telegram_sum_lst = []
            else:
                try:
                    logger.error(f'could not write {len(listOfDicts[0])} and {len(listOfDicts[1])} e.a.: {listOfDicts[1][0]["measurement"]} with times: {listOfDicts[1][0]["time"]}')
                    #write to influx unsuccessful, don't clear telegrams
                except Exception as exceptionMessage:
                    logger.error(f'could not write to influxdb: {exceptionMessage}')
                    pass

            if listOfDicts:
                if len(listOfDicts[0]) > max_json_listOfDicts:
                    sum_listOfDicts = listOfDicts[0] + listOfDicts[1]
                    fileNameSuffix = datetime.datetime.utcnow().strftime('%y%m%d%H%M%SZ')
                    try:
                        fullPathfileNameJson = os.path.join(json_path, fileNameSuffix+'.json')
                        with open(fullPathfileNameJson, 'w') as fh:
                            json.dump(sum_listOfDicts, fh)
                    except Exception as exceptionMessage: 
                        logger.error(f'could not write json to file "{fullPathfileNameJson}"; EXCEPTION MESSAGE: {exceptionMessage}')
            else:
                logger.debug(f'Nothing to write to json: len(listOfDicts): {len(listOfDicts)}')
        else:
            logger.info(f'Wrote No telegrams, frozen_dynamic_telegram_sum_lst empty: len(frozen_dynamic_telegram_sum_lst): {len(frozen_dynamic_telegram_sum_lst)}')
            # print('No telegrams, buffer empty')
        time.sleep(time_between_influx_parses_s)    # take a brake between 2 telegram files; let Influx ingest; depends on specs server
        # print(f'Going back to work, slept for {time_between_influx_parses_s} seconds, next while cylce')
        logger.debug(f'Going back to work, slept for {time_between_influx_parses_s} seconds, next while cylce')

        client.close()

# if __name__ == '__main__':
t1 = Thread(target=compose_telegram_txt_files_and_dynamic_telegram_sum_lst)
t1.start()
t2 = Thread(target=write_dynamic_telegram_sum_lst_to_influx)
t2.start()

# exit(_ExitCode = 'Exit defined by user - testing')