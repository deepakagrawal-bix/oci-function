import io
import json
import oci
import requests
from fdk import response
from requests.auth import HTTPBasicAuth
from datetime import datetime, timedelta
import csv

class Record:
    def __init__(self, date_time_str, count, timeseries):
        self.date_time_str = datetime.strptime(date_time_str, '%Y-%m-%d %H:%M:%S')
        self.count = count
        self.timeseries = timeseries

def get_last_date_from_primary_dataset(file_name):
    finalList = []
    with open(file_name, 'r') as f:
        reader = csv.reader(f, delimiter=',')
        headings = next(reader)
        for x in reader:
            finalList.append(x)

    map_id_vs_record_dict = {}
    record_list = []
    for date_time_str, count, timeseries in finalList:
        if timeseries in map_id_vs_record_dict:
            record_list = map_id_vs_record_dict[timeseries]
            new_record = Record(date_time_str, count, timeseries)
            record_list.append(new_record)
        else:
            new_list = [Record(date_time_str, count, timeseries)]
            map_id_vs_record_dict[timeseries] = new_list

    first_dict_key = list(map_id_vs_record_dict.keys())[0]
    last_date = \
        sorted(map_id_vs_record_dict.get(first_dict_key), key=lambda object1: object1.date_time_str, reverse=True)[
            0].date_time_str
    return map_id_vs_record_dict, last_date+timedelta(1)


def get_latest_dataset_dict(records_data):
    mapIdVsRecord = {}
    recordList = []
    for date_time_str, count, timeseries in records_data:
        if timeseries in mapIdVsRecord:
            recordList = mapIdVsRecord[timeseries]
            newRecord = Record(date_time_str, count, timeseries)
            recordList.append(newRecord)
        else:
            newList = [Record(date_time_str, count, timeseries)]
            mapIdVsRecord[timeseries] = newList

    for k, v in mapIdVsRecord.items():
        v.sort(key=lambda x: x.date_time_str)
        mapIdVsRecord[k] = v
    return mapIdVsRecord


def fill_missing_series_dataset(latest_dict, missing_series, number_of_days, start_date):
    curr_date = start_date
    missing_value_list = []
    i = 0
    while i < number_of_days:
        new_record = Record(curr_date.strftime('%Y-%m-%d %H:%M:%S'), 0, missing_series)
        missing_value_list.append(new_record)
        i += 1
        curr_date += timedelta(1)
    latest_dict[missing_series] = missing_value_list


def fill_missing_date_from_start(mapIdVsRecord, stDate, end_date_in):
    for k, v in mapIdVsRecord.items():
        length = len(mapIdVsRecord[k])
        tsRecordList = mapIdVsRecord[k]
        start_date_time = stDate
        end_date = end_date_in
        final_series_record_list = []
        tsRecordList.sort(key=lambda x: x.date_time_str, reverse=False)
        if (tsRecordList[0].date_time_str.date() - start_date_time.date()).days >= 1:
            tsRecordList.append(Record(start_date_time.strftime('%Y-%m-%d %H:%M:%S'), 0, tsRecordList[0].timeseries))
        if (end_date.date() - tsRecordList[length-1].date_time_str.date()).days >= 1:
            tsRecordList.append(Record(end_date.strftime('%Y-%m-%d %H:%M:%S'), 0, tsRecordList[0].timeseries))
        tsRecordList.sort(key=lambda x: x.date_time_str, reverse=False)
        length = len(tsRecordList)
        i = 1
        while i < length:
            diff_in_date = (tsRecordList[i].date_time_str.date() - start_date_time.date()).days
            if diff_in_date > 1:
                curr_date = start_date_time
                while diff_in_date > 1:
                    next_date = curr_date + timedelta(1)
                    missingDateRecord = Record(next_date.strftime('%Y-%m-%d %H:%M:%S'), 0, tsRecordList[i].timeseries)
                    final_series_record_list.append(missingDateRecord)
                    curr_date = next_date
                    diff_in_date -= 1
            start_date_time = tsRecordList[i].date_time_str
            i += 1
        for new_record in final_series_record_list:
            mapIdVsRecord[k].append(new_record)


def write_latest_data_into_csv_file(mapIdVsRecord, file_name, resp):
    finalList = []
    for k, v in mapIdVsRecord.items():
        for values in mapIdVsRecord[k]:
            finalList.append(values)

    with open(file_name, 'w') as f:
        writer = csv.writer(f, delimiter=',')
        writer.writerow(resp.json()['columnNames'])
        for x in finalList:
            writer.writerow([x.date_time_str, x.count, x.timeseries])


def get_missing_series(mapIdVsRecord, primary_map_id_vs_record_dict):
    missing_series = []
    for k in primary_map_id_vs_record_dict.keys():
        if mapIdVsRecord.get(k) is None:
            missing_series.append(k)
    return missing_series


def dump_primary_data_set_from_object_storage(object_storage_client, namespace, bucketName, fileName, primary_data_set_file_loc):
    get_obj = object_storage_client.get_object(namespace, bucketName, fileName)
    with open(primary_data_set_file_loc, 'wb') as f:
        for chunk in get_obj.data.raw.stream(1024 * 1024, decode_content=False):
            f.write(chunk)

def handler(ctx, data: io.BytesIO = None):
    signer = oci.auth.signers.get_resource_principals_signer()
    body = json.loads(data.getvalue())
    url = body.get("url")
    method = body.get("method")
    httpbody = body.get("body")
    headers = body.get("headers")
    user = body.get("user")
    password = body.get("password")
    auth = body.get("auth")
    data = body.get("data")
    target_objectname = body.get("target_objectname")
    target_bucket = body.get("target_bucket")
    primary_objectname = body.get("primary_objectname")
    

    if (target_bucket == None or target_objectname == None or url == None):
      resp_data = {"status":"400", "info":"Required parameters have not been supplied - target_objectname, target_bucket, url need to be supplied"}
      return response.Response(
            ctx, response_data=resp_data, headers={"Content-Type": "application/json"}
      )

    try:
      object_storage_client = oci.object_storage.ObjectStorageClient(config={}, signer=signer)
      namespace = object_storage_client.get_namespace().data

      resp = None
      if auth is not None and auth == "OCI":
        if method == None or method == "GET":
          resp = requests.get(url, auth=signer, headers=headers)
      else:
        session = requests.Session()
        if method == None or method == "POST":
          session = requests.Session()
          resp = requests.post(url, auth=HTTPBasicAuth(user, password), data=json.dumps(data), stream=True, headers=headers)


      records = resp.json()['rows']

      primary_data_set_file_loc = '/tmp/primary_data_set.csv'
      dump_primary_data_set_from_object_storage(object_storage_client, namespace, target_bucket, primary_objectname, primary_data_set_file_loc)

      primary_map_id_vs_record_dict, stDate = get_last_date_from_primary_dataset(primary_data_set_file_loc)
      current_date = datetime.now()
      current_date_string = current_date.strftime("%Y-%m-%d %H:%M:%S")
      current_date = datetime.strptime(current_date_string, "%Y-%m-%d %H:%M:%S")
      if (current_date.date() - stDate.date()).days >= 1:
          mapIdVsRecord = get_latest_dataset_dict(records)
          end_date_by_st_date = stDate + timedelta(days=6)
          end_date_by_current_date = current_date - timedelta(1)
          endDate = end_date_by_st_date if end_date_by_st_date < end_date_by_current_date else end_date_by_st_date
          fill_missing_date_from_start(mapIdVsRecord, stDate, endDate)
          
          missing_series = get_missing_series(mapIdVsRecord, primary_map_id_vs_record_dict)
          missing_series_days_count = (endDate-stDate).days + 1
          for series in missing_series:
              fill_missing_series_dataset(mapIdVsRecord, series, missing_series_days_count, stDate)

      latest_dataset_file_name = '/tmp/sample.csv'
      write_latest_data_into_csv_file(mapIdVsRecord, latest_dataset_file_name, resp)

      with open(latest_dataset_file_name, "rb") as in_file:
        object_storage_client.put_object(namespace,
                                     target_bucket,
                                     target_objectname, in_file)

      resp_data = {"status":"200"}
      return response.Response( ctx, response_data=resp_data, headers={"Content-Type": "application/json"})
    except oci.exceptions.ServiceError as inst:
      return response.Response( ctx, response_data=inst, headers={"Content-Type": "application/json"})
