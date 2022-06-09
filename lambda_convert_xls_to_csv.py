import json
import boto3
import xlrd
import io
import csv
import os
from datetime import datetime

s3 = boto3.client("s3")
s3_resource = boto3.resource("s3")

BUCKET_TARGET_PREFIX = "raw/excel-source"

def lambda_handler(event, context):
    
    if event:
        bucket_name = str(event["Records"][0]["s3"]["bucket"]["name"])
        source_filename = str(event["Records"][0]["s3"]["object"]["key"])
        print(source_filename)
        source_obj = s3.get_object(Bucket=bucket_name, Key=source_filename)
        source_content = source_obj["Body"].read()
        read_excel_data = io.BytesIO(source_content)
        book = xlrd.open_workbook(file_contents=read_excel_data.read(), encoding_override="utf-8")
        sheet = book.sheet_by_name('Sheet1')
        tmp_filepath = "/tmp/" + os.path.basename(source_filename)
        csv_filename = os.path.splitext(os.path.basename(source_filename))[0] + ".csv"
        print("source_filename:", source_filename)
        print("filepath:", tmp_filepath)
        print("csv_filename:", csv_filename)

        with open(tmp_filepath, 'w') as csvfile:
            wr = csv.writer(csvfile)
            for rownum in range(sheet.nrows):
                # date = sheet.row_values(rownum)[0] # Here 0 represents the Excel column where the date format is present.
                # if isinstance( date, float) or isinstance( date, int ):
                #     year, month, day, hour, minute, sec = xlrd.xldate_as_tuple(date, book.datemode)
                #     py_date = "%04d-%02d-%02d" % (year,month, day)
                #     wr.writerow([py_date] + sheet.row_values(rownum)[1:])
                # else:
                #     wr.writerow(sheet.row_values(rownum))
                wr.writerow(sheet.row_values(rownum))
            # To resolve raw date format issue, un-comment lines 35-41 and comment line 42. 
        
        s3.upload_file(tmp_filepath, bucket_name, '%s/%s' % (BUCKET_TARGET_PREFIX, csv_filename))