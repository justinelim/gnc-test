import os
import boto3
import openpyxl
import csv

s3 = boto3.resource('s3')
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    
    data_target_bucket = os.environ['TARGET_BUCKET']
    data_source_bucket = event['Records'][0]['s3']['bucket']['name']
    data_source_key = event['Records'][0]['s3']['object']['key']    # Get the object key
    
    print("data_source_bucket:", data_source_bucket)
    # Change the current working directory to /tmp
    os.chdir('/tmp')

    # Download the Excel file that was PUT into S3
    source_bucket_obj = s3.Bucket(data_source_bucket)
    source_bucket_obj.download_file(data_source_key, os.path.basename(data_source_key))

    # Open Excel Book/Worksheet
    wb = openpyxl.load_workbook(os.path.basename(data_source_key))
    ws = wb.worksheets[0]

    # CSV filename
    csv_filename = os.path.splitext(os.path.basename(data_source_key))[0] + ".csv"
    print("csv_filename:", csv_filename)

    # Create CSV file
    with open(csv_filename, 'w', newline="") as csvfile:
        writer = csv.writer(csvfile)
        for row in ws.rows:
            # Get Excel row data in list form
            row_data = [cell.value for cell in row]
            # Write to CSV
            writer.writerow(row_data)

    # Upload CSV file
    s3_client.upload_file(csv_filename, data_target_bucket, '%s/%s' % (os.environ['TARGET_BUCKET_PREFIX'], csv_filename))

    return None