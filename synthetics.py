""" -----------------------------------------------------------
        Generates Csv report for CloudWatch Synthetics data
                    @Author: Omkar More
                    @Date: 30-Jan-2022
    -----------------------------------------------------------"""

import csv
import json
import json
import boto3
import random
import string
import datetime

def lambda_handler(event, context):
    try:

        s3 = boto3.resource('s3')
        
        my_bucket = s3.Bucket(event['bucket_name'])

        prefix_path = "/".join(list(event.values())[1:])

        dataList = []

        # Randomly Generate csv Filename
        csv_filename = "/tmp/"+event['web']+"".join(random.choices(string.ascii_lowercase,k=5))+".csv"
        csvBucketSavePath = "csvReport/"+event['web']+"/"+str(datetime.datetime.now())+"/"+csv_filename.rsplit('/',1)[1]
        print(csv_filename, csvBucketSavePath)

        for file in my_bucket.objects.filter(Prefix=prefix_path):
            if ((file.key).rsplit("/",1)[1]).split('.')[1] == "json":

                # Get Json File Path
                basePath = file.key 
                listPath = basePath.split("/")
                
                #  Read basePath Json
                basePathDic = {
                    "Region":listPath[1],
                    "WebSite":listPath[2],
                    "Year":listPath[3],
                    "Month":listPath[4],
                    "Fix":listPath[5],
                    "Day":listPath[6],
                    "Min-Sec-Milli":listPath[7],
                    "ReportStatus":listPath[8].split(".")[0] 
                }

                # Read Json File 
                content_object = s3.Object('cw-syn-results-828232558101-ap-south-1', basePath)
                file_content = content_object.get()['Body'].read().decode('utf-8')
                json_content = json.loads(file_content)

                # Filtering synthetics result
                resp = {
                    "startTime": json_content["startTime"], 
                    "endTime": json_content["endTime"],
                    "executionStatus": json_content["executionStatus"],
                    "executionError": json_content["executionError"],
                    "customerScript_failureReason":json_content["customerScript"]["failureReason"],
                    "customerScript_metricsPublished":json_content["customerScript"]["metricsPublished"]
                }

                # combine basepath and json data
                json_data = {**basePathDic, **resp}

                # append to List
                dataList.append(json_data)

        # write json data to file
        columns =  dataList[0].keys()
        with open(csv_filename,'w') as f:
            csv_file = csv.writer(f)
            csv_file.writerow(columns)
            csv_file.writerows([[item[col] for col in columns] for item in dataList])

        # put csv report to s3 
        my_bucket.put_object(
            Key = csvBucketSavePath, 
            Body = open(csv_filename, 'rb')
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps('CSV report Generated Successfully...')
        }

    except Exception as e:
        return {
            'statusCode': 400,
            'body': json.dumps('CSV report Generation Error...'+str(e))
        }
