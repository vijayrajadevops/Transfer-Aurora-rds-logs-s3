import boto3
import gzip
import argparse
import sys
from botocore.exceptions import ClientError

def copy_logs_from_cloudwatch_to_s3(log_group_name, s3_bucket_name, aws_region, log_prefix="", min_size=0):
    cw_client = boto3.client('logs', region_name=aws_region)
    s3_client = boto3.client('s3', region_name=aws_region)

    config_file_name = f"{log_prefix}/config_file"
    last_written_time = 0
    copied_file_count = 0
    last_written_this_run = 0

    # Check if the S3 bucket exists and is accessible
    try:
        s3_client.head_bucket(Bucket=s3_bucket_name)
    except ClientError as e:
        error_code = int(e.response['ResponseMetadata']['HTTPStatusCode'])
        if error_code == 404:
            raise RuntimeError(f"Error: Bucket name {s3_bucket_name} not found")
        raise RuntimeError(f"Error: Unable to access bucket name, error: {e.response['Error']['Message']}")

    # Check for config file and read last_written_time
    try:
        s3_response = s3_client.get_object(Bucket=s3_bucket_name, Key=config_file_name)
        last_written_time = int(s3_response['Body'].read(s3_response['ContentLength']))
        print(f"Config file found. Last written time (checkpoint): {last_written_time}")
    except ClientError as e:
        error_code = int(e.response['ResponseMetadata']['HTTPStatusCode'])
        if error_code == 404:
            print("It appears this is the first log import, all files will be retrieved from RDS")
            min_size = 0  # We don't want to filter by file size on the first run
        else:
            raise RuntimeError(f"Error: Unable to access config file, error: {e.response['Error']['Message']}")
            
    # Get log streams updated after last_written_time
    paginator = cw_client.get_paginator('describe_log_streams')
    log_streams = []
    for page in paginator.paginate(
        logGroupName=log_group_name,
        orderBy='LastEventTime',
        descending=True
    ):
        log_streams.extend(page['logStreams'])

    # Filter log streams with events newer than checkpoint
    logs_to_process = [ls for ls in log_streams if ls.get('lastEventTimestamp', 0) > last_written_time]

    print(f"Total log streams to process: {len(logs_to_process)}")

    for log_stream in logs_to_process:
        log_stream_name = log_stream['logStreamName']
        print(f"Processing log stream: {log_stream_name}")

        # Paginate over log events in this stream after last_written_time
        event_paginator = cw_client.get_paginator('filter_log_events')
        for page in event_paginator.paginate(
            logGroupName=log_group_name,
            logStreamNames=[log_stream_name],
            startTime=last_written_time + 1
        ):
            for event in page['events']:
                event_time = event['timestamp']
                log_data = event['message']
                log_data_size = len(log_data.encode('utf-8'))

                if event_time <= last_written_time:
                    continue  # Skip already processed

                if log_data_size >= min_size:
                    compressed_log_data = gzip.compress(log_data.encode('utf-8'))
                    object_key = f"{log_prefix}/logs/{log_stream_name}/{event_time}.gz"

                    print(f"Compressing log event from stream '{log_stream_name}' with timestamp {event_time} (original size: {log_data_size} bytes)")

                    try:
                        s3_client.put_object(Bucket=s3_bucket_name, Key=object_key, Body=compressed_log_data)
                        copied_file_count += 1
                        print(f"Uploaded log file '{object_key}' to S3 (compressed size: {len(compressed_log_data)} bytes)")
                    except ClientError as e:
                        raise RuntimeError(f"Error uploading {object_key} to S3: {e.response['Error']['Message']}")

                    if event_time > last_written_this_run:
                        last_written_this_run = event_time

    print(f"Copied {copied_file_count} file(s) to S3")

    # Update the last written time in the config if any new logs were processed
    if last_written_this_run > 0:
        try:
            s3_client.put_object(
                Bucket=s3_bucket_name,
                Key=config_file_name,
                Body=str.encode(str(last_written_this_run))
            )
        except ClientError as e:
            err_msg = f"Error writing the config to S3 bucket, S3 ClientError: {e.response['Error']['Message']}"
            raise RuntimeError(err_msg)

        print(f"Wrote new config to '{config_file_name}' in S3 bucket '{s3_bucket_name}' with timestamp {last_written_this_run}")

    print("Log file export complete")

def parse_args():
    parser = argparse.ArgumentParser(description='Move logs from CloudWatch to S3.')
    parser.add_argument('--log-group-name', required=True, help='The CloudWatch Log Group name')
    parser.add_argument('--s3-bucket-name', required=True, help='The S3 bucket name')
    parser.add_argument('--aws-region', required=True, help='The AWS region')
    parser.add_argument('--log-prefix', default="", help='Prefix for storing logs in S3')
    parser.add_argument('--min-size', type=int, default=0, help='Minimum log size in bytes to upload')
    return parser.parse_args()

def lambda_handler(event, context):
    print(f"[Lambda] Invoked with event: {event}")
    copy_logs_from_cloudwatch_to_s3(
        event['log_group_name'],
        event['s3_bucket_name'],
        event['aws_region'],
        event.get('log_prefix', ""),
        event.get('min_size', 0)
    )

if __name__ == '__main__':
    args = parse_args()
    copy_logs_from_cloudwatch_to_s3(
        args.log_group_name,
        args.s3_bucket_name,
        args.aws_region,
        args.log_prefix,
        args.min_size
    )
