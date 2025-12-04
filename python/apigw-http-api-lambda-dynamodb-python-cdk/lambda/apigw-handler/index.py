# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import boto3
from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core import patch_all
import os
import json
import logging
import uuid

# Patch all AWS SDK calls for X-Ray tracing
patch_all()

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb_client = boto3.client("dynamodb")


@xray_recorder.capture('handler')
def handler(event, context):
    # Log request context for security investigations
    request_context = event.get("requestContext", {})
    identity = request_context.get("identity", {})
    
    logger.info(json.dumps({
        "message": "Processing request",
        "request_id": context.request_id,
        "source_ip": identity.get("sourceIp"),
        "user_agent": identity.get("userAgent"),
        "http_method": event.get("httpMethod"),
        "path": event.get("path"),
    }))
    
    table = os.environ.get("TABLE_NAME")
    logger.info(f"## Loaded table name from environemt variable DDB_TABLE: {table}")
    
    try:
        if event["body"]:
            item = json.loads(event["body"])
            logger.info(f"## Received payload: {item}")
            year = str(item["year"])
            title = str(item["title"])
            id = str(item["id"])
            dynamodb_client.put_item(
                TableName=table,
                Item={"year": {"N": year}, "title": {"S": title}, "id": {"S": id}},
            )
            message = "Successfully inserted data!"
            return {
                "statusCode": 200,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"message": message}),
            }
        else:
            logger.info("## Received request without a payload")
            dynamodb_client.put_item(
                TableName=table,
                Item={
                    "year": {"N": "2012"},
                    "title": {"S": "The Amazing Spider-Man 2"},
                    "id": {"S": str(uuid.uuid4())},
                },
            )
            message = "Successfully inserted data!"
            return {
                "statusCode": 200,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"message": message}),
            }
    except Exception as e:
        logger.error(json.dumps({
            "message": "Error processing request",
            "error": str(e),
            "error_type": type(e).__name__,
            "request_id": context.request_id,
        }))
        raise
