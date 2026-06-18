import json
import boto3
import os
import datetime
import urllib.parse
import csv
import decimal
import time

dynamodb = boto3.resource('dynamodb')

# Main products table
table_name = os.environ.get('TABLE_NAME', 'products-kryss1234-cf')
table = dynamodb.Table(table_name)

# Inventory table
inventory_table_name = os.environ.get('INVENTORY_TABLE_NAME', 'ProductInventory-kryss1234')
inventory_table = dynamodb.Table(inventory_table_name)

# SQS Queue
SQS_QUEUE_NAME = 'products-queue-kryss1234-sqs'

# CloudWatch Log Group and Stream
LOG_GROUP = 'ProductsEventLogGroup'
LOG_STREAM = 'ProductEventStream'


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return float(obj) if obj % 1 != 0 else int(obj)
        return super(DecimalEncoder, self).default(obj)


def hello(event, context):
    print("Received event:", json.dumps(event, indent=2))
    return {
        "statusCode": 200,
        "body": json.dumps({"message": "Hello from Serverless!"})
    }


def get_all(event, context):
    try:
        response = table.scan()
        items = response.get('Items', [])
        return {
            "statusCode": 200,
            "body": json.dumps(items, default=str)
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }


def get_one(event, context):
    try:
        product_id = event['pathParameters']['id']
        product_resp = table.get_item(Key={'product_id': product_id})
        product = product_resp.get('Item')
        if not product:
            return {
                "statusCode": 404,
                "body": json.dumps({"error": "Product not found"})
            }
        inv_resp = inventory_table.query(
            KeyConditionExpression=boto3.dynamodb.conditions.Key('product_id').eq(product_id)
        )
        total_stock = sum(int(item['quantity']) for item in inv_resp.get('Items', []))
        product['current_stock'] = total_stock
        return {
            "statusCode": 200,
            "body": json.dumps(product, default=str)
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }


def create_one(event, context):
    try:
        body = json.loads(event['body'])
        item = {
            'product_id': body['product_id'],
            'product_name': body.get('product_name', ''),
            'brand_name': body.get('brand_name', ''),
            'price': decimal.Decimal(str(body.get('price', 0))),
            'quantity': decimal.Decimal(str(body.get('quantity', 0)))
        }
        table.put_item(Item=item)

        # ----- Send message to SQS -----
        sqs = boto3.resource('sqs', region_name='us-east-2')
        queue = sqs.get_queue_by_name(QueueName=SQS_QUEUE_NAME)
        queue.send_message(
            MessageBody=json.dumps(body, default=str)
        )
        print(f"✅ SQS message sent for product: {body['product_id']}")

        # ----- Push to CloudWatch Logs -----
        logs_client = boto3.client('logs', region_name='us-east-2')
        LOG_GROUP = "ProductsEventLogGroup"
        LOG_STREAM = "ProductEventStream"

        # Create log group if it doesn't exist
        try:
            logs_client.create_log_group(logGroupName=LOG_GROUP)
        except logs_client.exceptions.ResourceAlreadyExistsException:
            pass

        # Create log stream if it doesn't exist
        try:
            logs_client.create_log_stream(logGroupName=LOG_GROUP, logStreamName=LOG_STREAM)
        except logs_client.exceptions.ResourceAlreadyExistsException:
            pass

        # Push the log event
        log_event = {
            "event": "product_created",
            "pid": body.get("product_id"),
            "data": body
        }

        logs_client.put_log_events(
            logGroupName=LOG_GROUP,
            logStreamName=LOG_STREAM,
            logEvents=[
                {
                    'timestamp': int(time.time() * 1000),
                    'message': json.dumps(log_event, default=str)
                }
            ]
        )
        print("✅ Product creation event logged in CloudWatch.")

        return {
            "statusCode": 201,
            "body": json.dumps({"message": "Product created", "product": item}, default=str)
        }
    except Exception as e:
        print(f"❌ Error in create_one: {str(e)}")
        return {
            "statusCode": 400,
            "body": json.dumps({"error": str(e)})
        }


def update_one(event, context):
    try:
        product_id = event['pathParameters']['id']
        body = json.loads(event['body'])
        update_expr = "SET "
        expr_vals = {}
        for field in ['product_name', 'brand_name', 'price', 'quantity']:
            if field in body:
                update_expr += f" {field} = :{field},"
                if field in ['price', 'quantity']:
                    expr_vals[f":{field}"] = decimal.Decimal(str(body[field]))
                else:
                    expr_vals[f":{field}"] = body[field]
        if not expr_vals:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "No fields to update"})
            }
        update_expr = update_expr.rstrip(',')
        table.update_item(
            Key={'product_id': product_id},
            UpdateExpression=update_expr,
            ExpressionAttributeValues=expr_vals
        )
        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Product updated"})
        }
    except Exception as e:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": str(e)})
        }


def delete_one(event, context):
    try:
        product_id = event['pathParameters']['id']
        table.delete_item(Key={'product_id': product_id})
        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Product deleted"})
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }


def add_inventory(event, context):
    try:
        body = json.loads(event['body'])
        item = {
            'product_id': body['product_id'],
            'datetime': datetime.datetime.now().isoformat(),
            'quantity': decimal.Decimal(str(body['quantity'])),
            'remarks': body.get('remarks', '')
        }
        inventory_table.put_item(Item=item)
        return {
            "statusCode": 201,
            "body": json.dumps({"message": "Inventory entry added"})
        }
    except Exception as e:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": str(e)})
        }


def batch_create_products(event, context):
    print("📁 File uploaded to S3 - triggering batch create...")
    try:
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
        local_filename = f'/tmp/{key}'
        print(f"📥 Downloading s3://{bucket}/{key} to {local_filename}")
        os.makedirs(os.path.dirname(local_filename), exist_ok=True)
        s3_client = boto3.client('s3', region_name='us-east-2')
        s3_client.download_file(bucket, key, local_filename)
        print(f"✅ File downloaded successfully to {local_filename}")

        table_name = os.environ.get('TABLE_NAME', 'products-kryss1234-cf')
        dynamodb_resource = boto3.resource('dynamodb', region_name='us-east-2')
        table = dynamodb_resource.Table(table_name)
        count = 0
        with open(local_filename, 'r') as f:
            csv_reader = csv.DictReader(f)
            for row in csv_reader:
                item = {
                    'product_id': row.get('product_id', '').strip(),
                    'product_name': row.get('product_name', '').strip(),
                    'brand_name': row.get('brand_name', '').strip(),
                    'price': decimal.Decimal(row.get('price', 0)),
                    'quantity': decimal.Decimal(row.get('quantity', 0))
                }
                table.put_item(Item=item)
                count += 1
        print(f"✅ Successfully created {count} products in DynamoDB!")
        return {
            "statusCode": 200,
            "body": json.dumps({"message": f"Created {count} products"})
        }
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }


def batch_delete_products(event, context):
    print("🗑️ File uploaded to S3 (for_delete) - triggering batch delete...")
    try:
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
        local_filename = f'/tmp/{key}'
        print(f"📥 Downloading s3://{bucket}/{key} to {local_filename}")
        os.makedirs(os.path.dirname(local_filename), exist_ok=True)
        s3_client = boto3.client('s3', region_name='us-east-2')
        s3_client.download_file(bucket, key, local_filename)
        print(f"✅ File downloaded successfully to {local_filename}")

        table_name = os.environ.get('TABLE_NAME', 'products-kryss1234-cf')
        dynamodb_resource = boto3.resource('dynamodb', region_name='us-east-2')
        table = dynamodb_resource.Table(table_name)
        deleted_count = 0
        not_found_count = 0
        with open(local_filename, 'r') as f:
            csv_reader = csv.DictReader(f)
            for row in csv_reader:
                product_id = row.get('product_id', '').strip()
                if not product_id:
                    continue
                try:
                    response = table.delete_item(
                        Key={'product_id': product_id},
                        ReturnValues='ALL_OLD'
                    )
                    if response.get('Attributes'):
                        deleted_count += 1
                        print(f"✅ Deleted product: {product_id}")
                    else:
                        not_found_count += 1
                        print(f"⚠️ Product not found: {product_id}")
                except Exception as e:
                    print(f"❌ Error deleting {product_id}: {str(e)}")
        print(f"✅ Summary: {deleted_count} deleted, {not_found_count} not found")
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": f"Deleted {deleted_count} products, {not_found_count} not found"
            })
        }
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }


def receive_message_from_sqs(event, context):
    print("📨 Received SQS messages:", json.dumps(event, indent=2))
    try:
        s3_client = boto3.client('s3', region_name='us-east-2')
        bucket_name = "products-s3bucket-kryss1234"
        prefix = "product-logs/"
        for record in event['Records']:
            body = json.loads(record['body'])
            product_id = body.get('product_id', 'unknown')
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            filename = f"{prefix}{product_id}_{timestamp}.json"
            s3_client.put_object(
                Bucket=bucket_name,
                Key=filename,
                Body=json.dumps(body, indent=2, default=str),
                ContentType='application/json'
            )
            print(f"✅ Log stored in s3://{bucket_name}/{filename}")
        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Messages processed"})
        }
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }