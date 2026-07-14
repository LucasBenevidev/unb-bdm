import os
import sys
import boto3
from dotenv import load_dotenv

# Load configuration from .env file
load_dotenv()

# Retrieve settings
aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
bucket_name = os.getenv("S3_BUCKET_NAME", "unb-bdm-siorg")

# Safety checks
if not aws_access_key or "YOUR_ACCESS_KEY_ID_HERE" in aws_access_key:
    print("Error: AWS_ACCESS_KEY_ID is not configured in the .env file.")
    print("Please open the .env file in this directory and replace placeholders with your actual AWS keys.")
    sys.exit(1)

if not aws_secret_key or "YOUR_SECRET_ACCESS_KEY_HERE" in aws_secret_key:
    print("Error: AWS_SECRET_ACCESS_KEY is not configured in the .env file.")
    sys.exit(1)

print(f"Attempting S3 connection to bucket: '{bucket_name}' in region: '{region}'...")

try:
    # Initialize the S3 client
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=region
    )

    # File to upload
    test_key = "connection_test.txt"
    test_content = b"Connection verification successful! The Python script has successfully connected to your AWS S3 bucket."

    print(f"Uploading verification file '{test_key}'...")
    s3_client.put_object(
        Bucket=bucket_name,
        Key=test_key,
        Body=test_content,
        ContentType="text/plain"
    )
    print("Success! File uploaded successfully.")
    
    # Confirm bucket location
    location_resp = s3_client.get_bucket_location(Bucket=bucket_name)
    bucket_region = location_resp.get("LocationConstraint") or "us-east-1"
    print(f"Bucket is verified to be in region: {bucket_region}")
    
except Exception as e:
    print("\nConnection failed!")
    print(f"Error details: {e}")
    print("\nPlease verify that:")
    print("1. Your AWS Credentials in the .env file are correct and have no extra spaces.")
    print("2. The bucket name is correct.")
    print("3. Your IAM user policy allows s3:PutObject and s3:GetBucketLocation on this bucket.")
    sys.exit(1)
