import boto3
import os

# Check environment
print(f"AWS_PROFILE: {os.environ.get('AWS_PROFILE')}")

# Test S3 connection
try:
    s3 = boto3.client('s3')
    buckets = s3.list_buckets()
    print("✓ AWS connection successful!")
    print(f"Found {len(buckets['Buckets'])} buckets")
except Exception as e:
    print(f"✗ Error: {e}")

# Test Glue connection
try:
    glue = boto3.client('glue')
    databases = glue.get_databases()
    print(f"✓ Glue connection successful!")
    print(f"Found {len(databases['DatabaseList'])} databases")
    for db in databases['DatabaseList']:
        print(f"  - {db['Name']}")
except Exception as e:
    print(f"✗ Glue error: {e}")

exit()