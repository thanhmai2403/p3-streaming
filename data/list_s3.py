import boto3
from botocore import UNSIGNED
from botocore.config import Config

s3 = boto3.client("s3", region_name="us-east-1", config=Config(signature_version=UNSIGNED))
r = s3.list_objects_v2(Bucket="openaq-data-archive", Prefix="records/csv.gz/", Delimiter="/", MaxKeys=5)
print([p["Prefix"] for p in r.get("CommonPrefixes", [])])
print(r.get("KeyCount"))