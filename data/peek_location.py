import sys, gzip, io
import boto3
from botocore import UNSIGNED
from botocore.config import Config

loc = sys.argv[1]
s3 = boto3.client("s3", region_name="us-east-1", config=Config(signature_version=UNSIGNED))
B = "openaq-data-archive"

# liệt kê các năm
r = s3.list_objects_v2(Bucket=B, Prefix=f"records/csv.gz/locationid={loc}/", Delimiter="/")
years = [p["Prefix"] for p in r.get("CommonPrefixes", [])]
print("Năm có dữ liệu:", years)
if not years:
    sys.exit("Không có dữ liệu cho location này")

# lấy vài file ở năm cuối cùng
paginator = s3.get_paginator("list_objects_v2")
keys = []
for page in paginator.paginate(Bucket=B, Prefix=years[-1]):
    keys += [o["Key"] for o in page.get("Contents", [])]
print("Số file ở năm cuối:", len(keys))
print("File đầu / cuối:", keys[0], "|", keys[-1])

# đọc thử 1 file
body = s3.get_object(Bucket=B, Key=keys[-1])["Body"].read()
text = gzip.decompress(body).decode("utf-8")
print("\n".join(text.splitlines()[:5]))