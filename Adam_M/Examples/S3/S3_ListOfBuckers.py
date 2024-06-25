import boto3

#script does list of buckets in main S3 folder

s3_resource = boto3.resource("s3")
print("Hello, Amazon S3! Let's list your buckets:")
for bucket in s3_resource.buckets.all():
    print(f"\t{bucket.name}")
