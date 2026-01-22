"""
s3_service.py

This file contains ALL AWS S3 related logic.
FastAPI (or any other framework) should ONLY call functions from here.

Concepts used:
- Bucket
- Object (file)
- Key (full path inside bucket)
- Prefix (virtual folder)
"""

import boto3
from botocore.exceptions import ClientError
from app.config import AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_REGION


# -------------------------------------------------------------------
# Create S3 client
# -------------------------------------------------------------------

s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION
)


# -------------------------------------------------------------------
# 1. LIST BUCKETS
# -------------------------------------------------------------------

def list_buckets():
    response = s3.list_buckets()
    return [b["Name"] for b in response["Buckets"]]



# -------------------------------------------------------------------
# 2. LIST FILES & FOLDERS (GitHub-style)
# -------------------------------------------------------------------

def list_objects(bucket_name, prefix=""):
    """
    Lists folders and files under a given prefix.

    bucket_name: name of the bucket
    prefix: current path (example: 'docs/python/')

    Returns:
    - folders: ['docs/', 'images/']
    - files: ['file1.txt', 'notes.pdf']
    """

    folders = []
    files = []

    try:
        response = s3.list_objects_v2(
            Bucket=bucket_name,
            Prefix=prefix,
            Delimiter="/"   # IMPORTANT: makes folder-like behavior
        )

        # Virtual folders
        if "CommonPrefixes" in response:
            for item in response["CommonPrefixes"]:
                folders.append(item["Prefix"])

        # Files
        if "Contents" in response:
            for obj in response["Contents"]:
                if obj["Key"] != prefix:
                    files.append(obj["Key"])

    except ClientError as e:
        print("Error listing objects:", e)

    return folders, files


# -------------------------------------------------------------------
# 3. CREATE BUCKET
# -------------------------------------------------------------------

def create_bucket(bucket_name):
    """
    Creates a new S3 bucket.
    Bucket name must be globally unique.
    """

    try:
        s3.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={
                "LocationConstraint": AWS_REGION
            }
        )
        return True

    except ClientError as e:
        print("Create bucket failed:", e)
        return False


# -------------------------------------------------------------------
# 4. DELETE BUCKET (must be empty)
# -------------------------------------------------------------------

def delete_bucket(bucket_name):
    """
    Deletes a bucket.
    AWS requires the bucket to be EMPTY.
    This function deletes all objects first.
    """

    try:
        # Step 1: list all objects
        response = s3.list_objects_v2(Bucket=bucket_name)

        # Step 2: delete each object
        if "Contents" in response:
            for obj in response["Contents"]:
                s3.delete_object(
                    Bucket=bucket_name,
                    Key=obj["Key"]
                )

        # Step 3: delete the bucket
        s3.delete_bucket(Bucket=bucket_name)
        return True

    except ClientError as e:
        print("Delete bucket failed:", e)
        return False


# -------------------------------------------------------------------
# 5. UPLOAD FILE
# -------------------------------------------------------------------

def upload_file(bucket_name, file_obj, key):
    """
    Uploads a file to S3.

    bucket_name: target bucket
    file_obj: file object from UploadFile.file
    key: full path inside bucket
         example: 'docs/python/file.txt'
    """

    try:
        s3.upload_fileobj(
            Fileobj=file_obj,
            Bucket=bucket_name,
            Key=key
        )
        return True

    except ClientError as e:
        print("Upload failed:", e)
        return False


# -------------------------------------------------------------------
# 6. DELETE FILE
# -------------------------------------------------------------------

def delete_file(bucket_name, key):
    """
    Deletes a single file (object) from S3.
    """

    try:
        s3.delete_object(
            Bucket=bucket_name,
            Key=key
        )
        return True

    except ClientError as e:
        print("Delete file failed:", e)
        return False


# -------------------------------------------------------------------
# 7. CREATE FOLDER (Virtual)
# -------------------------------------------------------------------

def create_folder(bucket_name, folder_path):
    """
    Creates a virtual folder.
    In S3, folder = empty object ending with '/'
    """

    try:
        if not folder_path.endswith("/"):
            folder_path += "/"

        s3.put_object(
            Bucket=bucket_name,
            Key=folder_path
        )
        return True

    except ClientError as e:
        print("Create folder failed:", e)
        return False


# -------------------------------------------------------------------
# 8. COPY FILE
# -------------------------------------------------------------------

def copy_file(src_bucket, src_key, dest_bucket, dest_key):
    """
    Copies a file within S3.
    """

    try:
        s3.copy_object(
            CopySource={
                "Bucket": src_bucket,
                "Key": src_key
            },
            Bucket=dest_bucket,
            Key=dest_key
        )
        return True

    except ClientError as e:
        print("Copy failed:", e)
        return False


# -------------------------------------------------------------------
# 9. MOVE FILE (COPY + DELETE)
# -------------------------------------------------------------------

def move_file(src_bucket, src_key, dest_bucket, dest_key):
    """
    Moves a file inside S3.
    """

    try:
        copy_file(src_bucket, src_key, dest_bucket, dest_key)
        delete_file(src_bucket, src_key)
        return True

    except ClientError as e:
        print("Move failed:", e)
        return False
    

def delete_folder(bucket_name: str, folder_prefix: str):
    """
    Deletes a folder by deleting all objects with the given prefix.
    """
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket_name, Prefix=folder_prefix):
        if "Contents" in page:
            objects = [{"Key": obj["Key"]} for obj in page["Contents"]]
            s3.delete_objects(
                Bucket=bucket_name,
                Delete={"Objects": objects}
            )

