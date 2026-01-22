"""
main.py

This file contains all FastAPI routes.
Responsibilities:
- Handle HTTP requests
- Render UI (Jinja2 templates)
- Call S3 logic from s3_service.py

NOTE:
No AWS logic should be written here.
"""

from fastapi import FastAPI, Request, UploadFile, File, Form
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates

from app import s3_service


# FastAPI app initialization


app = FastAPI()

# Template directory
templates = Jinja2Templates(directory="app/templates")


# HOME PAGE – LIST ALL BUCKETS


@app.get("/")
def home(request: Request):
    """
    Home page.
    Lists all S3 buckets.
    """
    buckets = s3_service.list_buckets()

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "view": "buckets",
            "buckets": buckets
        }
    )


# VIEW BUCKET CONTENT (GitHub-style navigation)


@app.get("/bucket/{bucket_name}")
def view_bucket(
    request: Request,
    bucket_name: str,
    prefix: str = ""
):
    """
    Displays folders and files inside a bucket.
    prefix = current virtual path (folder)
    """

    # Get folders and files from S3
    folders, files = s3_service.list_objects(bucket_name, prefix)

    # Build breadcrumb list from prefix
    # Example: "docs/images/" -> ["docs", "images"]
    breadcrumbs = prefix.strip("/").split("/") if prefix else []

    # Calculate parent path (for ".." navigation)
    parent_path = "/".join(prefix.rstrip("/").split("/")[:-1])
    if parent_path:
        parent_path += "/"

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "view": "objects",
            "bucket": bucket_name,
            "current_path": prefix,
            "breadcrumbs": breadcrumbs,
            "folders": folders,
            "files": files,
            "parent_path": parent_path
        }
    )


# CREATE NEW BUCKET


@app.post("/bucket/create")
def create_bucket(bucket_name: str = Form(...)):
    """
    Creates a new S3 bucket.
    """
    s3_service.create_bucket(bucket_name.strip())
    return RedirectResponse("/", status_code=303)


# DELETE BUCKET (empties first)


@app.post("/bucket/delete")
def delete_bucket(bucket_name: str = Form(...)):
    """
    Deletes a bucket.
    Bucket must be empty (handled in service).
    """
    s3_service.delete_bucket(bucket_name.strip())
    return RedirectResponse("/", status_code=303)


# UPLOAD FILE


@app.post("/upload")
def upload_file(
    bucket_name: str = Form(...),
    prefix: str = Form(""),
    file: UploadFile = File(...)
):
    """
    Uploads a file into the current folder.
    """
    key = prefix + file.filename
    s3_service.upload_file(bucket_name, file.file, key)

    return RedirectResponse(
        f"/bucket/{bucket_name}?prefix={prefix}",
        status_code=303
    )


# CREATE FOLDER


@app.post("/folder/create")
def create_folder(
    bucket_name: str = Form(...),
    prefix: str = Form(""),
    folder_name: str = Form(...)
):
    """
    Creates a virtual folder (key ending with '/').
    """
    folder_path = prefix + folder_name.strip("/") + "/"
    s3_service.create_folder(bucket_name, folder_path)

    return RedirectResponse(
        f"/bucket/{bucket_name}?prefix={prefix}",
        status_code=303
    )


# DELETE FILE


@app.post("/file/delete")
def delete_file(
    bucket_name: str = Form(...),
    key: str = Form(...),
    prefix: str = Form("")
):
    """
    Deletes a single file from S3.
    """
    s3_service.delete_file(bucket_name, key)

    return RedirectResponse(
        f"/bucket/{bucket_name}?prefix={prefix}",
        status_code=303
    )



# DELETE FOLDER

@app.post("/folder/delete")
def delete_folder(
    bucket_name: str = Form(...),
    folder_prefix: str = Form(...),
    prefix: str = Form("")
):
    """
    Deletes a folder (all objects under prefix).
    """
    s3_service.delete_folder(bucket_name, folder_prefix)

    return RedirectResponse(
        f"/bucket/{bucket_name}?prefix={prefix}",
        status_code=303
    )




# COPY FILE


@app.post("/file/copy")
def copy_file(
    bucket_name: str = Form(...),
    source_key: str = Form(...),
    destination_path: str = Form(""),
    prefix: str = Form("")
):
    """
    Copies a file inside the same bucket.
    """
    file_name = source_key.split("/")[-1]
    destination_key = destination_path + file_name

    s3_service.copy_file(
        bucket_name,
        source_key,
        bucket_name,
        destination_key
    )

    return RedirectResponse(
        f"/bucket/{bucket_name}?prefix={prefix}",
        status_code=303
    )


# MOVE FILE


@app.post("/file/move")
def move_file(
    bucket_name: str = Form(...),
    source_key: str = Form(...),
    destination_path: str = Form(""),
    prefix: str = Form("")
):
    """
    Moves a file inside the same bucket.
    (Copy + Delete)
    """
    file_name = source_key.split("/")[-1]
    destination_key = destination_path + file_name

    s3_service.move_file(
        bucket_name,
        source_key,
        bucket_name,
        destination_key
    )

    return RedirectResponse(
        f"/bucket/{bucket_name}?prefix={prefix}",
        status_code=303
    )
