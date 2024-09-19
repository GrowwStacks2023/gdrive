import logging
from fastapi import FastAPI, HTTPException
import os
import gdown
import boto3

# Initialize FastAPI app
app = FastAPI()

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Function to connect to AWS S3
def connectToAwsS3():
    # Hardcoded AWS credentials (for demo purposes, best practice is to use environment variables or AWS IAM roles)
    s3 = boto3.client(
        's3',
        aws_access_key_id='AKIA4MTWMRJ32J3UE4FK',
        aws_secret_access_key='MVfzchO7NotQ9uylfJ3HJL1Wh4Ro/w9ZxQ/6vQaR',
        region_name='us-east-1'  # Adjust to your region
    )
    return s3

# Function to download file from Google Drive
def download_file_from_google_drive(file_id: str):
    try:
        logger.info(f"Downloading file from Google Drive with ID {file_id}")
        file_url = f"https://drive.google.com/uc?export=view&id={file_id}"
        file = gdown.download(file_url, quiet=False)
        logger.info(f"File downloaded successfully: {file}")
        return file
    except Exception as e:
        logger.error(f"Error occurred while downloading file: {e}")
        raise HTTPException(status_code=500, detail=f"An error occurred while downloading the file: {e}")

# FastAPI route to download the file from Google Drive
@app.get("/download_file_from_google_drive/{file_id}")
async def download_file_from_google_drive_endpoint(file_id: str):
    try:
        file = download_file_from_google_drive(file_id)
        return {"file_path": file}
    except HTTPException as e:
        raise e

# Function to upload file to AWS S3
def upload_to_s3(file: str, bucket_name: str):
    try:
        object_key = os.path.basename(file)
        s3 = connectToAwsS3()

        logger.info(f"Uploading file to S3: {file} -> {object_key}")
        s3.upload_file(file, bucket_name, object_key)

        logger.info(f"File uploaded successfully to S3")
        response = s3.generate_presigned_url('get_object', Params={'Bucket': bucket_name, 'Key': object_key})
        logger.info(f"Generated presigned URL: {response}")

        # Remove the local file after uploading
        os.remove(file)
        logger.info(f"Removed temporary file: {file}")

        return response
    except Exception as ex:
        logger.error(f"Error occurred while uploading file to S3: {ex}")
        raise HTTPException(status_code=500, detail=f"An error occurred while uploading the file: {ex}")

# FastAPI route to upload file to AWS S3
@app.get("/upload_to_s3/{file_id}")
async def upload_to_s3_endpoint(file_id: str):
    try:
        logger.info(f"Uploading file to S3 with ID {file_id}")
        file = download_file_from_google_drive(file_id)
        bucket_name = 'briangdrivebucket'
        presigned_url = upload_to_s3(file, bucket_name)
        return {"presigned_url": presigned_url}
    except HTTPException as e:
        raise e

if __name__ == "__main__":
    import uvicorn
    # Run the FastAPI app on host 0.0.0.0 and port 8000
    uvicorn.run(app, host="0.0.0.0", port=8000)
