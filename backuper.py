import os
import gzip
import boto3
import docker
import rotate_backups
from pathlib import Path
from pytz import timezone
from datetime import datetime

client = docker.from_env()
backup_folder = Path("/var/www/db_backup/backup")
os.makedirs(backup_folder.as_posix(), exist_ok=True)


def backup():
    for container in client.containers.list(filters={"expose": "5432"}):
        service_folder = backup_folder / container.name
        os.makedirs(service_folder.as_posix(), exist_ok=True)
        # dump postgres db
        code, result = container.exec_run(
            cmd=["pg_dumpall", "-c", "-U", "postgres"], tty=True
        )
        if code != 0:
            continue
        # store dump as gz archive
        date = datetime.now(timezone("US/Eastern")).strftime("%Y-%m-%d_%H:%M")
        file_path = (service_folder / "{}.sql.gz".format(date)).as_posix()
        with gzip.open(file_path, "wb") as f:
            f.write(result)

        session = boto3.Session(
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        )
        s3 = session.resource("s3")
        # Filename - File to upload
        # Bucket - Bucket to upload to (the top level directory under AWS S3)
        # Key - S3 object name (can contain subdirectories). If not specified then file_name is used
        s3.meta.client.upload_file(
            Filename=file_path, Bucket=BUCKET_NAME, Key=file_path.split("/")[-1]
        )


def rotate_old_backups():
    rotation_scheme = {
        "hourly": 4 * 2,  # last 2 days
        "daily": 7,  # last one week
        "weekly": 4,  # last one month
        "monthly": 4,
    }
    rotate_instance = rotate_backups.RotateBackups(
        rotation_scheme, prefer_recent=True, strict=False
    )
    for container in client.containers.list(filters={"expose": "5432"}):
        service_folder = backup_folder / container.name
        rotate_instance.rotate_backups(service_folder.as_posix())


backup()
rotate_old_backups()
