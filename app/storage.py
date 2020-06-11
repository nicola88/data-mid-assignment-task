import logging
import os
import re
import tempfile
from datetime import datetime
from typing import List

import boto3


class StorageException(Exception):

    def __init__(self, message: str):
        super().__init__(message)


class StorageObject:

    def __init__(self, key: str, size: int, last_modified: datetime):
        self.key = key
        self.size = size
        self.last_modified = last_modified

    def __str__(self):
        return self.key


class StorageFile:

    def __init__(self, path: str, content_type: str):
        self.path = path
        self.content_type = content_type

    def __str__(self):
        return self.path

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        os.remove(self.path)


class StorageConfig:

    def __init__(self, bucket_name: str, folder_name: str, max_object_size: int, allowed_content_types: List[str]):
        self.bucket_name = bucket_name
        self.folder_name = folder_name
        self.max_object_size = max_object_size
        self.allowed_content_types = allowed_content_types


class StorageClient:

    ALLOWED_KEY_PATTERN = r"\d{4}-\d{2}-\d{2}\.tsv$"

    def __init__(self, config: StorageConfig):
        self.config = config
        self.client = boto3.client('s3')

    def __check_object(self, obj: StorageObject) -> bool:
        is_size_ok = 0 < obj.size <= self.config.max_object_size
        is_key_ok = re.search(StorageClient.ALLOWED_KEY_PATTERN, obj.key)
        logging.info(
            "Checked object '%s' size (%s) and key (%s)",
            obj, 'OK' if is_size_ok else 'KO', 'OK' if is_key_ok else 'KO'
        )
        return is_key_ok and is_size_ok

    def __check_file(self, file: StorageFile):
        is_file_content_ok = file.content_type in self.config.allowed_content_types
        if not is_file_content_ok:
            msg = "{} has an unsupported content type: {} (supported: {})".format(
                file, file.content_type, ", ".join(self.config.allowed_content_types).strip()
            )
            logging.error(msg)
            raise StorageException(msg)

    def get_objects(self) -> List[StorageObject]:
        objects = self.client.list_objects_v2(
            Bucket=self.config.bucket_name,
            Prefix=self.config.folder_name
        )
        logging.info("Found %s object(s) listed in the storage bucket", len(objects['Contents']))
        for obj in objects['Contents']:
            storage_object = StorageObject(key=obj['Key'], size=obj['Size'], last_modified=obj['LastModified'])
            if self.__check_object(storage_object):
                yield storage_object

    def download_object(self, obj: StorageObject) -> StorageFile:
        obj_metadata = self.client.head_object(Bucket=self.config.bucket_name, Key=obj.key)
        logging.info("Fetched metadata for object %s: %s", obj, obj_metadata)
        content_type = obj_metadata['ContentType']
        temp_file = tempfile.NamedTemporaryFile(delete=False)
        logging.info("Created temp file to store object content at %s", temp_file.name)
        self.client.download_fileobj(Bucket=self.config.bucket_name, Key=obj.key, Fileobj=temp_file)
        downloaded_file = StorageFile(path=temp_file.name, content_type=content_type)
        logging.info("Completed object download at %s", downloaded_file)
        self.__check_file(downloaded_file)
        return downloaded_file
