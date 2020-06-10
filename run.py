import csv
import json
import logging
from datetime import datetime

import psycopg2

from app import config
from app.database import DatabaseClient
from app.storage import StorageClient, StorageConfig

try:
    logging.basicConfig(level=logging.INFO)
    storage_config = StorageConfig(
        bucket_name=config.AWS_BUCKET_NAME,
        folder_name=config.AWS_FOLDER_NAME,
        max_object_size=10240000,
        allowed_content_types=['text/tab-separated-values']
    )
    sc = StorageClient(storage_config)
    dc = DatabaseClient(
        config.DB_HOST,
        config.DB_PORT,
        config.DB_USER,
        config.DB_PASSWORD,
        config.DB_TABLE
    )
    dc.init_db()
    for so in sc.get_objects():
        sf = sc.download_object(so)
        with open(sf.path, newline='') as input_file:
            logging.info("Reading file {}".format(input_file.name))
            csv_file = csv.reader(input_file, delimiter='\t')
            next(csv_file)  # Skip headers
            for line in csv_file:
                logging.debug(line)
                dc.insert_event(
                    timestamp=datetime.strptime(line[0], "%Y-%m-%d %H:%M:%S.%f %z"),
                    session_id_md5=line[1],
                    event_name=line[2],
                    user_id_md5=line[3],
                    attributes=json.loads(line[4]) if line[4] else {}
                )
            dc.commit()
            logging.info("File {} loaded".format(input_file.name))
    dc.build_staging_tables()
    dc.build_final_tables()
except (Exception, psycopg2.Error) as error:
    # TODO Rollback
    logging.exception('An error occurred with PostgreSQL', error)

