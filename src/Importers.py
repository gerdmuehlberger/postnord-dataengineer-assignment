import pandas as pd
import boto3
from io import StringIO


class CSV_Importer:
    def __init__(self) -> None:
        pass

    def import_from_csv(self, filepath):
        try:
            return pd.read_csv(filepath)
        except Exception as e:
            raise e


class JSON_Importer:
    def __init__(self) -> None:
        pass

    def import_from_json(self, filepath):
        try:
            return pd.read_json(filepath, lines=True)
        except Exception as e:
            raise e

# this assumes credentials have been set up properly in the environment the job is run
class AWS_Importer:
    def __init__(self, s3_bucket_name: str, filename: str) -> None:
        self.s3_bucket_name = s3_bucket_name
        self.filename = filename

    def import_from_aws(self):
        try:
            s3_client = boto3.client('s3')
            obj = s3_client.get_object(Bucket=self.s3_bucket_name, Key=self.filename) 
            body = obj['Body']
            csv_as_string = body.read().decode('utf-8')
            
            return pd.read_csv(StringIO(csv_as_string))

        except Exception as e:
            raise e
