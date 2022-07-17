import boto3
from botocore import exceptions
from botocore.exceptions import ClientError
import logging
from dotenv import load_dotenv
from abc import ABC
from datetime import datetime
import pandas as pd
import os 

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

load_dotenv('/opt/airflow/outputs/.env')

class AWS_Airflow(ABC):
    def __init__(self) -> None:
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id = os.getenv('AWS_ID'),
            aws_secret_access_key = os.getenv('AWS_KEY')
        )
        self.s3_resource = boto3.resource(
            's3',
            aws_access_key_id = os.getenv('AWS_ID'),
            aws_secret_access_key = os.getenv('AWS_KEY')
        )

    def _valida_bucket(self) -> bool:
            try:
                self.s3_client.list_buckets()['Buckets'][0]['Name']
            except IndexError:
                print('Não existe bucket nenhum')
                return True
            return False
        
    def _criar_bucket(self,nome) -> bool:
            try:
                self.s3_client.create_bucket(Bucket = nome)
            except ClientError as e:
                logging.error(e)
                return False
            return True

    def baixar_base(self):
            url = "https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/LEITOS/2022-07-12/esus-vepi.LeitoOcupacao_2020.csv"
            df = pd.read_csv(url, low_memory=False)
            df.to_csv("/opt/airflow/outputs/dados.csv",index=False)

    def iniciar(self) -> None:
            if self._valida_bucket():
                self._criar_bucket('marry-bucket-teste')
                print('Bucket criado')
            else:
                nome = self.s3_client.list_buckets()['Buckets'][0]['Name']
                print(f'Bucket já criado: {nome}')

    def enviar_arquivo(self) -> None:
            data_proc = datetime.now().strftime("%Y%m%d_%H%M%S")
            self.s3_resource.Bucket('marry-bucket-teste').upload_file(
                "/opt/airflow/outputs/dados.csv",
                f"airflow/dados/input/data_{data_proc}.csv"
            )

