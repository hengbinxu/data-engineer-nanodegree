from typing import Optional

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    copy_stmt_template = (
        "COPY {target_table}\n"
        "FROM '{data_source}'\n"
        "CREDENTIALS 'aws_access_key_id={access_key};aws_secret_access_key={secret_key}'\n"
        "FORMAT AS JSON '{json_path}'"
    )

    @apply_defaults
    def __init__(self,
                redshift_conn_id: str,
                data_source: str,
                target_table: str,
                authorization_id: str,
                json_path: str='auto',
                region: Optional[str]=None,
                *args, 
                **kwargs):
        '''
        Load data from S3 to Redshift.

        args:
            redshift_conn_id(str): the connection id that you set in airflow metadb, 
                        which saves information how to connect Redshift
            data_source(str): S3 bucket
            
            target_table(str): the table which you load data
            
            authorization_id(str): the authorization id that you set in airflow metadb,
                        which saves AWS access_key_id and secret_key
            
            json_path(str): S3 bucket, which saves the file records how to map columns

            region(str): Specifies the AWS Region where the source data is located
        
        tip:
            Set reshift cluster and S3 bucket on the same region, it will accelerate your loading.
        '''
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.data_source = data_source
        self.target_table = target_table
        self.authorization_id = authorization_id
        self.region = region
        self.json_path = json_path

    def execute(self, context):
        # Get AWS credentials
        aws_hook = AwsHook(aws_conn_id=self.authorization_id)
        aws_credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info('Coping data from S3 to Redshift...')
        copy_stmt = self.copy_stmt_template.format(
            target_table=self.target_table,
            data_source=self.data_source,
            access_key=aws_credentials.access_key,
            secret_key=aws_credentials.secret_key,
            json_path=self.json_path,
            region=self.region
        )
        if self.region:
            copy_stmt = copy_stmt +\
                "\nREGION '{}'".format(self.region)

        redshift.run(copy_stmt)
        self.log.info((
            'Successfully stage data from S3 bucket: {data_source} to '
            'Redshift table: {target_table}'   
        ).format(
            data_source=self.data_source,
            target_table=self.target_table
        ))

# Reference:
# AWS Redshift Copy from Json format
# https://docs.aws.amazon.com/redshift/latest/dg/copy-usage_notes-copy-from-json.html




