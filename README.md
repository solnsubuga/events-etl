# Etl pipeline

Automated Apache airflow ETL pipeline, create connections between Airflow and AWS by creating credentials, copying s3 data, leveraging connections and hooks, build s3 data to the redshift DAG.

# Setup

Login into the Airflow through webserver UI page

After logging in, add the needed connections through the Admin > Connections menu, namely the aws_credentials and redshift connections.

On the asame UI you need to set the following variables

```
key=value
s3_final_bucket = udacity-dend
s3_log_data_prefix = log_data
s3_song_data_prefix=song_data
metadata_path=log_json_path.json
```

or you can insert the missing credentials in and run the script `./set_connections.sh`
