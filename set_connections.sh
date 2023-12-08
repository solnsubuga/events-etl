# create aws credentials
airflow connections add aws_credentials --conn-uri 'aws://<insert-access-key>:<insert-secret-key>@'

airflow connections add redshift --conn-uri '<insert-connection-id-here>'

## create variables
airflow variables set s3_final_bucket udacity-dend
airflow variables set s3_log_data_prefix log_data
airflow variables set s3_song_data_prefix song_data
airflow variables set metadata_path log_json_path.json

