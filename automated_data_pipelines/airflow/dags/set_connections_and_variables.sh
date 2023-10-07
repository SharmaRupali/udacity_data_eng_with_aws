#!/bin/bash
#
# TO-DO: run the follwing command and observe the JSON output: 
# we have to manually setup the connection the first time, only then the next command would produce any results
# airflow connections get aws_credentials -o json 
# 
#[{"id": "1", 
# "conn_id": "aws_credentials",
# "conn_type": "aws", 
# "description": "", 
# "host": "", 
# "schema": "", 
# "login": "AKIA4QE4NTH3R7EBEANN", 
# "password": "s73eJIJRbnqRtll0/YKxyVYgrDWXfoRpJCDkcG2m", 
# "port": null, 
# "is_encrypted": "False", 
# "is_extra_encrypted": "False", 
# "extra_dejson": {}, 
# "get_uri": "aws://AKIA4QE4NTH3R7EBEANN:s73eJIJRbnqRtll0%2FYKxyVYgrDWXfoRpJCDkcG2m@"
#}]
#
# Copy the value after "get_uri":
#
# For example: aws://AKIA4QE4NTH3R7EBEANN:s73eJIJRbnqRtll0%2FYKxyVYgrDWXfoRpJCDkcG2m@
#
# 
# [{
#     "id": "67", 
#     "conn_id": "aws_credentials", 
#     "conn_type": "aws", 
#     "description": "", 
#     "host": "", 
#     "schema": "", 
#     "login": "AKIAR52K74ML45RHMCGD", 
#     "password": "skJWKU/kjPkIhg8HMzxkIhJd7/FSOBcJ9bZNMhLb", 
#     "port": null, 
#     "is_encrypted": "True", 
#     "is_extra_encrypted": "True", 
#     "extra_dejson": {}, 
#     "get_uri": "aws://AKIAR52K74ML45RHMCGD:skJWKU%2FkjPkIhg8HMzxkIhJd7%2FFSOBcJ9bZNMhLb@"
# }]
# 
#
# TO-DO: Update the following command with the URI and un-comment it:
#
airflow connections add aws_credentials --conn-uri 'aws://AKIAR52K74ML45RHMCGD:skJWKU%2FkjPkIhg8HMzxkIhJd7%2FFSOBcJ9bZNMhLb@'
#
#
# TO-DO: run the follwing command and observe the JSON output: 
# we have to manually setup the connection the first time, only then the next command would produce any results
# airflow connections get redshift -o json
# 
# [{"id": "3", 
# "conn_id": "redshift", 
# "conn_type": "redshift", 
# "description": "", 
# "host": "default.859321506295.us-east-1.redshift-serverless.amazonaws.com", 
# "schema": "dev", 
# "login": "awsuser", 
# "password": "R3dsh1ft", 
# "port": "5439", 
# "is_encrypted": "False", 
# "is_extra_encrypted": "False", 
# "extra_dejson": {}, 
# "get_uri": "redshift://awsuser:R3dsh1ft@default.859321506295.us-east-1.redshift-serverless.amazonaws.com:5439/dev"}]
#
# Copy the value after "get_uri":
#
# For example: redshift://awsuser:R3dsh1ft@default.859321506295.us-east-1.redshift-serverless.amazonaws.com:5439/dev
#
# 
# [{
#     "id": "68", 
#     "conn_id": "redshift", 
#     "conn_type": "redshift", 
#     "description": "", 
#     "host": "default-workgroup.132764394263.us-west-2.redshift-serverless.amazonaws.com", 
#     "schema": "dev", 
#     "login": "awsuser", 
#     "password": "R3dsh1ft", 
#     "port": "5439", 
#     "is_encrypted": "True", 
#     "is_extra_encrypted": "True", 
#     "extra_dejson": {}, 
#     "get_uri": "redshift://awsuser:R3dsh1ft@default-workgroup.132764394263.us-west-2.redshift-serverless.amazonaws.com:5439/dev"
# }]
# 
#
# TO-DO: Update the following command with the URI and un-comment it:
#
airflow connections add redshift --conn-uri 'redshift://awsuser:R3dsh1ft@default-workgroup.132764394263.us-west-2.redshift-serverless.amazonaws.com:5439/dev'
#
# TO-DO: update the following bucket name to match the name of your S3 bucket and un-comment it:
#
airflow variables set s3_bucket udacity.rugo.automated.pipelines
#
# TO-DO: un-comment the below line:
#
airflow variables set s3_prefix data-pipelines

airflow users create --username admin --firstname Rupali --lastname Sharma --role Admin --email student@udacity.com --password admin