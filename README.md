# aws-state-machine-etl
ETL based on State Machines and Lambda functions

## Requirements
* python 3.6
* pip
* Docker
* AWS credentials configured
* Serverless

### AWS Infrastructure
* S3 Deployment Bucket `aws s3api create-bucket --bucket nicor-deployment --region us-east-1`
* S3 staging Bucket `aws s3api create-bucket --bucket nicor-staging --region us-east-1`
* Secret Manager Key called `sample_etl`
  <pre>
  {
    "redshift_connection_string": "postgresql://your_user:your_password@your_database:5439/your_database",
    "redshift_iam_role": "arn:aws:iam::your_aws_account_id:role/your_redshift_role",
    "exchange_rate_app_id": "exchange_rate_app_id"
	}
  </pre>
* Step Machine IAM role with the permission to execute the lambda functions

## Architecture
* Lambda function called extractor to extract exchange rates and save to S3
* Lambda function to transform JSON exchange rates in CSV, in a format that can be easily copied from Redshift
* Lambda loader, that can copy a CSV(or whatever other supported format) to Redshift

## Deployment
<pre>
# prepare libs to package
./install_dependencies.sh
#
sls deploy
</pre>

## Backfill
<pre>
python backfill.py --start-date 2019-02-01 --end-date 2019-02-10 --state-machine your-state-machine-arn
</pre>


