include .env

setup: start create-bucket

start:
	docker-compose up -d

create-bucket:
	awslocal s3api create-bucket --bucket $(BUCKET_NAME)