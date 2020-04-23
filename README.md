# glueetl
A command line tool to help deploy AWS Glue Jobs at ease :)


## Install
```bash
$ pip install glueetl
```


## How to develop a Glue job
You can develop a Glue job by following the steps below.

### Set up AWS Credentials and Region
Before you can deploy a Glue job to AWS Glue, you must set up AWS Credentials and Region.
```bash
$ vim ~/.aws/credentials

[default]
aws_access_key_id=<AWS_ACCESS_KEY_ID>
aws_secret_access_key=<AWS_SECRET_ACCESS_KEY>
region=<REGION>
```

### Initialize a Glue job
```bash
$ mkdir sample
$ cd sample
$ glueetl init
```
```
.
├── README.md
├── config.yaml
└── script.py
```

`config.yaml` includes job properties and currently it supports the following properties:  
```yaml
job: 
  name: sample-glue-job
  role_name: AWSGlueServiceRole
  script_location: s3://glue-job-scripts/sample-glue-job/script.py
  max_concurrent_runs: 10
  command_name: glueetl
  max_retries: 0
  timeout: 28800
  max_capacity: 10
  connections:
    - first_connection
    - second_connection
  trigger:
    name: trigger-sample-glue-job
    schedule: cron(5 * * * ? *)
  tags:
    key1: value1
    key2: value2

```

Please change default values in file `config.yaml` and write your job logic in file `script.py`.  

### Deploy a Glue job
```bash
$ cd sample
$ glueetl deploy
```

### Run a Glue job
```bash
$ cd sample
$ glueetl run --arg1=value1 --arg2=value2
```
