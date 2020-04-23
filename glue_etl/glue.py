import os
import boto3
import yaml
import time


BREAK_STATES = [
  'STOPPED',
  'SUCCEEDED',
  'FAILED',
  'TIMEOUT'
]


def init_job():
  with open('config.yaml', 'w') as conf:
    conf.write("""job: 
  name: sample-glue-job
  role_name: AWSGlueServiceRoleSample
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
"""
    )
  with open('script.py', 'w') as script:
    script.write("# This is your Glue job's script.\n")
    script.write("# Please write your code in this file!\n")
  with open('README.md', 'w') as readme:
    readme.write("# sample-glue-job\n\n")
    readme.write("## Deploy\n\n")
    readme.write("```\nglueetl deploy\n```\n\n")
    readme.write("## Run\n\n")
    readme.write("```\nglueetl run --arg1=value1 --arg2=value2\n```\n")


def config():
  with open('config.yaml') as f:
    return yaml.load(f, Loader=yaml.FullLoader)['job']


def deploy_job():
  conf = config()
  job = get_job(conf['name'])
  if job['Deployed']:
    _update_job_script(job["ScriptLocation"])
  else:
    _create_job(conf)

  print("Deployed {} successfully".format(conf['name']))


def run_job(args):
  arguments = {}
  for a in args:
    k, v = a.split('=')
    arguments[k] = v
  conf = config()
  client = boto3.client('glue')
  response = client.start_job_run(
    JobName = conf['name'],
    Arguments = arguments
  )
  jobRunID = response['JobRunId']
  print('JobName: {}'.format(conf['name']))
  print("  JobRunId: {}".format(jobRunID))
  while True:
    stats = _get_job_run(conf['name'], jobRunID)
    print('  JobRunState: {}'.format(stats['JobRunState']))
    if 'ErrorMessage' in stats: 
      print('  ErrorMessage: {}'.format(stats['ErrorMessage']))
    if stats['JobRunState'] in BREAK_STATES:
      break
    time.sleep(30)


def get_job(jobName):
  client = boto3.client('glue')
  try:
    response = client.get_job(
      JobName=jobName
    )
    if 'Job' in response and response['Job']['Name'] == jobName:
      scriptLocation = response['Job']['Command']['ScriptLocation']
      return {'JobName': jobName, 'Deployed': True, 'ScriptLocation': scriptLocation}
    else:
      return {'JobName': jobName, 'Deployed': False, 'ScriptLocation': None}
  except:
    return {'JobName': jobName, 'Deployed': False, 'ScriptLocation': None}


def _create_job(conf):
  parts = conf['script_location'].split('//')[1].split('/')
  bucket = parts[0]
  key = '/'.join(parts[1:])
  boto3.resource('s3').meta.client.upload_file('script.py', bucket, key)

  client = boto3.client('glue')
  response = client.create_job(
    Name=conf['name'],
    Role=conf['role_name'],
    ExecutionProperty={
      'MaxConcurrentRuns': conf['max_concurrent_runs'] if 'max_concurrent_runs' in conf else 1
    },
    Command={
      'Name': conf['command_name'] if 'command_name' in conf else 'glueetl',
      'ScriptLocation': conf['script_location'],
      'PythonVersion': '3'
    },
    Connections={
      'Connections': conf['connections'] if 'connections' in conf else []
    },
    MaxRetries=conf['max_retries'] if 'max_retries' in conf else 0,
    Timeout=conf['timeout'] if 'timeout' in conf else 28800,
    MaxCapacity=conf['max_capacity'] if 'max_capacity' in conf else 10,
    Tags=conf['tags'] if 'tags' in conf else {},
    GlueVersion='1.0'
  )
  return response['Name']


def _update_job_script(scriptLocation):
  s3 = boto3.resource('s3')
  parts = scriptLocation.split('//')[1].split('/')
  bucket = parts[0]
  key = '/'.join(parts[1:])
  s3.meta.client.upload_file('script.py', bucket, key)


def _get_job_run(jobName, jobRunID):
  client = boto3.client('glue')
  response = client.get_job_run(
    JobName=jobName,
    RunId=jobRunID
  )
  return response['JobRun']
