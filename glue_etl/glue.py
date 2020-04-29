import os
import boto3
import yaml
import time
import signal


BREAK_STATES = [
  'STOPPED',
  'SUCCEEDED',
  'FAILED',
  'TIMEOUT'
]


_jobNameCache = ''
_jobRunIdCache = ''


def init_job():
  with open('config.yaml', 'w') as conf:
    conf.write("""job: 
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
  default_arguments:
    argument1: value1
    argument2: value2
  non_overridable_arguments:
    argument1: value1
    argument2: value2
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
    _update_job(conf)
  else:
    _create_job(conf)
  print("Deployed {} successfully".format(conf['name']))


def run_job(args):
  global _jobNameCache
  global _jobRunIdCache
  arguments = {}
  for a in args:
    k, v = a.split('=')
    arguments[k] = v

  conf = config()
  _jobNameCache = conf['name']

  client = boto3.client('glue')
  response = client.start_job_run(
    JobName = conf['name'],
    Arguments = arguments
  )
  jobRunId = response['JobRunId']
  _jobRunIdCache = jobRunId
  signal.signal(signal.SIGINT, _signal_handler)
  signal.signal(signal.SIGTERM, _signal_handler)
  print('JobName: {}'.format(conf['name']))
  print("  JobRunId: {}".format(jobRunId))
  while True:
    stats = _get_job_run(conf['name'], jobRunId)
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
    DefaultArguments=conf['default_arguments'] if 'default_arguments' in conf else {},
    NonOverridableArguments=conf['non_overridable_arguments'] if 'non_overridable_arguments' in conf else {},
    Connections={
      'Connections': conf['connections'] if 'connections' in conf else []
    },
    MaxRetries=conf['max_retries'] if 'max_retries' in conf else 0,
    Timeout=conf['timeout'] if 'timeout' in conf else 28800,
    MaxCapacity=conf['max_capacity'] if 'max_capacity' in conf else 10,
    Tags=conf['tags'] if 'tags' in conf else {},
    GlueVersion='1.0'
  )
  if 'trigger' in conf:
    response = client.create_trigger(
      Name=conf['trigger']['name'],
      Type='SCHEDULED',
      Schedule=conf['trigger']['schedule'],
      Actions=[
        {
          'JobName': conf['name']
        }
      ]
    )
  return response['Name']


def _update_job(conf):
  parts = conf['script_location'].split('//')[1].split('/')
  bucket = parts[0]
  key = '/'.join(parts[1:])
  boto3.resource('s3').meta.client.upload_file('script.py', bucket, key)

  client = boto3.client('glue')

  response = client.update_job(
    JobName=conf['name'],
    JobUpdate={
      'Role': conf['role_name'],
      'ExecutionProperty': {
        'MaxConcurrentRuns': conf['max_concurrent_runs'] if 'max_concurrent_runs' in conf else 1
      },
      'DefaultArguments': conf['default_arguments'] if 'default_arguments' in conf else {},
      'NonOverridableArguments': conf['non_overridable_arguments'] if 'non_overridable_arguments' in conf else {},
      'Command': {
        'Name': conf['command_name'] if 'command_name' in conf else 'glueetl',
        'ScriptLocation': conf['script_location'],
        'PythonVersion': '3'
      },
      'Connections': {
        'Connections': conf['connections'] if 'connections' in conf else []
      },
      'MaxRetries': conf['max_retries'] if 'max_retries' in conf else 0,
      'Timeout': conf['timeout'] if 'timeout' in conf else 28800,
      'MaxCapacity': conf['max_capacity'] if 'max_capacity' in conf else 10,
      'GlueVersion': '1.0'
    }
  )
  jobName = response['JobName']

  if 'trigger' in conf:
    try:
      response = client.get_trigger(
        Name=conf['trigger']['name']
      )
      if 'Trigger' in response:
        _update_trigger(conf['trigger'], conf['name'])
      else:
        _create_trigger(conf['trigger'], conf['name'])
    except:
      _create_trigger(conf['trigger'], conf['name'])

  return jobName


def _get_job_run(jobName, jobRunId):
  client = boto3.client('glue')
  response = client.get_job_run(
    JobName=jobName,
    RunId=jobRunId
  )
  return response['JobRun']


def _stop_job_run():
  client = boto3.client('glue')
  response = client.batch_stop_job_run(
    JobName=_jobNameCache,
    JobRunIds=[
      _jobRunIdCache,
    ]
  )
  if 'Errors' in response and len(response['Errors']) > 0:
    for err in response['Errors']:
      print('  ErrorMessage: {} ({})'.format(err['ErrorMessage'], err['ErrorCode']))
  else:
    print('\n  *Stopping {}'.format(_jobRunIdCache))


def _create_trigger(trigger, jobName):
  client = boto3.client('glue')
  client.create_trigger(
    Name=trigger['name'],
    Type='SCHEDULED',
    Schedule=trigger['schedule'],
    Actions=[
      {
        'JobName': jobName
      }
    ]
  )


def _update_trigger(trigger, jobName):
  client = boto3.client('glue')
  client.update_trigger(
    Name=trigger['name'],
    TriggerUpdate={
      'Schedule': trigger['schedule'],
      'Actions': [
        {
          'JobName': jobName
        }
      ]
    }
  )


def _signal_handler(signal_number, frame):
  _stop_job_run()
