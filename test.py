#!/bin/env python3

import requests
import json
import re
import sys
import time
import traceback
from datetime import datetime

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

username = ''
password = ''
hostname = ''

def print_err(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

def find_property(properties, name):
    return next((e for e in properties if e['Name'] == name), None)

def is_4xx_error(code):
    return code < 500 and code >= 400

class ApiClient:
    def __init__(self, hostname, username, password):
        self.hostname = hostname
        self.username = username
        self.password = password
        self.apibase = 'https://%s/hpc' % hostname

    def url(self, path):
        return self.apibase + path

    def invoke(self, method, path, **kwargs):
        url = self.url(path)
        res = requests.request(method, url, verify=False, auth=(username, password), **kwargs)
        msg = '''
%s %s
Headers: %s
Body: %s

Code: %d
Headers: %s
Body: %s
        ''' % (
            res.request.method, res.request.url, res.request.headers, res.request.body,
            res.status_code, res.headers, res.text
        )
        print_err(msg)
        return res

class TestBase:
    def __init__(self, api_client, title):
        self.api_client = api_client
        self.title = title
        self.passed = None

    def start(self):
        try:
            print('# %s' % self.title)
            self.run()
        except AssertionError as error:
            self.passed = False
            print('Failed with error: %s' % str(error))
            traceback.print_exc()
        else:
            self.passed = True
            print('Passed!')

    def run(self):
        pass

class QueryClusterTest(TestBase):
    def run(self):
        print('## Query cluster version')
        res = self.api_client.invoke('GET', '/cluster/version')
        assert res.ok
        body = res.json()
        assert isinstance(body, str)
        assert re.match('\d+\.\d+\.\d+\.\d+', body)

        print('## Query active head node')
        res = self.api_client.invoke('GET', '/cluster/activeHeadNode')
        assert res.ok
        body = res.json()
        assert isinstance(body, str)
        assert body

        print('## Query datetime format')
        res = self.api_client.invoke('GET', '/cluster/info/dateTimeFormat')
        assert res.ok
        body = res.json()
        assert isinstance(body, str)
        assert body

class QueryNodeTest(TestBase):
    def run(self):
        print('## Query nodes')
        params = { '$filter': 'NodeState eq Online', 'rowsPerRead': 2 }
        res = self.api_client.invoke('GET', '/nodes', params=params)
        assert res.ok
        body = res.json()
        assert isinstance(body, list)
        assert body and len(body) <= params['rowsPerRead']
        node = body[0]
        assert node and node['Properties']
        prop = find_property(node['Properties'], 'Name')
        assert prop and prop['Value']

        node_name = prop['Value']

        print('## Query node %s' % node_name)
        res = self.api_client.invoke('GET', '/nodes/%s' % node_name)
        assert res.ok
        body = res.json()
        assert isinstance(body, list)
        assert body
        prop = find_property(body, 'Name')
        assert prop and prop['Value'] == node_name

        invalid_node_name = 'thisisaninvalidnodename'

        print('## Query invalid node %s' % invalid_node_name)
        res = self.api_client.invoke('GET', '/nodes/%s' % invalid_node_name)
        assert is_4xx_error(res.status_code)

        print('## Query node groups')
        res = self.api_client.invoke('GET', '/nodes/groups')
        assert res.ok
        body = res.json()
        assert isinstance(body, list)
        assert body

        print('## Query node group HeadNodes')
        res = self.api_client.invoke('GET', '/nodes/groups/HeadNodes')
        assert res.ok
        body = res.json()
        assert isinstance(body, list)
        assert body and node_name in body

        invalid_node_group = 'thisisaninvalidnodegroup'

        print('## Query invalid node group %s' % invalid_node_group )
        res = self.api_client.invoke('GET', '/nodes/groups/%s' % invalid_node_group)
        assert is_4xx_error(res.status_code)

class JobOperationTest(TestBase):
    run_until_cancel_job = '''
<Job Name="RunUntilCanceledJob" MinCores="1" MaxCores="1" RunUntilCanceled="True" >
  <Tasks>
    <Task Name="TestTaskInXML" CommandLine="echo Hello" MinCores="1" MaxCores="1" />
  </Tasks>
</Job>
    '''

    simple_job = '''
<Job Name="SimpleJob">
  <Tasks>
    <Task Name="TestTaskInXML" CommandLine="echo Hello" MinCores="1" MaxCores="1" />
  </Tasks>
</Job>
    '''

    def create_simple_job(self):
        return self.create_job(self.__class__.simple_job)

    def create_run_until_cancel_job(self):
        return self.create_job(self.__class__.run_until_cancel_job)

    def create_job(self, xml_job):
        print('## Create a job from xml')

        res = self.api_client.invoke('POST', '/jobs/jobFile', json=xml_job)
        assert res.ok
        body = res.json()
        assert isinstance(body, int)
        job_id = int(body)

        print('## Submit job %d' % job_id)
        res = self.api_client.invoke('POST', '/jobs/%d/submit' % job_id)
        assert res.ok

        return job_id

    def wait_job(self, job_id, state):
        print('## Wait job %d to be %s' % (job_id, state))
        ready = None
        for _ in range(30):
            res = self.api_client.invoke('GET', '/jobs/%d?properties=Id,State,ErrorMessage' % job_id)
            assert res.ok
            prop = find_property(res.json(), 'State')
            if prop['Value'] == state:
                ready = True
                break
            else:
                time.sleep(1)
        assert ready
        return res

class CancelJobTest(JobOperationTest):
    def run(self):
        job_id = self.create_run_until_cancel_job()
        self.wait_job(job_id, 'Running')

        print('## Cancel job %d' % job_id)
        msg = "Canceled by test."
        res = self.api_client.invoke('POST', '/jobs/%d/cancel' % job_id, json=msg)
        assert res.ok

        res = self.wait_job(job_id, 'Canceled')
        prop = find_property(res.json(), 'ErrorMessage')
        assert prop and msg in prop['Value']

class FinishJobTest(JobOperationTest):
    def run(self):
        job_id = self.create_run_until_cancel_job()
        self.wait_job(job_id, 'Running')

        print('## Finish job %d' % job_id)
        msg = "Finished by test."
        res = self.api_client.invoke('POST', '/jobs/%d/finish' % job_id, json=msg)
        assert res.ok

        res = self.wait_job(job_id, 'Finished')
        # NOTE: Error message is not set for "Finished" job?
        # prop = find_property(res.json(), 'ErrorMessage')
        # assert prop and msg in prop['Value']

class RequeueJobTest(JobOperationTest):
    def run(self):
        job_id = self.create_run_until_cancel_job()
        self.wait_job(job_id, 'Running')

        print('## Cancel job %d' % job_id)
        res = self.api_client.invoke('POST', '/jobs/%d/cancel' % job_id, json="Canceled by BVT tester.")
        assert res.ok

        self.wait_job(job_id, 'Canceled')

        print('## Requeue job %d' % job_id)
        res = self.api_client.invoke('POST', '/jobs/%d/requeue' % job_id)
        assert res.ok

        self.wait_job(job_id, 'Running')

        print('## Finish job %d' % job_id)
        res = self.api_client.invoke('POST', '/jobs/%d/finish' % job_id, json="Finished by BVT tester.")
        assert res.ok

        self.wait_job(job_id, 'Finished')

class CreateJobTest(JobOperationTest):
    def run(self):
        print('## Create a job')
        job = [
            { 'Name': 'Name', 'Value': 'TestJob' },
        ]
        res = self.api_client.invoke('POST', '/jobs', json=job)
        assert res.ok
        body = res.json()
        assert isinstance(body, int)
        job_id = int(body)

        print('## Query job %d' % job_id)
        res = self.api_client.invoke('GET', '/jobs/%d?properties=Id,State' % job_id)
        assert res.ok
        body = res.json()
        assert isinstance(body, list) and body
        prop = find_property(body, 'Id')
        assert prop and int(prop['Value']) == job_id
        prop = find_property(body, 'State')
        assert prop and prop['Value'] == 'Configuring'

        print('## Add a task to job')
        task = [
            { 'Name': 'Name', 'Value': 'TestTask' },
            { 'Name': 'CommandLine', 'Value': 'echo Hello' },
        ]
        res = self.api_client.invoke('POST', '/jobs/%d/tasks' % job_id, json=task)
        assert res.ok
        body = res.json()
        assert isinstance(body, int)

        print('## Add another task to job')
        task = [
            { 'Name': 'Name', 'Value': 'TestTask2' },
            { 'Name': 'CommandLine', 'Value': 'echo World' },
        ]
        res = self.api_client.invoke('POST', '/jobs/%d/tasks' % job_id, json=task)
        assert res.ok
        body = res.json()
        assert isinstance(body, int)

        print('## Submit the job')
        res = self.api_client.invoke('POST', '/jobs/%d/submit' % job_id)
        assert res.ok

        print('## Query job %d' % job_id)
        res = self.api_client.invoke('GET', '/jobs/%d?properties=Id,State' % job_id)
        assert res.ok
        body = res.json()
        assert isinstance(body, list) and body
        prop = find_property(body, 'Id')
        assert prop and int(prop['Value']) == job_id
        prop = find_property(body, 'State')
        assert prop and prop['Value'] in ['Submitted', 'Validating', 'Queued', 'Dispatching', 'Running', 'Finishing', 'Finished']

        self.wait_job(job_id, 'Finished')

        print('## Query tasks of job %d' % job_id)
        res = self.api_client.invoke('GET', '/jobs/%d/tasks' % job_id, params={ 'properties': 'TaskId,State,ExitCode' })
        assert res.ok
        body = res.json()
        assert isinstance(body, list) and len(body) == 2
        for t in body:
            prop = find_property(t['Properties'], 'State')
            assert prop and prop['Value'] == 'Finished'

class QueryJobTest(JobOperationTest):
    def run(self):
        now = datetime.utcnow()
        for _ in range(4):
            job_id = self.create_simple_job()

        print('## Query jobs')
        params = {
            'rowsPerRead': 3,
            'owner': self.api_client.username,
            'properties': 'Id,Owner,ChangeTime',
            # Server datetime format is "M/d/yyyy h:mm:ss tt"
            '$filter': 'ChangeTimeFrom eq %s' % now.strftime('%m/%d/%Y %H:%M:%S')
        }
        res = self.api_client.invoke('GET', '/jobs', params=params)
        assert res.ok
        body = res.json()
        assert isinstance(body, list)
        assert body and len(body) == params['rowsPerRead']
        job = body[0]
        assert job and job['Properties']
        prop = find_property(job['Properties'], 'Id')
        assert prop and prop['Value']
        prop = find_property(job['Properties'], 'Owner')
        assert prop and prop['Value'].lower() == params['owner'].lower()
        prop = find_property(job['Properties'], 'ChangeTime')
        assert prop and prop['Value']
        assert res.headers['x-ms-continuation-QueryId']

        while True:
            params['queryId'] = res.headers['x-ms-continuation-QueryId']
            res = self.api_client.invoke('GET', '/jobs', params=params)
            assert res.ok
            body = res.json()
            assert isinstance(body, list)
            assert body and len(body) <= params['rowsPerRead']
            if not res.headers.get('x-ms-continuation-QueryId', None):
                break

        print('## Query job %d' % job_id)
        params = { 'properties': 'Id,State,ChangeTime' }
        res = self.api_client.invoke('GET', '/jobs/%d' % job_id, params=params)
        assert res.ok
        body = res.json()
        assert isinstance(body, list) and body
        prop = find_property(body, 'Id')
        assert prop and int(prop['Value']) == job_id
        prop = find_property(body, 'State')
        assert prop and prop['Value']
        prop = find_property(job['Properties'], 'ChangeTime')
        assert prop and prop['Value']

        invalid_job_id = job_id + 1000

        print('## Query invalid job %d' % invalid_job_id)
        res = self.api_client.invoke('GET', '/jobs/%d' % invalid_job_id)
        assert is_4xx_error(res.status_code)

class JobEnvTest(JobOperationTest):
    def run(self):
        print('## Create a job from XML')
        # NOTE: the echo command should output an envrionment variable on both Linux and Windows.
        xml_job = '''
<Job Name="CustomEnvJob">
  <Tasks>
    <Task CommandLine="echo $myvar %myvar%" MinCores="1" MaxCores="1" />
  </Tasks>
</Job>
        '''
        res = self.api_client.invoke('POST', '/jobs/jobFile', json=xml_job)
        assert res.ok
        body = res.json()
        assert isinstance(body, int)
        job_id = int(body)

        print('## Set envrionment variables for job %d' % job_id)
        name = 'myvar'
        value = 'My Var'
        env = [
            { 'Name': name, 'Value': value },
            { 'Name': 'myvar2', 'Value': 'Another Var' },
        ]
        res = self.api_client.invoke('POST', '/jobs/%d/envVariables' % job_id, json=env)
        assert res.ok

        print('## Submit job %d' % job_id)
        res = self.api_client.invoke('POST', '/jobs/%d/submit' % job_id)
        assert res.ok

        self.wait_job(job_id, 'Finished')

        print('## Get envrionment variables of job %d' % job_id)
        params = { 'names': name }
        res = self.api_client.invoke('GET', '/jobs/%d/envVariables' % job_id, params=params)
        assert res.ok
        body = res.json()
        assert isinstance(body, list) and len(body) == 1
        prop = find_property(body, name)
        assert prop and prop['Value'] == value

        print('## Query task output of job %d' % job_id)
        params = { 'properties': 'TaskId,ExitCode,Output' }
        res = self.api_client.invoke('GET', '/jobs/%d/tasks/1' % job_id, params=params)
        assert res.ok
        body = res.json()
        assert isinstance(body, list) and body
        prop = find_property(body, 'Output')
        assert prop and re.search('\\b%s\\b' % value, prop['Value'])

class JobCustomPropertyTest(JobOperationTest):
    def run(self):
        print('## Create a job from XML')
        # NOTE: the echo command should output an envrionment variable on both Linux and Windows.
        xml_job = '''
<Job Name="CustomPropJob">
  <Tasks>
    <Task CommandLine="echo $myvar %myvar%" MinCores="1" MaxCores="1" />
  </Tasks>
</Job>
        '''
        res = self.api_client.invoke('POST', '/jobs/jobFile', json=xml_job)
        assert res.ok
        body = res.json()
        assert isinstance(body, int)
        job_id = int(body)

        print('## Set custom properties for job %d' % job_id)
        name = 'myvar'
        value = 'My Var'
        env = [
            { 'Name': name, 'Value': value },
            { 'Name': 'myvar2', 'Value': 'Another Var' },
        ]
        res = self.api_client.invoke('POST', '/jobs/%d/customProperties' % job_id, json=env)
        assert res.ok

        print('## Submit job %d' % job_id)
        res = self.api_client.invoke('POST', '/jobs/%d/submit' % job_id)
        assert res.ok

        self.wait_job(job_id, 'Finished')

        print('## Get custom properties of job %d' % job_id)
        params = { 'names': name }
        res = self.api_client.invoke('GET', '/jobs/%d/customProperties' % job_id, params=params)
        assert res.ok
        body = res.json()
        assert isinstance(body, list) and len(body) == 1
        prop = find_property(body, name)
        assert prop and prop['Value'] == value

        print('## Query task output of job %d' % job_id)
        params = { 'properties': 'TaskId,ExitCode,Output' }
        res = self.api_client.invoke('GET', '/jobs/%d/tasks/1' % job_id, params=params)
        assert res.ok
        body = res.json()
        assert isinstance(body, list) and body
        prop = find_property(body, 'Output')
        assert prop and not re.search('\\b%s\\b' % value, prop['Value'])

class TaskOperationTest(JobOperationTest):
    # NOTE: The command should be runnable on both Windows and Linux
    job_with_long_running_task = '''
<Job>
  <Tasks>
    <Task CommandLine="sleep 60 || ping localhost -n 60" MinCores="1" MaxCores="1" />
  </Tasks>
</Job>
    '''

    job_with_long_running_subtask = '''
<Job Name="ParametricSweepJob" MinCores="1" MaxCores="1">
  <Tasks>
    <Task CommandLine="sleep 6* || ping localhost -n 6*" StartValue="1" EndValue="3" IncrementValue="1" Type="ParametricSweep" MinCores="1" MaxCores="1" Name="Sweep Task" />
  </Tasks>
</Job>
    '''

    def create_job_with_long_running_task(self):
        return self.create_job(self.__class__.job_with_long_running_task)

    def create_job_with_long_running_subtask(self):
        return self.create_job(self.__class__.job_with_long_running_subtask)

    def wait_task(self, job_id, task_id, state):
        print('## Wait task %d of job %d to be %s' % (task_id, job_id, state))
        ready = None
        for _ in range(30):
            res = self.api_client.invoke('GET', '/jobs/%d/tasks/%d?properties=TaskId,State,ErrorMessage' % (job_id, task_id))
            assert res.ok
            prop = find_property(res.json(), 'State')
            if prop['Value'] == state:
                ready = True
                break
            else:
                time.sleep(1)
        assert ready
        return res

class QueryTaskTest(TaskOperationTest):
    def run(self):
        print('## Create job from XML')
        xml_job = '''
<Job Name="JobWithAFewTasks">
  <Tasks>
    <Task CommandLine="echo a" MinCores="1" MaxCores="1" Name="Good Task" />
    <Task CommandLine="echo b" MinCores="1" MaxCores="1" Name="Good Task" />
    <Task CommandLine="echo c" MinCores="1" MaxCores="1" Name="Good Task" />
    <Task CommandLine="thiscommanddoesnotexist" MinCores="1" MaxCores="1" Name="Bad Task" />
  </Tasks>
</Job>
        '''
        job_id = self.create_job(xml_job)
        self.wait_job(job_id, 'Failed')

        print('## Query tasks of job %d' % job_id)
        params = { 'properties': 'TaskId,Name,State,CommandLine', 'rowsPerRead': 3 }
        res = self.api_client.invoke('GET', '/jobs/%d/tasks' % job_id, params=params)
        assert res.ok
        body = res.json()
        assert isinstance(body, list) and len(body) == params['rowsPerRead']
        assert res.headers['x-ms-continuation-queryId']

        params['queryId'] = res.headers['x-ms-continuation-queryId']
        res = self.api_client.invoke('GET', '/jobs/%d/tasks' % job_id, params=params)
        assert res.ok
        body = res.json()
        assert isinstance(body, list) and len(body) == 1
        assert not res.headers.get('x-ms-continuation-queryId', None)

        params = { '$filter': 'TaskState eq Failed' }
        res = self.api_client.invoke('GET', '/jobs/%d/tasks' % job_id, params=params)
        assert res.ok
        body = res.json()
        assert isinstance(body, list) and len(body) == 1

        print('## Query a task of job %d' % job_id)
        res = self.api_client.invoke('GET', '/jobs/%d/tasks/4' % job_id)
        assert res.ok
        body = res.json()
        assert isinstance(body, list)

        print('## Query an invalid task of job %d' % job_id)
        res = self.api_client.invoke('GET', '/jobs/%d/tasks/5' % job_id)
        assert is_4xx_error(res.status_code)

class CancelTaskTest(TaskOperationTest):
    def run(self):
        job_id = self.create_job_with_long_running_task()

        self.wait_job(job_id, "Running")

        print('## Cancel task of job %d' % job_id)
        msg = 'Canceled by test!'
        res = self.api_client.invoke('POST', '/jobs/%d/tasks/1/cancel' % job_id, json=msg)
        assert res.ok

        # NOTE: When a task is canceled, its state will be "Failed"?
        res = self.wait_task(job_id, 1, "Failed")
        prop = find_property(res.json(), 'ErrorMessage')
        assert prop and msg in prop['Value']

        # NOTE: When a task is canceled, its parent job will fail?
        self.wait_job(job_id, "Failed")

class FinishTaskTest(TaskOperationTest):
    def run(self):
        job_id = self.create_job_with_long_running_task()

        self.wait_job(job_id, "Running")

        print('## Finish task of job %d' % job_id)
        msg = 'Finished by test!'
        res = self.api_client.invoke('POST', '/jobs/%d/tasks/1/finish' % job_id, json=msg)
        assert res.ok

        res = self.wait_task(job_id, 1, "Finished")
        # NOTE: When a job is "Finished", the error message is set as expected. But It's not
        # true when finishing a job!
        prop = find_property(res.json(), 'ErrorMessage')
        assert prop and msg in prop['Value']

        self.wait_job(job_id, "Finished")

class RequeueTaskTest(TaskOperationTest):
    def run(self):
        xml_job = '''
<Job Name="RunUntilCanceledJob" MinCores="1" MaxCores="1" RunUntilCanceled="True" >
  <Tasks>
    <Task CommandLine="sleep 60 || ping localhost -n 60" MinCores="1" MaxCores="1" />
  </Tasks>
</Job>
        '''
        job_id = self.create_job(xml_job)

        self.wait_job(job_id, "Running")

        print('## Cancel task of job %d' % job_id)
        res = self.api_client.invoke('POST', '/jobs/%d/tasks/1/cancel' % job_id)
        assert res.ok

        self.wait_task(job_id, 1, "Failed")

        print('## Requeue task of job %d' % job_id)
        res = self.api_client.invoke('POST', '/jobs/%d/tasks/1/requeue' % job_id)

        self.wait_task(job_id, 1, "Running")

        print('## Cancel job %d' % job_id)
        res = self.api_client.invoke('POST', '/jobs/%d/cancel' % job_id)
        assert res.ok

        # NOTE: To ensure a single node can finish all the tests, wait it over.
        self.wait_job(job_id, "Canceled")

class CreateParametricSweepJobTest(JobOperationTest):
    def run(self):
        print('## Create job from XML')
        xml_job = '''
<Job Name="ParametricSweepJob" MinCores="1" MaxCores="1">
  <Tasks>
    <Task CommandLine="echo *" StartValue="1" EndValue="3" IncrementValue="1" Type="ParametricSweep" MinCores="1" MaxCores="1" Name="Sweep Task" />
    <Task CommandLine="echo Hello" Name="Basic Task" MinCores="1" MaxCores="1" />
  </Tasks>
</Job>
        '''
        job_id = self.create_job(xml_job)

        print('## Query tasks of job %d' % job_id)
        params = { 'properties': 'TaskId,Name,State,CommandLine' }
        res = self.api_client.invoke('GET', '/jobs/%d/tasks' % job_id, params=params)
        assert res.ok
        body = res.json()
        assert isinstance(body, list) and len(body) == 4

        params = { 'properties': 'TaskId,Name,State,CommandLine', 'expandParametric': 'false' }
        res = self.api_client.invoke('GET', '/jobs/%d/tasks' % job_id, params=params)
        assert res.ok
        body = res.json()
        assert isinstance(body, list) and len(body) == 2

class CancelSubtaskTest(TaskOperationTest):
    def run(self):
        job_id = self.create_job_with_long_running_subtask()

        self.wait_subtask(job_id, 1, 1, 'Running')

        print('## Cancel subtask')

client = ApiClient(hostname, username, password)
# QueryClusterTest(client, 'Query Cluster').start()
# QueryNodeTest(client, 'Query Node').start()
# QueryJobTest(client, 'Query Job').start()
# CreateJobTest(client, 'Create Job').start()
# CancelJobTest(client, 'Cancel Job').start()
# FinishJobTest(client, 'Finish Job').start()
# RequeueJobTest(client, 'Requeue Job').start()
# JobEnvTest(client, 'Set/Get Job Environment Variable').start()
# JobCustomPropertyTest(client, 'Set/Get Job Custom Properties').start()
# QueryTaskTest(client, 'Query Task').start()
# CancelTaskTest(client, 'Cancel Task').start()
# FinishTaskTest(client, 'Finish Task').start()
# RequeueTaskTest(client, 'Requeue Task').start()
# CreateParametricSweepJobTest(client, 'Create Parameteric Sweep Task').start()

