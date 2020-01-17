#!/bin/env python3

import requests
import json
import re
import os
import sys
import time
import traceback
from datetime import datetime

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

WAIT_MAX_TRIES = 30

def print_err(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

def find_property(properties, name):
    return next((e for e in properties if e['Name'] == name), None)

def find_property_value(properties, name):
    p = find_property(properties, name)
    return p['Value'] if p != None else None

def is_4xx_error(code):
    return code < 500 and code >= 400

def is_expected(expected, value):
    if isinstance(expected, list):
        return value in expected
    else:
        return value == expected

def append_as_user(msg, as_user = None):
    if as_user:
        msg += ' as user %s' % as_user
    return msg

def header_as_user(as_user):
    if as_user:
        headers = { 'x-ms-as-user': as_user }
    else:
        headers = None
    return headers

class ApiClient:
    def __init__(self, hostname = None, username = None, password = None):
        self.hostname = hostname or os.environ['bvt_hostname']
        self.username = username or os.environ['bvt_username']
        self.password = password or os.environ['bvt_password']
        self.apibase = 'https://%s/hpc' % self.hostname

    def url(self, path):
        return self.apibase + path

    def invoke(self, method, path, **kwargs):
        url = self.url(path)
        res = requests.request(method, url, verify=False, auth=(self.username, self.password), **kwargs)
        msg = '''
* %s %s
* Headers: %s
* Body: %s

* Code: %d
* Headers: %s
* Body: %s
        ''' % (
            res.request.method, res.request.url, res.request.headers, res.request.body,
            res.status_code, res.headers, res.text
        )
        print_err(msg)
        return res

class TestCounter:
    def __init__(self):
        self.pass_count = 0
        self.fail_count = 0

class TestBase:
    title = ''
    counter = TestCounter()

    def __init__(self, api_client):
        self.api_client = api_client
        self.passed = None

    def start(self):
        try:
            print('# %s' % self.__class__.title)
            self.run()
        except AssertionError as error:
            self.__class__.counter.fail_count += 1
            self.passed = False
            print('Failed with error: %s' % str(error))
            traceback.print_exc()
        else:
            self.__class__.counter.pass_count += 1
            self.passed = True
            print('Passed!')

    def run(self):
        pass

    @classmethod
    def report(cls):
        msg = '''
## Total Result
* Total: %d
* Passed: %d
* Failed: %d
''' % (cls.counter.pass_count + cls.counter.fail_count, cls.counter.pass_count, cls.counter.fail_count)
        print(msg)

class QueryClusterTest(TestBase):
    title = 'Query Cluster'

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
    title = 'Query Node'

    def run(self):
        print('## Query nodes')
        params = { '$filter': 'NodeState eq Online', 'properties': 'id,name', 'rowsPerRead': 2 }
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

        print('## Query nodes in pagination')
        page_size = 2
        params = { '$filter': 'NodeState eq Online', 'properties': 'id,name', 'rowsPerRead': page_size, 'startRow': 0 }
        res = self.api_client.invoke('GET', '/nodes', params=params)
        assert res.ok
        body = res.json()
        assert isinstance(body, list)
        assert body and len(body) <= params['rowsPerRead']
        row_count = int(res.headers['x-ms-row-count'])
        assert row_count >= 1

        params['startRow'] = row_count - 1
        res = self.api_client.invoke('GET', '/nodes', params=params)
        assert res.ok
        body = res.json()
        assert isinstance(body, list)
        assert body and len(body) == 1
        assert int(res.headers['x-ms-row-count']) == row_count

        params['startRow'] = row_count
        res = self.api_client.invoke('GET', '/nodes', params=params)
        assert res.ok
        body = res.json()
        assert isinstance(body, list)
        assert len(body) == 0
        assert int(res.headers['x-ms-row-count']) == row_count

        print('## Query nodes with sorting')
        params = { '$filter': 'NodeState eq Online', 'properties': 'id,name', 'rowsPerRead': row_count, 'startRow': 0, 'sortNodesBy': 'Id', 'asc': True }
        res = self.api_client.invoke('GET', '/nodes', params=params)
        assert res.ok
        body = res.json()
        assert isinstance(body, list)
        ids = [find_property_value(n['Properties'], 'Id') for n in body]

        params['asc'] = False
        res = self.api_client.invoke('GET', '/nodes', params=params)
        assert res.ok
        body = res.json()
        assert isinstance(body, list)
        ids2 = [find_property_value(n['Properties'], 'Id') for n in body]
        ids2.reverse()

        assert ids == ids2

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
        assert isinstance(body, list) and body
        props = body[0]['Properties']
        prop = find_property(props, 'Name')
        assert prop and prop['Value']

        group_name = prop['Value']

        print('## Query node group %s' % group_name)
        res = self.api_client.invoke('GET', '/nodes/groups/%s' % group_name)
        assert res.ok
        body = res.json()
        assert isinstance(body, list)

        # invalid_node_group = 'thisisaninvalidnodegroup'

        # print('## Query invalid node group %s' % invalid_node_group )
        # res = self.api_client.invoke('GET', '/nodes/groups/%s' % invalid_node_group)
        # assert is_4xx_error(res.status_code)

class JobOperationTest(TestBase):
    run_until_cancel_job = '''
<Job Name="RunUntilCanceledJob" MinCores="1" MaxCores="1" RunUntilCanceled="True" NodeGroups="ComputeNodes" NodeGroupOp="Uniform" >
  <Tasks>
    <Task Name="TestTaskInXML" CommandLine="echo Hello" MinCores="1" MaxCores="1" />
  </Tasks>
</Job>
    '''

    simple_job = '''
<Job Name="SimpleJob" NodeGroups="ComputeNodes" NodeGroupOp="Uniform">
  <Tasks>
    <Task Name="TestTaskInXML" CommandLine="echo Hello" MinCores="1" MaxCores="1" />
  </Tasks>
</Job>
    '''

    def create_simple_job(self, as_user=None):
        return self.create_job(self.__class__.simple_job, as_user)

    def create_run_until_cancel_job(self, as_user=None):
        return self.create_job(self.__class__.run_until_cancel_job, as_user)

    def create_job(self, xml_job, as_user=None):
        print(append_as_user('## Create a job from xml', as_user))
        res = self.api_client.invoke('POST', '/jobs/jobFile', json=xml_job, headers=header_as_user(as_user))
        assert res.ok
        body = res.json()
        assert isinstance(body, int)
        job_id = int(body)

        print(append_as_user('## Submit job %d' % job_id, as_user))
        res = self.api_client.invoke('POST', '/jobs/%d/submit' % job_id, headers=header_as_user(as_user))
        assert res.ok

        return job_id

    def wait_job(self, job_id, state):
        print('## Wait job %d to be %s' % (job_id, state))
        ready = None
        for _ in range(WAIT_MAX_TRIES):
            res = self.api_client.invoke('GET', '/jobs/%d?properties=Id,State,ErrorMessage' % job_id)
            assert res.ok
            prop = find_property(res.json(), 'State')
            if is_expected(state, prop['Value']):
                ready = True
                break
            else:
                time.sleep(1)
        assert ready
        return res

class CancelJobTest(JobOperationTest):
    title = 'Cancel Job'

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
    title = 'Finish Job'

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
    title = 'Requeue Job'

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

        self.wait_job(job_id, ['Queued', 'Running'])

        print('## Finish job %d' % job_id)
        res = self.api_client.invoke('POST', '/jobs/%d/finish' % job_id, json="Finished by BVT tester.")
        assert res.ok

        self.wait_job(job_id, 'Finished')

class CreateJobTest(JobOperationTest):
    title = 'Create Job'

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
    title = 'Query Job'

    def run(self):
        now = datetime.utcnow()
        row_count = 4
        job_ids = [self.create_simple_job() for _ in range(row_count)]
        job_id = job_ids[-1]

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

        print('## Query jobs in pagination')
        params = {
            'startRow': 0,
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
        assert res.headers['x-ms-row-count'] and int(res.headers['x-ms-row-count']) == row_count

        params['startRow'] = 3
        page_size = params['rowsPerRead']
        res = self.api_client.invoke('GET', '/jobs', params=params)
        assert res.ok
        body = res.json()
        assert isinstance(body, list)
        rest = row_count - page_size
        assert body and len(body) == page_size if rest >= page_size else rest
        assert res.headers['x-ms-row-count'] and int(res.headers['x-ms-row-count']) == row_count

        params['startRow'] = row_count
        res = self.api_client.invoke('GET', '/jobs', params=params)
        assert res.ok
        body = res.json()
        assert isinstance(body, list)
        assert len(body) == 0
        assert res.headers['x-ms-row-count'] and int(res.headers['x-ms-row-count']) == row_count

        print('## Query jobs with sorting')
        params = {
            'startRow': 0,
            'rowsPerRead': row_count,
            'owner': self.api_client.username,
            'properties': 'Id,Owner,ChangeTime',
            # Server datetime format is "M/d/yyyy h:mm:ss tt"
            '$filter': 'ChangeTimeFrom eq %s' % now.strftime('%m/%d/%Y %H:%M:%S'),
            'sortJobsBy': 'id',
            'asc': True
        }
        res = self.api_client.invoke('GET', '/jobs', params=params)
        assert res.ok
        body = res.json()
        assert isinstance(body, list)
        assert body and len(body) == row_count
        ids = [int(find_property_value(job['Properties'], 'Id')) for job in body]
        assert ids == job_ids

        params['asc'] = False
        res = self.api_client.invoke('GET', '/jobs', params=params)
        assert res.ok
        body = res.json()
        assert isinstance(body, list)
        assert body and len(body) == row_count
        ids = [int(find_property_value(job['Properties'], 'Id')) for job in body]
        ids.reverse()
        assert ids == job_ids

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

class QueryJobTemplateTest(JobOperationTest):
    title = 'Query Job Template'

    def run(self):
        print('## Query job template')
        res = self.api_client.invoke('GET', '/jobs/templates')
        assert res.ok
        body = res.json()
        assert isinstance(body, list) and 'Default' in body

class JobEnvTest(JobOperationTest):
    title = 'Set/Get Job Environment Variable'

    def run(self):
        print('## Create a job from XML')
        # NOTE:
        # 1. The echo command should output an envrionment variable on both Linux and Windows.
        # 2. Specify ComputeNodes so that the it won't run on AzureBatchPool nodes, which will cause bad "Output"
        xml_job = '''
<Job Name="CustomEnvJob" NodeGroups="ComputeNodes" NodeGroupOp="Uniform">
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
    title = 'Set/Get Job Custom Properties'

    def run(self):
        print('## Create a job from XML')
        # NOTE: the echo command should output an envrionment variable on both Linux and Windows.
        xml_job = '''
<Job Name="CustomPropJob" NodeGroups="ComputeNodes" NodeGroupOp="Uniform">
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

class SetJobPropertyTest(JobOperationTest):
    title = 'Set Job Properties'

    def run(self):
        job_id = self.create_run_until_cancel_job()

        # Job name can't be changed in Queued state.
        self.wait_job(job_id, 'Running')

        print('## Update properties of job %d' % job_id)
        name = 'Name'
        value = 'Updated Name'
        props = [{ 'name': name, 'value': value }]
        res = self.api_client.invoke('PUT', '/jobs/%d' % job_id, json=props)
        assert res.ok

        print('## Query properties of job %d' % job_id)
        res = self.api_client.invoke('GET', '/jobs/%d' % job_id, params={ 'properties': 'Id,Name,State' })
        assert res.ok
        body = res.json()
        assert isinstance(body, list) and body
        prop = find_property(body, name)
        assert prop and prop['Value'] == value

        print('## Cancel job %d' % job_id)
        res = self.api_client.invoke('POST', '/jobs/%d/cancel' % job_id)
        assert res.ok

        # Job name can't be changed after Canceled state.
        self.wait_job(job_id, 'Canceled')

        print('## Update properties of job %d' % job_id)
        value2 = 'Updated again'
        props = [{ 'name': name, 'value': value2 }]
        res = self.api_client.invoke('PUT', '/jobs/%d' % job_id, json=props)
        assert is_4xx_error(res.status_code)

        print('## Query properties of job %d' % job_id)
        res = self.api_client.invoke('GET', '/jobs/%d' % job_id, params={ 'properties': 'Id,Name,State' })
        assert res.ok
        body = res.json()
        assert isinstance(body, list) and body
        prop = find_property(body, name)
        assert prop and prop['Value'] == value

class TaskOperationTest(JobOperationTest):
    # NOTE: The command should be runnable on both Windows and Linux
    job_with_long_running_task = '''
<Job NodeGroups="ComputeNodes" NodeGroupOp="Uniform">
  <Tasks>
    <Task CommandLine="sleep 60 || ping localhost -n 60" MinCores="1" MaxCores="1" />
  </Tasks>
</Job>
    '''

    job_with_long_running_subtask = '''
<Job Name="ParametricSweepJob" MinCores="1" MaxCores="1" NodeGroups="ComputeNodes" NodeGroupOp="Uniform">
  <Tasks>
    <Task CommandLine="sleep 60 || ping localhost -n 60" StartValue="1" EndValue="3" IncrementValue="1" Type="ParametricSweep" MinCores="1" MaxCores="1" Name="Sweep Task" />
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
        for _ in range(WAIT_MAX_TRIES):
            res = self.api_client.invoke('GET', '/jobs/%d/tasks/%d?properties=TaskId,State,ErrorMessage' % (job_id, task_id))
            assert res.ok
            prop = find_property(res.json(), 'State')
            if is_expected(state, prop['Value']):
                ready = True
                break
            else:
                time.sleep(1)
        assert ready
        return res

    def wait_subtask(self, job_id, task_id, subtask_id, state):
        print('## Wait subtask %d of task %d of job %d to be %s' % (subtask_id, task_id, job_id, state))
        ready = None
        for _ in range(WAIT_MAX_TRIES):
            res = self.api_client.invoke('GET',
                '/jobs/%d/tasks/%d/subtasks/%d?properties=TaskId,State,ErrorMessage' % (job_id, task_id, subtask_id))
            if not res.ok:
                if is_4xx_error(res.status_code) and 'the specified subtask has not been expanded yet' in res.text:
                    time.sleep(1)
                    continue
                else:
                    assert False
            prop = find_property(res.json(), 'State')
            if is_expected(state, prop['Value']):
                ready = True
                break
            else:
                time.sleep(1)
        assert ready
        return res

class QueryTaskTest(TaskOperationTest):
    title = 'Query Task'

    def run(self):
        print('## Create job from XML')
        xml_job = '''
<Job Name="JobWithAFewTasks" NodeGroups="ComputeNodes" NodeGroupOp="Uniform">
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

        print('## Query tasks of job %d in pagination' % job_id)
        params = { 'properties': 'TaskId,Name,State,CommandLine', 'rowsPerRead': 3, 'startRow': 0 }
        res = self.api_client.invoke('GET', '/jobs/%d/tasks' % job_id, params=params)
        assert res.ok
        body = res.json()
        assert isinstance(body, list) and len(body) == params['rowsPerRead']
        assert int(res.headers['x-ms-row-count']) == 4

        params['startRow'] = 3
        res = self.api_client.invoke('GET', '/jobs/%d/tasks' % job_id, params=params)
        assert res.ok
        body = res.json()
        assert isinstance(body, list) and len(body) == 1
        assert int(res.headers['x-ms-row-count']) == 4

        params['startRow'] = 4
        res = self.api_client.invoke('GET', '/jobs/%d/tasks' % job_id, params=params)
        assert res.ok
        body = res.json()
        assert isinstance(body, list) and len(body) == 0
        assert int(res.headers['x-ms-row-count']) == 4

        print('## Query tasks of job %d with sorting' % job_id)
        params = { 'properties': 'TaskId,Name,State,CommandLine', 'rowsPerRead': 100, 'startRow': 0, 'sortTasksBy': 'TaskId', 'asc': True }
        res = self.api_client.invoke('GET', '/jobs/%d/tasks' % job_id, params=params)
        assert res.ok
        body = res.json()
        assert isinstance(body, list) and len(body) == 4
        ids = [find_property_value(t['Properties'], 'TaskId') for t in body]

        params['asc'] = False
        res = self.api_client.invoke('GET', '/jobs/%d/tasks' % job_id, params=params)
        assert res.ok
        body = res.json()
        assert isinstance(body, list) and len(body) == 4
        ids2 = [find_property_value(t['Properties'], 'TaskId') for t in body]
        ids2.reverse()

        assert ids == ids2

        print('## Query a task of job %d' % job_id)
        res = self.api_client.invoke('GET', '/jobs/%d/tasks/4' % job_id)
        assert res.ok
        body = res.json()
        assert isinstance(body, list)

        print('## Query an invalid task of job %d' % job_id)
        res = self.api_client.invoke('GET', '/jobs/%d/tasks/5' % job_id)
        assert is_4xx_error(res.status_code)

class CancelTaskTest(TaskOperationTest):
    title = 'Cancel Task'

    def run(self):
        job_id = self.create_job_with_long_running_task()

        self.wait_task(job_id, 1, "Running")

        print('## Cancel task of job %d' % job_id)
        msg = 'Canceled by test!'
        res = self.api_client.invoke('POST', '/jobs/%d/tasks/1/cancel' % job_id, json=msg)
        assert res.ok

        # NOTE: When a task is canceled, its state will be "Failed".
        res = self.wait_task(job_id, 1, "Failed")
        prop = find_property(res.json(), 'ErrorMessage')
        assert prop and msg in prop['Value']

        # NOTE: When a task is canceled, its parent job will fail.
        self.wait_job(job_id, "Failed")

class FinishTaskTest(TaskOperationTest):
    title = 'Finish Task'

    def run(self):
        job_id = self.create_job_with_long_running_task()

        self.wait_task(job_id, 1, "Running")

        print('## Finish task of job %d' % job_id)
        msg = 'Finished by test!'
        res = self.api_client.invoke('POST', '/jobs/%d/tasks/1/finish' % job_id, json=msg)
        assert res.ok

        res = self.wait_task(job_id, 1, "Finished")
        # NOTE: When a task is "Finished", the error message is set as expected. But It's not
        # when finishing a job!
        prop = find_property(res.json(), 'ErrorMessage')
        assert prop and msg in prop['Value']

        self.wait_job(job_id, "Finished")

class RequeueTaskTest(TaskOperationTest):
    title = 'Requeue Task'

    def run(self):
        xml_job = '''
<Job Name="RunUntilCanceledJob" MinCores="1" MaxCores="1" RunUntilCanceled="True" NodeGroups="ComputeNodes" NodeGroupOp="Uniform" >
  <Tasks>
    <Task CommandLine="sleep 60 || ping localhost -n 60" MinCores="1" MaxCores="1" />
  </Tasks>
</Job>
        '''
        job_id = self.create_job(xml_job)

        self.wait_task(job_id, 1, "Running")

        print('## Cancel task of job %d' % job_id)
        res = self.api_client.invoke('POST', '/jobs/%d/tasks/1/cancel' % job_id)
        assert res.ok

        self.wait_task(job_id, 1, "Failed")

        print('## Requeue task of job %d' % job_id)
        res = self.api_client.invoke('POST', '/jobs/%d/tasks/1/requeue' % job_id)

        self.wait_task(job_id, 1, ['Queued', 'Running'])

        print('## Cancel job %d' % job_id)
        res = self.api_client.invoke('POST', '/jobs/%d/cancel' % job_id)
        assert res.ok

        # NOTE: To ensure a single node can finish all the tests, wait it over.
        self.wait_job(job_id, "Canceled")

class CreatePSJobTest(JobOperationTest):
    title = 'Create Parameteric Sweep Task'

    def run(self):
        print('## Create job from XML')
        xml_job = '''
<Job Name="ParametricSweepJob" MinCores="1" MaxCores="1" NodeGroups="ComputeNodes" NodeGroupOp="Uniform">
  <Tasks>
    <Task CommandLine="echo *" StartValue="1" EndValue="3" IncrementValue="1" Type="ParametricSweep" MinCores="1" MaxCores="1" Name="Sweep Task" />
    <Task CommandLine="echo Hello" Name="Basic Task" MinCores="1" MaxCores="1" />
  </Tasks>
</Job>
        '''
        job_id = self.create_job(xml_job)

        # Wait for the Parametric Sweep job expanded
        # NOTE: it seems Running state doesn't ensure subtasks expanded.
        # self.wait_job(job_id, ['Running', 'Finishing', 'Finished'])
        self.wait_job(job_id, ['Finishing', 'Finished'])

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
    title = 'Cancel Subtask'

    def run(self):
        job_id = self.create_job_with_long_running_subtask()

        self.wait_subtask(job_id, 1, 1, 'Running')

        print('## Cancel subtask')
        msg = "Canceled by test."
        res = self.api_client.invoke('POST', '/jobs/%d/tasks/1/subtasks/1/cancel' % job_id, json=msg)

        res = self.wait_subtask(job_id, 1, 1, "Failed")
        prop = find_property(res.json(), 'ErrorMessage')
        assert prop and msg in prop['Value']

        print('## Cancel job %d' % job_id)
        self.api_client.invoke('POST', '/jobs/%d/cancel' % job_id)

        self.wait_job(job_id, "Canceled")

class FinishSubtaskTest(TaskOperationTest):
    title = 'Finish Subtask'

    def run(self):
        job_id = self.create_job_with_long_running_subtask()

        self.wait_subtask(job_id, 1, 1, 'Running')

        print('## Finish subtask')
        msg = "Finished by test."
        res = self.api_client.invoke('POST', '/jobs/%d/tasks/1/subtasks/1/finish' % job_id, json=msg)

        res = self.wait_subtask(job_id, 1, 1, "Finished")
        prop = find_property(res.json(), 'ErrorMessage')
        assert prop and msg in prop['Value']

        print('## Cancel job %d' % job_id)
        self.api_client.invoke('POST', '/jobs/%d/cancel' % job_id)

        self.wait_job(job_id, "Canceled")

class RequeueSubtaskTest(TaskOperationTest):
    title = 'Requeue Subtask'

    def run(self):
        # NOTE: To ensure the test can be done on a node with only 2 cores, limit the number of subtasks to 2.
        xml_job = '''
<Job Name="ParametricSweepJob" RunUntilCanceled="True" MinCores="1" MaxCores="1" NodeGroups="ComputeNodes" NodeGroupOp="Uniform">
  <Tasks>
    <Task CommandLine="sleep 60 || ping localhost -n 60" StartValue="1" EndValue="2" IncrementValue="1" Type="ParametricSweep" MinCores="1" MaxCores="1" Name="Sweep Task" />
  </Tasks>
</Job>
        '''
        job_id = self.create_job(xml_job)

        self.wait_subtask(job_id, 1, 1, 'Running')

        print('## Cancel subtask')
        msg = "Canceled by test."
        self.api_client.invoke('POST', '/jobs/%d/tasks/1/subtasks/1/cancel' % job_id, json=msg)

        self.wait_subtask(job_id, 1, 1, "Failed")

        print('## Requeue subtask')
        self.api_client.invoke('POST', '/jobs/%d/tasks/1/subtasks/1/requeue' % job_id)

        self.wait_subtask(job_id, 1, 1, ['Queued', 'Running'])

        print('## Cancel job %d' % job_id)
        self.api_client.invoke('POST', '/jobs/%d/cancel' % job_id)

        self.wait_job(job_id, "Canceled")

class TaskEnvTest(TaskOperationTest):
    title = 'Set/Get Task Environment Variable'

    def run(self):
        print('## Create a job from XML')
        # NOTE:
        # 1. The echo command should output an envrionment variable on both Linux and Windows.
        # 2. Specify ComputeNodes so that the it won't run on AzureBatchPool nodes, which will cause bad "Output"
        xml_job = '''
<Job Name="CustomEnvJob" NodeGroups="ComputeNodes" NodeGroupOp="Uniform">
  <Tasks>
    <Task CommandLine="echo $myvar %myvar%" MinCores="1" MaxCores="1" />
    <Task CommandLine="echo $myvar %myvar%" MinCores="1" MaxCores="1" />
  </Tasks>
</Job>
        '''
        res = self.api_client.invoke('POST', '/jobs/jobFile', json=xml_job)
        assert res.ok
        body = res.json()
        assert isinstance(body, int)
        job_id = int(body)

        print('## Set envrionment variables for task 1 of job %d' % job_id)
        name = 'myvar'
        value = 'My Var'
        env = [
            { 'Name': name, 'Value': value },
            { 'Name': 'myvar2', 'Value': 'Another Var' },
        ]
        res = self.api_client.invoke('POST', '/jobs/%d/tasks/1/envVariables' % job_id, json=env)
        assert res.ok

        print('## Submit job %d' % job_id)
        res = self.api_client.invoke('POST', '/jobs/%d/submit' % job_id)
        assert res.ok

        self.wait_job(job_id, 'Finished')

        print('## Get envrionment variables of tasks of job %d' % job_id)
        params = { 'names': name }
        res = self.api_client.invoke('GET', '/jobs/%d/tasks/1/envVariables' % job_id, params=params)
        assert res.ok
        body = res.json()
        assert isinstance(body, list) and len(body) == 1
        prop = find_property(body, name)
        assert prop and prop['Value'] == value

        res = self.api_client.invoke('GET', '/jobs/%d/tasks/2/envVariables' % job_id)
        assert res.ok
        body = res.json()
        assert isinstance(body, list) and len(body) == 0

        print('## Query output of tasks of job %d' % job_id)
        params = { 'properties': 'TaskId,ExitCode,Output' }
        res = self.api_client.invoke('GET', '/jobs/%d/tasks/1' % job_id, params=params)
        assert res.ok
        body = res.json()
        assert isinstance(body, list) and body
        prop = find_property(body, 'Output')
        assert prop and re.search('\\b%s\\b' % value, prop['Value'])

        res = self.api_client.invoke('GET', '/jobs/%d/tasks/2' % job_id, params=params)
        assert res.ok
        body = res.json()
        assert isinstance(body, list) and body
        prop = find_property(body, 'Output')
        assert prop and not re.search('\\b%s\\b' % value, prop['Value'])

class TaskCustomPropertyTest(TaskOperationTest):
    title = 'Set/Get Task Custom Properties'

    def run(self):
        print('## Create a job from XML')
        # NOTE: the echo command should output an envrionment variable on both Linux and Windows.
        xml_job = '''
<Job Name="CustomPropJob" NodeGroups="ComputeNodes" NodeGroupOp="Uniform">
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

        print('## Set custom properties for task 1 of job %d' % job_id)
        name = 'myvar'
        value = 'My Var'
        env = [
            { 'Name': name, 'Value': value },
            { 'Name': 'myvar2', 'Value': 'Another Var' },
        ]
        res = self.api_client.invoke('POST', '/jobs/%d/tasks/1/customProperties' % job_id, json=env)
        assert res.ok

        print('## Submit job %d' % job_id)
        res = self.api_client.invoke('POST', '/jobs/%d/submit' % job_id)
        assert res.ok

        self.wait_job(job_id, 'Finished')

        print('## Get custom properties of tasks of job %d' % job_id)
        params = { 'names': name }
        res = self.api_client.invoke('GET', '/jobs/%d/tasks/1/customProperties' % job_id, params=params)
        assert res.ok
        body = res.json()
        assert isinstance(body, list) and len(body) == 1
        prop = find_property(body, name)
        assert prop and prop['Value'] == value

        print('## Query output of tasks of job %d' % job_id)
        params = { 'properties': 'TaskId,ExitCode,Output' }
        res = self.api_client.invoke('GET', '/jobs/%d/tasks/1' % job_id, params=params)
        assert res.ok
        body = res.json()
        assert isinstance(body, list) and body
        prop = find_property(body, 'Output')
        assert prop and not re.search('\\b%s\\b' % value, prop['Value'])

class SetTaskPropertyTest(TaskOperationTest):
    title = 'Set Task Properties'

    def run(self):
        xml_job = '''
<Job Name="SimpleJob" NodeGroups="ComputeNodes" NodeGroupOp="Uniform">
  <Tasks>
    <Task Name="TestTaskInXML" CommandLine="echo Hello" MinCores="1" MaxCores="1" />
  </Tasks>
</Job>
        '''
        print('## Create a job from xml')
        res = self.api_client.invoke('POST', '/jobs/jobFile', json=xml_job)
        assert res.ok
        body = res.json()
        assert isinstance(body, int)
        job_id = int(body)

        # Properties can be set at Configuring state.
        print('## Update properties of task 1 of job %d' % job_id)
        name = 'Name'
        value = 'Updated Name'
        props = [{ 'name': name, 'value': value }]
        res = self.api_client.invoke('PUT', '/jobs/%d/tasks/1' % job_id, json=props)
        assert res.ok

        print('## Query properties of task 1 of job %d' % job_id)
        res = self.api_client.invoke('GET', '/jobs/%d/tasks/1' % job_id, params={ 'properties': 'TaskId,Name,State' })
        assert res.ok
        body = res.json()
        assert isinstance(body, list) and body
        prop = find_property(body, name)
        assert prop and prop['Value'] == value

        # NOTE: Without Submit, the task state would be Configuring, even when the job is cancled.
        print('## Submit job %d' % job_id)
        res = self.api_client.invoke('POST', '/jobs/%d/submit' % job_id)
        assert res.ok

        self.wait_job(job_id, 'Finished')

        # NOTE: Do not test updating task after submitting it, since the behaviour is unsure.
        # print('## Update properties of task 1 of job %d' % job_id)
        # value2 = 'Updated again'
        # props = [{ 'name': name, 'value': value2 }]
        # res = self.api_client.invoke('PUT', '/jobs/%d/tasks/1' % job_id, json=props)
        # assert is_4xx_error(res.status_code)

        # print('## Query properties of task 1 of job %d' % job_id)
        # res = self.api_client.invoke('GET', '/jobs/%d/tasks/1' % job_id, params={ 'properties': 'TaskId,Name,State' })
        # assert res.ok
        # body = res.json()
        # assert isinstance(body, list) and body
        # prop = find_property(body, name)
        # assert prop and prop['Value'] == value

class SetPSTaskPropertyTest(TaskOperationTest):
    title = 'Set Parameteric Sweep Task Properties'

    def run(self):
        xml_job = '''
<Job Name="ParametricSweepJob" MinCores="1" MaxCores="1" NodeGroups="ComputeNodes" NodeGroupOp="Uniform">
  <Tasks>
    <Task CommandLine="echo *" StartValue="1" EndValue="3" IncrementValue="1" Type="ParametricSweep" MinCores="1" MaxCores="1" Name="Sweep Task" />
  </Tasks>
</Job>
        '''
        print('## Create a job from xml')
        res = self.api_client.invoke('POST', '/jobs/jobFile', json=xml_job)
        assert res.ok
        body = res.json()
        assert isinstance(body, int)
        job_id = int(body)

        print('## Update properties of task 1 of job %d' % job_id)
        name = 'Name'
        value = 'Updated Name'
        props = [{ 'name': name, 'value': value }]
        res = self.api_client.invoke('PUT', '/jobs/%d/tasks/1' % job_id, json=props)
        assert res.ok

        print('## Query properties of task 1 of job %d' % job_id)
        res = self.api_client.invoke('GET', '/jobs/%d/tasks/1' % job_id, params={ 'properties': 'TaskId,Name,State' })
        assert res.ok
        body = res.json()
        assert isinstance(body, list) and body
        prop = find_property(body, name)
        assert prop and prop['Value'] == value

        print('## Submit job %d' % job_id)
        res = self.api_client.invoke('POST', '/jobs/%d/submit' % job_id)
        assert res.ok

        self.wait_job(job_id, 'Finished')

        # NOTE: Do not test updating task after submitting it, since the behaviour is unsure.
        # print('## Update properties of task 1 of job %d' % job_id)
        # value2 = 'Updated Name'
        # props = [{ 'name': name, 'value': value2 }]
        # res = self.api_client.invoke('PUT', '/jobs/%d/tasks/1' % job_id, json=props)
        # assert is_4xx_error(res.status_code)

        # print('## Query properties of task 1 of job %d' % job_id)
        # res = self.api_client.invoke('GET', '/jobs/%d/tasks/1' % job_id, params={ 'properties': 'TaskId,Name,State' })
        # assert res.ok
        # body = res.json()
        # assert isinstance(body, list) and body
        # prop = find_property(body, name)
        # assert prop and prop['Value'] == value

class ServiceAsClientTest(JobOperationTest):
    title = 'Service as Client'

    # NOTE: To pass the test, the username in api_client must be of role "Administrator" or "Job Administrator".
    def __init__(self, api_client, as_user):
        super().__init__(api_client)
        self.as_user = as_user

    def run(self):
        job_id = self.create_simple_job(self.as_user)

        print('## Query job %d' % job_id)
        res = self.api_client.invoke('GET', '/jobs/%d' % job_id, params={ 'properties': 'Id,State,Owner' })
        assert res.ok
        body = res.json()
        assert isinstance(body, list) and body
        prop = find_property(body, 'Owner')
        assert prop and prop['Value'].lower() == self.as_user.lower()

        self.wait_job(job_id, 'Finished')

        job_id = self.create_run_until_cancel_job()

        print(append_as_user('## Cancel job %d' % job_id, self.as_user));
        res = self.api_client.invoke('POST', '/jobs/%d/cancel' % job_id, headers=header_as_user(self.as_user))
        assert is_4xx_error(res.status_code)

        print('## Cancel job %d' % job_id);
        res = self.api_client.invoke('POST', '/jobs/%d/cancel' % job_id)
        assert res.ok

        self.wait_job(job_id, 'Canceled')

def main():
    client = ApiClient()

    QueryClusterTest(client).start()
    QueryNodeTest(client).start()
    QueryJobTemplateTest(client).start()
    QueryJobTest(client).start()
    CreateJobTest(client).start()
    CancelJobTest(client).start()
    FinishJobTest(client).start()
    RequeueJobTest(client).start()
    JobEnvTest(client).start()
    JobCustomPropertyTest(client).start()
    SetJobPropertyTest(client).start()
    QueryTaskTest(client).start()
    CancelTaskTest(client).start()
    FinishTaskTest(client).start()
    RequeueTaskTest(client).start()
    CreatePSJobTest(client).start()
    CancelSubtaskTest(client).start()
    FinishSubtaskTest(client).start()
    RequeueSubtaskTest(client).start()
    TaskEnvTest(client).start()
    TaskCustomPropertyTest(client).start()
    SetTaskPropertyTest(client).start()
    SetPSTaskPropertyTest(client).start()

    name = 'bvt_username2'
    value = os.environ.get(name, None)
    if value:
        ServiceAsClientTest(client, value).start()
    else:
        print('# Skiped ServiceAsClientTest since no %s defined.' % name)

    TestBase.report()

    sys.exit(TestBase.counter.fail_count)

if __name__ == '__main__':
    main()
