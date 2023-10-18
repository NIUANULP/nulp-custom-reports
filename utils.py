from cassandra.cluster import Cluster
from fastapi import Request
from cassandra.policies import RoundRobinPolicy,DCAwareRoundRobinPolicy, ExponentialReconnectionPolicy
# Loading ENV
from dotenv import load_dotenv
import os

load_dotenv()

# Get all personal info.
def get_all_personal_info():
    
    #1. Loading ENV.
    CLUSTER_LINK = os.environ.get('CLUSTER_LINK')
    cluster = Cluster([f'{CLUSTER_LINK}'],connect_timeout=20,load_balancing_policy=RoundRobinPolicy(),reconnection_policy=ExponentialReconnectionPolicy(max_attempts=10,max_delay=20,base_delay=10))

    USER_INFO_KEYSPACE = os.environ.get('USER_INFO_KEYSPACE')
    USER_INFO_TABLE = os.environ.get('USER_INFO_TABLE')

    session = cluster.connect()
    session.default_timeout = 60

    #2. Setting Keyspaces.
    session.set_keyspace(USER_INFO_KEYSPACE)
    
    query = f"SELECT userid, firstname, lastname, email, maskedemail, maskedphone, phone FROM {USER_INFO_KEYSPACE}.{USER_INFO_TABLE} ALLOW FILTERING;"
    query_result = session.execute(query=query)
    
    return query_result
    

# Get all Batch info.
def get_all_batch_info(courseid,batchid):
    
    #1. Loading ENV.
    CLUSTER_LINK = os.environ.get('CLUSTER_LINK')
    cluster = Cluster([f'{CLUSTER_LINK}'],connect_timeout=20,load_balancing_policy=RoundRobinPolicy(),reconnection_policy=ExponentialReconnectionPolicy(max_attempts=10,max_delay=20,base_delay=10))

    COURSE_KEYSPACE = os.environ.get('COURSE_KEYSPACE')
    COURSE_TABLE = os.environ.get('COURSE_TABLE')

    session = cluster.connect()
    session.default_timeout = 60

    #2. Setting Keyspaces.
    session.set_keyspace(COURSE_KEYSPACE)
    
    if batchid != '':
        query = f"SELECT courseid, batchid, name, end_date, start_date FROM {COURSE_KEYSPACE}.{COURSE_TABLE} WHERE courseid = '{courseid}' and batchid = '{batchid}' ALLOW FILTERING;"
        query_result = session.execute(query=query)
        
        return query_result
    else:
        query = f"SELECT courseid, batchid, name, end_date, start_date FROM {COURSE_KEYSPACE}.{COURSE_TABLE} WHERE courseid = '{courseid}' ALLOW FILTERING;"
        query_result_batchinfo = session.execute(query=query)
        
        
        return query_result_batchinfo

# Search Filter (username).
def filter_query_dict(input_list, search_value):
    filtered_list = []
    for item in input_list:
        for key, value in item.items():
            if isinstance(value, (str, int)) and isinstance(search_value, (str, int)):
                if str(search_value).lower() in str(value).lower():
                    filtered_list.append(item)
                    break 
    return filtered_list

def temp_response():
    # Temp dummy response.
    response = {
    "result": {
        "count": 3,
        "content": [
            {
                "channel": "0130152125009674241",
                "identifier": "do_1138466520964055041213",
                "name": "Test Course",
                "courseStatus": "1",
                "batchid": "92929292",
                "batchStatus": "1",
                "batchName": "xyz",
                "totalEnrolment": 134,
                "totalCompletion": 60,
                "totalCertificateIssued": 40,
                "batchStartdDate": "2023-08-31",
                "batchEndDate": "2023-08-31",
                "enrolmentEndDate": "2023-08-20"
            },
            {
                "channel": "0130152125009674241",
                "identifier": "do_1138466520964055041213",
                "name": "Test Course",
                "createdById": "1093a032-fb64-4e13-bf3b-4fb169032aae",
                "courseStatus": "1",
                "batchid": "92929292",
                "batchStatus": "1",
                "batchName": "xyz",
                "totalEnrolment": 134,
                "totalCompletion": 60,
                "totalCertificateIssued": 40,
                "batchStartdDate": "2023-08-31",
                "batchEndDate": "2023-08-31",
                "enrolmentEndDate": "2023-08-20"
            },
            {
                "channel": "0130152125009674241",
                "identifier": "do_1138466520964055041213",
                "name": "Test Course",
                "createdById": "1093a032-fb64-4e13-bf3b-4fb169032aae",
                "courseStatus": "1",
                "batchid": "92929292",
                "batchStatus": "1",
                "batchName": "xyz",
                "totalEnrolment": 134,
                "totalCompletion": 60,
                "totalCertificateIssued": 40,
                "batchStartdDate": "2023-08-31",
                "batchEndDate": "2023-08-31",
                "enrolmentEndDate": "2023-08-20"
            }
        ],
        "summary": [
            {
               "identifier": "do_1138466520964055041213",
                "totalEnrolment": 412
            },
            {
               "identifier": "do_3838383666664055041213",
                "totalEnrolment": 512
            }
        ]
    }
}

    return response


