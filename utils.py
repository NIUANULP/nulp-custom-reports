from cassandra.cluster import Cluster
from fastapi import Request

# Loading ENV
from dotenv import load_dotenv
import os


load_dotenv()

def get_personal_info(user_id):

    if user_id:
        
        #1. Loading ENV.
        CLUSTER_LINK = os.environ.get('CLUSTER_LINK')
        cluster = Cluster([f'{CLUSTER_LINK}'])
        
        USER_INFO_KEYSPACE = os.environ.get('USER_INFO_KEYSPACE')
        USER_INFO_TABLE = os.environ.get('USER_INFO_TABLE')

        session = cluster.connect()
        session.default_timeout = 60

        # 2.Setting Keyspaces.

        session.set_keyspace(USER_INFO_KEYSPACE)
        
        query = f"SELECT firstname, lastname, email, maskedemail, maskedphone, phone FROM {USER_INFO_KEYSPACE}.{USER_INFO_TABLE} WHERE userid = '{user_id}' ALLOW FILTERING;"
        query_result = session.execute(query=query)
        
        user_info = {'username':'','email':'','maskedemail':'','maskedphone':'','phone':''}
        
        try:
            if query_result:
                for i in query_result:
                    try:
                        user_info['username'] = i[0] + i[1]
                        user_info['email'] = i[2]
                        user_info['maskedemail'] = i[3]
                        user_info['maskedphone'] = i[4]
                        user_info['phone'] = i[5]
                    
                        return user_info
                    except:
                        return user_info
            else:
                return user_info
        except:
            return user_info

    else:
        return 'No userid passed.'


# Fetch Course Name.

def get_course_name(courseid,batchid):
    
    CLUSTER_LINK = os.environ.get('CLUSTER_LINK')
    COURSE_KEYSPACE = os.environ.get('COURSE_KEYSPACE')
    COURSE_TABLE = os.environ.get('COURSE_TABLE')
    
    if courseid and batchid:
        query = f"""SELECT name FROM {COURSE_KEYSPACE}.{COURSE_TABLE} WHERE courseid = '{courseid}' and batchid = '{batchid}' ALLOW FILTERING;"""
    else:
        query = f"""SELECT name FROM {COURSE_KEYSPACE}.{COURSE_TABLE} WHERE courseid = '{courseid}' ALLOW FILTERING;"""
    
    cluster = Cluster([f'{CLUSTER_LINK}'])
    session = cluster.connect()
    session.default_timeout = 60

    # 2.Setting Keyspaces.
    session.set_keyspace(COURSE_KEYSPACE)

    query_result = session.execute(query=query)
    course_name = ''
    try:
        if query_result:
            for i in query_result[:1]:
                try:
                    course_name = i[0]
                    return course_name
                except:
                    course_name = ''
                    return course_name
        else:
            course_name = ''
            return course_name
    except:
        course_name = ''
        return course_name


# Batch start and end date.

def batch_start_date(courseid,batchid):
    CLUSTER_LINK = os.environ.get('CLUSTER_LINK')
    #KEYSPACE= os.environ.get('KEYSPACE')
    


    COURSE_KEYSPACE = os.environ.get('COURSE_KEYSPACE')
    COURSE_TABLE = os.environ.get('COURSE_TABLE')
    CLUSTER_LINK = os.environ.get('CLUSTER_LINK')

    cluster = Cluster([f'{CLUSTER_LINK}'])
    session = cluster.connect()
    session.default_timeout = 60

    # 2.Setting Keyspaces.
    session.set_keyspace(COURSE_KEYSPACE)

    
    if courseid and batchid:
        query = f"""SELECT start_date FROM {COURSE_KEYSPACE}.{COURSE_TABLE} WHERE courseid = '{courseid}' and batchid = '{batchid}' ALLOW FILTERING;"""
    else:
        query = f"""SELECT start_date FROM {COURSE_KEYSPACE}.{COURSE_TABLE} WHERE courseid = '{courseid}' ALLOW FILTERING;"""
    
    #print('Query_date:-',query)
    query_result = session.execute(query=query)
    try:
            if query_result:
                for i in query_result[:1]:
                    try:
                        start_date = i[0]
                        return start_date
                    except:
                        start_date = ''
                        return start_date
            else:
                start_date = ''
                return start_date
    except:
        start_date = ''
        return start_date
    
# End Date.
def batch_end_date(courseid,batchid):
    CLUSTER_LINK = os.environ.get('CLUSTER_LINK')

    COURSE_KEYSPACE = os.environ.get('COURSE_KEYSPACE')
    COURSE_TABLE = os.environ.get('COURSE_TABLE')
    CLUSTER_LINK = os.environ.get('CLUSTER_LINK')

    cluster = Cluster([f'{CLUSTER_LINK}'])
    session = cluster.connect()
    session.default_timeout = 60

    # 2.Setting Keyspaces.
    session.set_keyspace(COURSE_KEYSPACE)

    
    if courseid and batchid:
        query = f"""SELECT end_date FROM {COURSE_KEYSPACE}.{COURSE_TABLE} WHERE courseid = '{courseid}' and batchid = '{batchid}' ALLOW FILTERING;"""
    else:
        query = f"""SELECT end_date FROM {COURSE_KEYSPACE}.{COURSE_TABLE} WHERE courseid = '{courseid}' ALLOW FILTERING;"""

    query_result = session.execute(query=query)
    try:
            if query_result:
                for i in query_result[:1]:
                    try:
                        end_date = i[0]
                        session.shutdown()
                        cluster.shutdown()
                        return end_date
                    except:
                        session.shutdown()
                        cluster.shutdown()
                        end_date = ''
                        return end_date
            else:
                session.shutdown()
                cluster.shutdown()
                end_date = ''
                return end_date
    except:
        session.shutdown()
        cluster.shutdown()
        end_date = ''
        return end_date
    
    finally:
        session.shutdown()
        cluster.shutdown()

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

def filter_query_dict(input_list, search_value):
    filtered_list = []
    for item in input_list:
        for key, value in item.items():
            if isinstance(value, (str, int)) and isinstance(search_value, (str, int)):
                if str(search_value).lower() in str(value).lower():
                    filtered_list.append(item)
                    break  # Break loop if a match is found
    return filtered_list