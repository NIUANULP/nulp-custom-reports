from fastapi import FastAPI, Response, status,HTTPException
from fastapi import Request
from fastapi_pagination import LimitOffsetPage, add_pagination, paginate
from typing import List
from fastapi import Query
from cassandra.policies import RoundRobinPolicy,DCAwareRoundRobinPolicy, ExponentialReconnectionPolicy
from cassandra.cluster import Cluster
from collections import defaultdict
from fastapi.middleware.cors import CORSMiddleware


# Loading from utils.

from utils import filter_query_dict, get_all_personal_info, get_all_batch_info

import os
import uvicorn
import pandas as pd
from tqdm import tqdm

# Loading env.
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

origins = [
    "*",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

add_pagination(app)

@app.get("/course/v1/progress/reports/csv/{courseid}", status_code=200)
async def get_user(

    courseid: str,
    page_number: int = Query(default=0, description="For pagination"),
    batchid: str = Query(default=None, description="Batch ID (optional)"),
    query: str = Query(default=None, description="Pass username only."),
    offset: int = Query(default=0, description="Offset for pagination"),
    limit: int = Query(default=0, description="Limit for pagination")
):
    CLUSTER_LINK = os.environ.get('CLUSTER_LINK')
    KEYSPACE = os.environ.get('KEYSPACE')
    PORT = os.environ.get('PORT')
    TABLE = os.environ.get('TABLE')

    cluster = Cluster(
        [f'{CLUSTER_LINK}'],
        connect_timeout=20,
        load_balancing_policy=RoundRobinPolicy(),
        reconnection_policy=ExponentialReconnectionPolicy(max_attempts=10, max_delay=20, base_delay=10)
    )
    session = cluster.connect()
    session.default_timeout = 60
    session.set_keyspace(KEYSPACE)

    TABLE = 'user_enrolments'

    print('Cluster connected.')

    temporary_list = []

    # Define the prepared statements

    prepared_query_courseid = session.prepare(
        f"SELECT userid, enrolled_date, courseid, batchid, progress, completionpercentage, "
        f"completedon, issued_certificates, status FROM {KEYSPACE}.{TABLE} WHERE courseid = ? ALLOW FILTERING;"
    )

    prepared_query_courseid_batchid = session.prepare(
        f"SELECT userid, enrolled_date, courseid, batchid, progress, completionpercentage, "
        f"completedon, issued_certificates, status FROM {KEYSPACE}.{TABLE} WHERE courseid = ? AND batchid = ? ALLOW FILTERING;"
    )

    if not courseid:
        return 'please pass inputs !!'

    if courseid and not batchid:
        
        query_result = session.execute(prepared_query_courseid, (courseid,))
    else:
        query_result = session.execute(prepared_query_courseid_batchid, (courseid, batchid))

    # Get full personal info.
    all_personal_info = get_all_personal_info()
    
    if courseid and batchid:
        all_batch_info = get_all_batch_info(courseid,batchid=batchid)
    else:
        all_batch_info = get_all_batch_info(courseid,batchid='')

    batchname = ''
    start_date = ''
    end_date = ''

    for row in all_batch_info:
        batchname = (row.name)
        start_date = row.start_date
        end_date = row.end_date

        #print(row.start_date)

    batch_end_date = ''
    found_user_ids = []
    enrolled_date_dict = {}
    courseid_dict = {}
    batchids_dict = {}
    progress_dict = {}
    completionpercentage_dict = {}
    completedon_dict = {}
    issued_certificates_dict = {}
    batch_start_date_dict = {}
    batch_end_date_dict = {}
    status_dict = {}

    for row in query_result:
        if not batchid:
            found_user_ids.append(row.userid)
            
            enrolled_date_dict[row.userid] = row.enrolled_date
            courseid_dict[row.userid] = row.courseid
            #batchids_dict[row.batchid] = row.batchid
            progress_dict[row.userid] = row.progress
            completionpercentage_dict[row.userid] = row.completionpercentage
            completedon_dict[row.userid] = row.completedon
            issued_certificates_dict[row.userid] = row.issued_certificates[0]['name'] if row.issued_certificates else None
            status_dict[row.userid] = row.status
        
        else:
            if courseid and batchid:
                found_user_ids.append(row.userid)
            
                enrolled_date_dict[row.userid] = row.enrolled_date
                courseid_dict[row.userid] = row.courseid
                batchids_dict[row.batchid] = row.batchid
                progress_dict[row.userid] = row.progress
                completionpercentage_dict[row.userid] = row.completionpercentage
                completedon_dict[row.userid] = row.completedon
                issued_certificates_dict[row.userid] = row.issued_certificates[0]['name'] if row.issued_certificates else None
                status_dict[row.userid] = row.status
                
    try:

        total_items = len(query_result.current_rows)

        if limit == 0:
            total_pages = 1
        else:
            total_pages = (total_items + limit - 1) // limit

        start_index = page_number * limit
        end_index = min(start_index + limit, total_items)
        
        if start_index == 0 and end_index == 0:
            limit_1 = None
            limit_2 = None
        else:
            limit_1 = start_index
            limit_2 = end_index

        result_dict = {
            "id": "api.content.read",
            "ver": "1.0",
            "ts": "2023-10-17T09:01:10.366Z",
            "params": {
                "resmsgid": "b74027e0-6ccb-11ee-bd65-e3b07a244498",
                "msgid": "b7326c40-6ccb-11ee-b8a9-8d3734381433",
                "status": "successful",
                "err": None,
                "errmsg": None
                        },
            "responseCode": "OK",
                    "result": {
                        "total_pages": total_pages,
                        "current_page": page_number,
                        "items_per_page": limit,
                        "total_items": total_items,
                        "content": []
                    }
        }

        # Get full personal info.
        filtered_entries = []
        
        if page_number != 0:
            start_index=(page_number - 1) * limit
            end_index=page_number * limit
        else:
            start_index= None
            end_index= None

        # Iterate through all_personal_info.
        for entry in tqdm(all_personal_info):
            if entry.userid in found_user_ids:
                temp_dict = {
                    "user_id": entry.userid,
                    "batchname": batchname if batchname else None,
                    "coursename": 'COURSENAME',
                    "userName": str(entry.firstname) + str(entry.lastname),
                    "userEmail": entry.email,
                    "maskedEmail": entry.maskedemail,
                    "maskedPhone": entry.maskedphone,
                    "Phone": entry.phone,
                    "enrolled_date": enrolled_date_dict.get(entry.userid, ""),
                    "courseid":courseid,
                    "batchid":batchid if batchid else None,
                    "progress":progress_dict.get(entry.userid, ""),
                    "completionpercentage":completionpercentage_dict.get(entry.userid, ""),
                    "completedon": completedon_dict.get(entry.userid, ""),
                    "issued_certificates":issued_certificates_dict.get(entry.userid, ""),
                    "start_date":start_date if start_date else None,
                    "end_date": end_date if end_date else None,
                    "status": status_dict.get(entry.userid, ""),     
                }
                filtered_entries.append(temp_dict)

        if query:
            filtered_entries = filter_query_dict(filtered_entries,query)
            result_dict['result']['total_items'] = len(filtered_entries)
            result_dict['result']['content'] = filtered_entries[start_index:end_index]
        
        else:
            
            result_dict['result']['total_items'] = len(filtered_entries)
            result_dict['result']['content'] = filtered_entries[start_index:end_index]
        
        if result_dict['result']['total_items'] == 0:

            result_dict['params']['status'] = None
            result_dict['params']['err'] = 'RESOURCE_NOT_FOUND'
            result_dict['responseCode'] = 'INPUTS_NOT_FOUND'
            result_dict['params']['status'] = 'Failed'
            return result_dict
        
        else:
            result_dict['responseCode'] = 'OK'
            return result_dict

    except Exception as e:

        result_dict['responseCode'] = 'RESOURCE_NOT_FOUND'
        result_dict['params']['msgid'] = None
        result_dict['params']['status'] = None
        result_dict['params']['err'] = Exception
        result_dict['params']["errmsg"]: e

        return result_dict
       #raise HTTPException(status_code=400, detail="Unable to process request")     

@app.get("/course/v1/progress/reports/{courseid}", status_code=200)
async def get_user(

    courseid: str,
    page_number: int = Query(default=0, description="For pagination"),
    batchid: str = Query(default=None, description="Batch ID (optional)"),
    query: str = Query(default=None, description="Pass username only."),
    offset: int = Query(default=0, description="Offset for pagination"),
    limit: int = Query(default=0, description="Limit for pagination")
):
    CLUSTER_LINK = os.environ.get('CLUSTER_LINK')
    KEYSPACE = os.environ.get('KEYSPACE')
    PORT = os.environ.get('PORT')
    TABLE = os.environ.get('TABLE')

    cluster = Cluster(
        [f'{CLUSTER_LINK}'],
        connect_timeout=20,
        load_balancing_policy=RoundRobinPolicy(),
        reconnection_policy=ExponentialReconnectionPolicy(max_attempts=10, max_delay=20, base_delay=10)
    )
    session = cluster.connect()
    session.default_timeout = 60
    session.set_keyspace(KEYSPACE)

    TABLE = 'user_enrolments'

    print('Cluster connected.')

    temporary_list = []

    # Define the prepared statements

    prepared_query_courseid = session.prepare(
        f"SELECT userid, enrolled_date, courseid, batchid, progress, completionpercentage, "
        f"completedon, issued_certificates, status FROM {KEYSPACE}.{TABLE} WHERE courseid = ? ALLOW FILTERING;"
    )

    prepared_query_courseid_batchid = session.prepare(
        f"SELECT userid, enrolled_date, courseid, batchid, progress, completionpercentage, "
        f"completedon, issued_certificates, status FROM {KEYSPACE}.{TABLE} WHERE courseid = ? AND batchid = ? ALLOW FILTERING;"
    )

    if not courseid:
        return 'please pass inputs !!'

    if courseid and not batchid:
        
        query_result = session.execute(prepared_query_courseid, (courseid,))
    else:
        query_result = session.execute(prepared_query_courseid_batchid, (courseid, batchid))

    # Get full personal info.
    all_personal_info = get_all_personal_info()
    
    if courseid and batchid:
        all_batch_info = get_all_batch_info(courseid,batchid=batchid)
    else:
        all_batch_info = get_all_batch_info(courseid,batchid='')

    batchname = ''
    start_date = ''
    end_date = ''

    for row in all_batch_info:
        batchname = (row.name)
        start_date = row.start_date
        end_date = row.end_date

        #print(row.start_date)

    batch_end_date = ''
    found_user_ids = []
    enrolled_date_dict = {}
    courseid_dict = {}
    batchids_dict = {}
    progress_dict = {}
    completionpercentage_dict = {}
    completedon_dict = {}
    issued_certificates_dict = {}
    batch_start_date_dict = {}
    batch_end_date_dict = {}
    status_dict = {}

    for row in query_result:
        if not batchid:
            found_user_ids.append(row.userid)
            
            enrolled_date_dict[row.userid] = row.enrolled_date
            courseid_dict[row.userid] = row.courseid
            #batchids_dict[row.batchid] = row.batchid
            progress_dict[row.userid] = row.progress
            completionpercentage_dict[row.userid] = row.completionpercentage
            completedon_dict[row.userid] = row.completedon
            issued_certificates_dict[row.userid] = row.issued_certificates[0]['name'] if row.issued_certificates else None
            status_dict[row.userid] = row.status
        
        else:
            if courseid and batchid:
                found_user_ids.append(row.userid)
            
                enrolled_date_dict[row.userid] = row.enrolled_date
                courseid_dict[row.userid] = row.courseid
                batchids_dict[row.batchid] = row.batchid
                progress_dict[row.userid] = row.progress
                completionpercentage_dict[row.userid] = row.completionpercentage
                completedon_dict[row.userid] = row.completedon
                issued_certificates_dict[row.userid] = row.issued_certificates[0]['name'] if row.issued_certificates else None
                status_dict[row.userid] = row.status
                
    try:

        total_items = len(query_result.current_rows)

        if limit == 0:
            total_pages = 1
        else:
            total_pages = (total_items + limit - 1) // limit

        start_index = page_number * limit
        end_index = min(start_index + limit, total_items)
        
        if start_index == 0 and end_index == 0:
            limit_1 = None
            limit_2 = None
        else:
            limit_1 = start_index
            limit_2 = end_index

        result_dict = {
            "id": "api.content.read",
            "ver": "1.0",
            "ts": "2023-10-17T09:01:10.366Z",
            "params": {
                "resmsgid": "b74027e0-6ccb-11ee-bd65-e3b07a244498",
                "msgid": "b7326c40-6ccb-11ee-b8a9-8d3734381433",
                "status": "successful",
                "err": None,
                "errmsg": None
                        },
            "responseCode": "OK",
                    "result": {
                        "total_pages": total_pages,
                        "current_page": page_number,
                        "items_per_page": limit,
                        "total_items": total_items,
                        "content": []
                    }
        }

        # Get full personal info.
        filtered_entries = []
        
        if page_number != 0:
            start_index=(page_number - 1) * limit
            end_index=page_number * limit
        else:
            start_index= None
            end_index= None

        # Iterate through all_personal_info.
        for entry in tqdm(all_personal_info):
            if entry.userid in found_user_ids:
                temp_dict = {
                    "user_id": entry.userid,
                    "batchname": batchname if batchname else None,
                    "coursename": 'COURSENAME',
                    "userName": str(entry.firstname) + str(entry.lastname),
                    "userEmail": entry.email,
                    "maskedEmail": entry.maskedemail,
                    "maskedPhone": entry.maskedphone,
                    "Phone": entry.phone,
                    "enrolled_date": enrolled_date_dict.get(entry.userid, ""),
                    "courseid":courseid,
                    "batchid":batchid if batchid else None,
                    "progress":progress_dict.get(entry.userid, ""),
                    "completionpercentage":completionpercentage_dict.get(entry.userid, ""),
                    "completedon": completedon_dict.get(entry.userid, ""),
                    "issued_certificates":issued_certificates_dict.get(entry.userid, ""),
                    "start_date":start_date if start_date else None,
                    "end_date": end_date if end_date else None,
                    "status": status_dict.get(entry.userid, ""),     
                }
                filtered_entries.append(temp_dict)

        if query:
            filtered_entries = filter_query_dict(filtered_entries,query)
            result_dict['result']['total_items'] = len(filtered_entries)
            result_dict['result']['content'] = filtered_entries[start_index:end_index]
        
        else:
            
            result_dict['result']['total_items'] = len(filtered_entries)
            result_dict['result']['content'] = filtered_entries[start_index:end_index]
        
        if result_dict['result']['total_items'] == 0:

            result_dict['params']['status'] = None
            result_dict['params']['err'] = 'RESOURCE_NOT_FOUND'
            result_dict['responseCode'] = 'INPUTS_NOT_FOUND'
            result_dict['params']['status'] = 'Failed'
            return result_dict
        
        else:
            result_dict['responseCode'] = 'OK'
            return result_dict

    except Exception as e:

        result_dict['responseCode'] = 'RESOURCE_NOT_FOUND'
        result_dict['params']['msgid'] = None
        result_dict['params']['status'] = None
        result_dict['params']['err'] = Exception
        result_dict['params']["errmsg"]: e

        return result_dict


if __name__ == "__main__":

    uvicorn.run(app, host="127.0.0.1", port=8009)
