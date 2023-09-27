# Imports.


from fastapi import FastAPI, Response, status,HTTPException
from fastapi import Request
from fastapi_pagination import LimitOffsetPage, add_pagination, paginate
from typing import List
from fastapi import Query

import uvicorn

import requests

# Loading from utils.

from utils import get_personal_info,get_course_name
from utils import temp_response,batch_start_date, batch_end_date
from utils import filter_query_dict


import os
from cassandra.cluster import Cluster
import pandas as pd
from tqdm import tqdm

# Loading env.
from dotenv import load_dotenv

load_dotenv()


PAGE_SIZE = 10

# 1. Initialise fast.
app = FastAPI()
add_pagination(app)



# 2.Setting Keyspaces.

result = {"result":{"count":"","content":[]}}


@app.get("/course/v1/progress/reports/{courseid}",status_code=200)

async def get_user(
    courseid: str,
    page_number: int = Query(default=0, description="For pagination"),
    batchid: str = Query(default=None, description="Batch ID (optional)"),
    query: str = Query(default=None, description="Pass username only."),
    offset: int = Query(default=0, description="Offset for pagination"),
    limit: int = Query(default= 0, description="Limit for pagination")
):  
    CLUSTER_LINK = os.environ.get('CLUSTER_LINK')
    KEYSPACE= os.environ.get('KEYSPACE')
    PORT = os.environ.get('PORT')
    TABLE = os.environ.get('TABLE')
    
    cluster = Cluster([f'{CLUSTER_LINK}'])
    session = cluster.connect()
    session.default_timeout = 60
    session.set_keyspace(KEYSPACE)
    
    TABLE = 'user_enrolments'

    print('Cluster connected.')
  
    temporary_list = []
    
    if not courseid:
        return 'please pass inputs !!'

    if courseid and not batchid:
        
        query_db = f"SELECT userid, enrolled_date, courseid, batchid, progress, completionpercentage, completedon, issued_certificates, status FROM {KEYSPACE}.{TABLE} WHERE courseid = '{courseid}' ALLOW FILTERING;"
    else:
        query_db = f"SELECT userid, enrolled_date, courseid, batchid, progress, completionpercentage, completedon, issued_certificates, status FROM {KEYSPACE}.{TABLE} WHERE courseid = '{courseid}' AND batchid = '{batchid}' ALLOW FILTERING;"
  
    try:
        query_result = session.execute(query=query_db)
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
            "result": {
                "total_pages": total_pages,
                "current_page": page_number,
                "items_per_page": limit,
                "total_items": total_items,
                "content": []
            }
        }

        for i in tqdm(query_result[limit_1:limit_2]):
            temp_dict = {
                "user_id": i[0],
                
                "coursename": get_course_name(i[2],i[3][0]),
                "batchname": 'Batchname',
                "userName": get_personal_info(i[0])['username'],
                "userEmail": get_personal_info(i[0])['email'],
                "maskedemail":get_personal_info(i[0])['maskedemail'],
                "maskedphone":get_personal_info(i[0])['maskedphone'],
                "Phone": get_personal_info(i[0])['phone'],
                "enrolled_date": i[1],
                "courseid": i[2],
                "batchid": i[3],
                "progress": i[4],
                "completionpercentage": i[5],
                "completedon": i[6],
                "issued_certificates": i[7][0]['name'] if i[7] else '',
                "batch_start_date":batch_start_date(courseid,batchid),
                "batch_end_date":batch_end_date(courseid,batchid),
                "status": i[8],
                
            }
            
            temporary_list.append(temp_dict)
            result_dict["result"]["content"].append(temp_dict)

        if query:
            filtered_list = filter_query_dict(result_dict["result"]["content"], query)
            result_dict["result"]["content"] = filtered_list
            return result_dict
        else:
            return result_dict

    except Exception as e:
        raise HTTPException(status_code=400, detail="unable to process request")


@app.get("/course/v1/progress/reports/csv/{courseid}",status_code=200)

async def get_user(
    courseid: str,
    page_number: int = Query(default=0, description="For pagination"),
    batchid: str = Query(default=None, description="Batch ID (optional)"),
    query: str = Query(default=None, description="Pass username only."),
    offset: int = Query(default=0, description="Offset for pagination"),
    limit: int = Query(default= 0, description="Limit for pagination")
):  
    CLUSTER_LINK = os.environ.get('CLUSTER_LINK')
    KEYSPACE= os.environ.get('KEYSPACE')
    PORT = os.environ.get('PORT')
    TABLE = os.environ.get('TABLE')
    
    cluster = Cluster([f'{CLUSTER_LINK}'])
    session = cluster.connect()
    session.default_timeout = 60
    session.set_keyspace(KEYSPACE)
    
    TABLE = 'user_enrolments'

    print('Cluster connected.')
  
    temporary_list = []
    
    if not courseid:
        return 'please pass inputs !!'

    if courseid and not batchid:
        
        query_db = f"SELECT userid, enrolled_date, courseid, batchid, progress, completionpercentage, completedon, issued_certificates, status FROM {KEYSPACE}.{TABLE} WHERE courseid = '{courseid}' ALLOW FILTERING;"
    else:
        query_db = f"SELECT userid, enrolled_date, courseid, batchid, progress, completionpercentage, completedon, issued_certificates, status FROM {KEYSPACE}.{TABLE} WHERE courseid = '{courseid}' AND batchid = '{batchid}' ALLOW FILTERING;"
  
    try:
        query_result = session.execute(query=query_db)
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
            "result": {
                "total_pages": total_pages,
                "current_page": page_number,
                "items_per_page": limit,
                "total_items": total_items,
                "content": []
            }
        }

        for i in tqdm(query_result[limit_1:limit_2]):
            temp_dict = {
                "user_id": i[0],
                
                "coursename": get_course_name(i[2],i[3][0]),
                "batchname": 'Batchname',
                "userName": get_personal_info(i[0])['username'],
                "userEmail": get_personal_info(i[0])['email'],
                "maskedemail":get_personal_info(i[0])['maskedemail'],
                "maskedphone":get_personal_info(i[0])['maskedphone'],
                "Phone": get_personal_info(i[0])['phone'],
                "enrolled_date": i[1],
                "courseid": i[2],
                "batchid": i[3],
                "progress": i[4],
                "completionpercentage": i[5],
                "completedon": i[6],
                "issued_certificates": i[7][0]['name'] if i[7] else '',
                "batch_start_date":batch_start_date(courseid,batchid),
                "batch_end_date":batch_end_date(courseid,batchid),
                "status": i[8],
                
            }
            
            temporary_list.append(temp_dict)
            result_dict["result"]["content"].append(temp_dict)

        if query:
            filtered_list = filter_query_dict(result_dict["result"]["content"], query)
            result_dict["result"]["content"] = filtered_list
            return result_dict
        else:
            return result_dict

    except Exception as e:
        raise HTTPException(status_code=400, detail="unable to process request")



@app.get("/learner/v1/course/enrolmentsummary/{channelid}/{courseid}/")
async def get_progress_report(
    channelid: str, courseid: str,
    offset: int = Query(default=0, description="Offset for pagination"),
    limit: int = Query(default=10, description="Limit for pagination"),
    page_number: int = Query(default=0, description="For pagination"),
):  
    return temp_response()
    

if __name__ == "__main__":

    uvicorn.run(app, host="127.0.0.1", port=9009)