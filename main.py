from fastapi import FastAPI, Response, status, HTTPException, Query
from fastapi_pagination import LimitOffsetPage, add_pagination, paginate
from typing import List
from cassandra.policies import RoundRobinPolicy, ExponentialReconnectionPolicy
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from collections import defaultdict
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
import os
import uvicorn
import logging
import pandas as pd
from tqdm import tqdm

# Loading environment variables
load_dotenv()

app = FastAPI()

# Configure logging
logging.basicConfig(level=logging.DEBUG)

# CORS setup
origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

add_pagination(app)

@app.get("/test-connection", status_code=200)
async def test_connection():
    try:
        CLUSTER_LINK = os.environ.get('CLUSTER_LINK')
        KEYSPACE = os.environ.get('KEYSPACE')
        PORT = int(os.environ.get('PORT', '9042'))
        USERNAME = os.environ.get('CASSANDRA_USER')
        PASSWORD = os.environ.get('CASSANDRA_PASS')

        auth_provider = PlainTextAuthProvider(username=USERNAME, password=PASSWORD)
        cluster = Cluster([CLUSTER_LINK], port=PORT, auth_provider=auth_provider, connect_timeout=20)
        session = cluster.connect()
        session.set_keyspace(KEYSPACE)

        return {"status": "success", "message": "Connected to Cassandra successfully!"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/env-check", status_code=200)
async def env_check():
    return {
        "CLUSTER_LINK": os.environ.get('CLUSTER_LINK'),
        "KEYSPACE": os.environ.get('KEYSPACE'),
        "PORT": os.environ.get('PORT'),
        "CASSANDRA_USER": os.environ.get('CASSANDRA_USER'),
        "CASSANDRA_PASS": os.environ.get('CASSANDRA_PASS')
    }

@app.get("/course/v1/progress/reports/csv/{courseid}", status_code=200)
async def get_user(
    courseid: str,
    page_number: int = Query(default=0, description="For pagination"),
    batchid: str = Query(default=None, description="Batch ID (optional)"),
    query: str = Query(default=None, description="Pass username only."),
    offset: int = Query(default=0, description="Offset for pagination"),
    limit: int = Query(default=0, description="Limit for pagination")
):
    try:
        CLUSTER_LINK = os.environ.get('CLUSTER_LINK')
        KEYSPACE = os.environ.get('KEYSPACE')
        PORT = int(os.environ.get('PORT', '9042'))
        USERNAME = os.environ.get('CASSANDRA_USER')
        PASSWORD = os.environ.get('CASSANDRA_PASS')
        TABLE = os.environ.get('TABLE', 'user_enrolments')

        auth_provider = PlainTextAuthProvider(username=USERNAME, password=PASSWORD)
        cluster = Cluster(
            [CLUSTER_LINK],
            port=PORT,
            auth_provider=auth_provider,
            load_balancing_policy=RoundRobinPolicy(),
            reconnection_policy=ExponentialReconnectionPolicy(max_attempts=10, max_delay=20, base_delay=10)
        )
        session = cluster.connect()
        session.set_keyspace(KEYSPACE)

        prepared_query_courseid = session.prepare(
            f"SELECT userid, enrolled_date, courseid, batchid, progress, completionpercentage, "
            f"completedon, issued_certificates, status FROM {KEYSPACE}.{TABLE} WHERE courseid = ? ALLOW FILTERING;"
        )

        prepared_query_courseid_batchid = session.prepare(
            f"SELECT userid, enrolled_date, courseid, batchid, progress, completionpercentage, "
            f"completedon, issued_certificates, status FROM {KEYSPACE}.{TABLE} WHERE courseid = ? AND batchid = ? ALLOW FILTERING;"
        )

        if not courseid:
            raise HTTPException(status_code=400, detail='Please pass inputs!')

        if courseid and not batchid:
            query_result = session.execute(prepared_query_courseid, (courseid,))
        else:
            query_result = session.execute(prepared_query_courseid_batchid, (courseid, batchid))

        # Assuming get_all_personal_info() and get_all_batch_info() are defined in utils
        from utils import filter_query_dict, get_all_personal_info, get_all_batch_info

        all_personal_info = get_all_personal_info()
        all_batch_info = get_all_batch_info(courseid, batchid=batchid) if batchid else get_all_batch_info(courseid, batchid='')

        batchname, start_date, end_date = '', '', ''

        for row in all_batch_info:
            batchname = row.name
            start_date = row.start_date
            end_date = row.end_date

        found_user_ids, enrolled_date_dict, progress_dict, completionpercentage_dict, completedon_dict, issued_certificates_dict, status_dict = [], {}, {}, {}, {}, {}, {}

        for row in query_result:
            found_user_ids.append(row.userid)
            enrolled_date_dict[row.userid] = row.enrolled_date
            progress_dict[row.userid] = row.progress
            completionpercentage_dict[row.userid] = row.completionpercentage
            completedon_dict[row.userid] = row.completedon
            issued_certificates_dict[row.userid] = row.issued_certificates[0]['name'] if row.issued_certificates else None
            status_dict[row.userid] = row.status

        total_items = len(query_result.current_rows)
        total_pages = (total_items + limit - 1) // limit if limit > 0 else 1
        start_index = page_number * limit
        end_index = min(start_index + limit, total_items)

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

        filtered_entries = []
        for entry in tqdm(all_personal_info):
            if entry.userid in found_user_ids:
                temp_dict = {
                    "user_id": entry.userid,
                    "batchname": batchname if batchname else None,
                    "coursename": 'COURSENAME',
                    "userName": f"{entry.firstname} {entry.lastname}" if entry.lastname else entry.firstname,
                    "userEmail": entry.email,
                    "maskedEmail": entry.maskedemail,
                    "maskedPhone": entry.maskedphone,
                    "Phone": entry.phone,
                    "enrolled_date": enrolled_date_dict.get(entry.userid, ""),
                    "courseid": courseid,
                    "batchid": batchid if batchid else None,
                    "progress": progress_dict.get(entry.userid, ""),
                    "completionpercentage": completionpercentage_dict.get(entry.userid, ""),
                    "completedon": completedon_dict.get(entry.userid, ""),
                    "issued_certificates": issued_certificates_dict.get(entry.userid, ""),
                    "start_date": start_date if start_date else None,
                    "end_date": end_date if end_date else None,
                    "status": status_dict.get(entry.userid, "")
                }
                filtered_entries.append(temp_dict)

        if query:
            filtered_entries = filter_query_dict(filtered_entries, query)

        result_dict['result']['total_items'] = len(filtered_entries)
        result_dict['result']['content'] = filtered_entries[start_index:end_index]

        if result_dict['result']['total_items'] == 0:
            result_dict['params']['err'] = 'RESOURCE_NOT_FOUND'
            result_dict['responseCode'] = 'Success'
            result_dict['params']['status'] = 'Success'
        else:
            result_dict['responseCode'] = 'OK'

        return result_dict

    except Exception as e:
        logging.error(f"Error: {e}")
        result_dict = {
            "id": "api.content.read",
            "ver": "1.0",
            "ts": "2023-10-17T09:01:10.366Z",
            "params": {
                "resmsgid": "b74027e0-6ccb-11ee-bd65-e3b07a244498",
                "msgid": "b7326c40-6ccb-11ee-b8a9-8d3734381433",
                "status": "failed",
                "err": "RESOURCE_NOT_FOUND",
                "errmsg": str(e)
            },
            "responseCode": "RESOURCE_NOT_FOUND",
            "result": {
                "total_pages": 0,
                "current_page": page_number,
                "items_per_page": limit,
                "total_items": 0,
                "content": []
            }
        }
        return result_dict

if __name__ == "__main__":
    import os
    host = os.environ.get('HOST', '0.0.0.0')
    port = int(os.environ.get('PORT', '8000'))
    uvicorn.run(app, host=host, port=port)
