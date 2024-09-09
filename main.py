from fastapi import FastAPI, Query
from cassandra.cluster import Cluster
from cassandra.policies import RoundRobinPolicy, ExponentialReconnectionPolicy
from cassandra.auth import PlainTextAuthProvider  # Add authentication
from fastapi.middleware.cors import CORSMiddleware
from fastapi_pagination import add_pagination
from utils import filter_query_dict, get_all_personal_info, get_all_batch_info
from dotenv import load_dotenv
import os
import uvicorn
import pandas as pd
from tqdm import tqdm

# Load environment variables
load_dotenv()

app = FastAPI()

# Allow CORS for all origins
origins = ["*"]
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
    # Fetch environment variables for Cassandra connection
    CLUSTER_LINK = os.environ.get('CLUSTER_LINK')  # Cassandra IP address
    KEYSPACE = os.environ.get('KEYSPACE')  # Cassandra keyspace
    PORT = int(os.environ.get('PORT', '9042'))  # Default Cassandra port is 9042
    TABLE = os.environ.get('TABLE', 'user_enrolments')  # Default table

    # Authentication provider for Cassandra (if authentication is required)
    auth_provider = PlainTextAuthProvider(
        username=os.environ.get('CASSANDRA_USER'),
        password=os.environ.get('CASSANDRA_PASS')
    )

    # Initialize the Cassandra cluster
    cluster = Cluster(
        [CLUSTER_LINK],
        port=PORT,
        auth_provider=auth_provider,  # Optional if no authentication is used
        connect_timeout=20,
        load_balancing_policy=RoundRobinPolicy(),
        reconnection_policy=ExponentialReconnectionPolicy(max_attempts=10, max_delay=20, base_delay=10)
    )

    # Establish the connection and set keyspace
    session = cluster.connect()
    session.default_timeout = 60
    session.set_keyspace(KEYSPACE)

    print('Cluster connected.')

    # Prepare CQL queries
    prepared_query_courseid = session.prepare(
        f"SELECT userid, enrolled_date, courseid, batchid, progress, completionpercentage, "
        f"completedon, issued_certificates, status FROM {KEYSPACE}.{TABLE} WHERE courseid = ? ALLOW FILTERING;"
    )

    prepared_query_courseid_batchid = session.prepare(
        f"SELECT userid, enrolled_date, courseid, batchid, progress, completionpercentage, "
        f"completedon, issued_certificates, status FROM {KEYSPACE}.{TABLE} WHERE courseid = ? AND batchid = ? ALLOW FILTERING;"
    )

    if not courseid:
        return 'Please pass inputs!'

    # Execute queries based on whether batchid is provided
    if courseid and not batchid:
        query_result = session.execute(prepared_query_courseid, (courseid,))
    else:
        query_result = session.execute(prepared_query_courseid_batchid, (courseid, batchid))

    # Retrieve personal and batch info
    all_personal_info = get_all_personal_info()
    all_batch_info = get_all_batch_info(courseid, batchid=batchid if batchid else '')

    # Extract batch info if available
    batchname, start_date, end_date = '', '', ''
    for row in all_batch_info:
        batchname = row.name
        start_date = row.start_date
        end_date = row.end_date

    # Dictionaries to store fetched data
    found_user_ids = []
    enrolled_date_dict, courseid_dict, batchids_dict = {}, {}, {}
    progress_dict, completionpercentage_dict, completedon_dict = {}, {}, {}
    issued_certificates_dict, status_dict = {}, {}

    for row in query_result:
        found_user_ids.append(row.userid)
        enrolled_date_dict[row.userid] = row.enrolled_date
        courseid_dict[row.userid] = row.courseid
        progress_dict[row.userid] = row.progress
        completionpercentage_dict[row.userid] = row.completionpercentage
        completedon_dict[row.userid] = row.completedon
        issued_certificates_dict[row.userid] = row.issued_certificates[0]['name'] if row.issued_certificates else None
        status_dict[row.userid] = row.status
        if batchid:
            batchids_dict[row.batchid] = row.batchid

    # Pagination and result preparation
    total_items = len(query_result.current_rows)
    total_pages = (total_items + limit - 1) // limit if limit else 1
    start_index, end_index = page_number * limit, min(page_number * limit + limit, total_items)

    result_dict = {
        "id": "api.content.read",
        "ver": "1.0",
        "ts": pd.Timestamp.now().isoformat(),
        "params": {"status": "successful", "err": None, "errmsg": None},
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
            filtered_entries.append({
                "user_id": entry.userid,
                "batchname": batchname,
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
                "start_date": start_date,
                "end_date": end_date,
                "status": status_dict.get(entry.userid, "")
            })

    result_dict['result']['content'] = filtered_entries[start_index:end_index]

    if not filtered_entries:
        result_dict['params']['err'] = 'RESOURCE_NOT_FOUND'
        result_dict['responseCode'] = 'RESOURCE_NOT_FOUND'

    return result_dict

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=9009)
