import aiohttp
import logging
import asyncio
import os
import generateN1notice
from PyPDF2 import PdfReader, PdfWriter
import aiofiles
import aiofiles.os
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

from rate_limiter import semaphore

# -------------------- small helpers --------------------
def _is_ignored(v) -> bool:
    """Normalize the ignored flag; treat only literal 'Y' (any case/space) as ignored."""
    return str(v or "").strip().upper() == "Y"

def _safe_get(d: dict, path: list, default=None):
    """Safely drill into nested dicts."""
    cur = d
    for k in path:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur

# -------------------- generic fetch with simple 429 handling --------------------
async def fetch_data(session, url, headers, method: str = "GET"):
    """Fetch data asynchronously with a small 429 backoff and a semaphore."""
    logging.info("Fetching Data From Buildium")
    try:
        async with semaphore:
            while True:
                async with session.request(method, url, headers=headers) as response:
                    status_code = response.status
                    try:
                        data = await response.json()
                    except Exception:
                        text = await response.text()
                        logging.info(f"Non-JSON response from {url}: {text[:300]}")
                        data = {}

                    if status_code == 429:
                        logging.info("Rate limit reached, sleeping for 0.201 seconds")
                        await asyncio.sleep(0.201)
                        continue

                    if isinstance(data, (dict, list)):
                        return data
                    logging.info(f"Unexpected response format: {data!r}")
                    return {}
    except Exception as e:
        logging.info(f"Error fetching data from {url}: {e}")
        return {}

# -------------------- categories --------------------
async def category(headers, session, date_label: str):
    """Find or create a Files Category named 'Increases {date_label}' and return its Id."""
    url = "https://api.buildium.com/v1/files/categories"
    params = {'limit': 1000}
    async with semaphore:
        async with session.get(url, headers=headers, params=params) as response:
            if response.status != 200:
                logging.error(f"Failed to fetch file categories: {response.status} {await response.text()}")
                return None

            category_list = await response.json()
            category_id = None
            for cat in category_list:
                if cat.get('Name') == f'Increases {date_label}':
                    category_id = cat.get('Id')
                    break

            if category_id is None:
                payload = {'Name': f'Increases {date_label}'}
                async with semaphore:
                    async with session.post(url, headers=headers, json=payload) as r2:
                        if r2.status != 201:
                            logging.error(f"Failed creating file category: {r2.status} {await r2.text()}")
                            return None
                        cat_json = await r2.json()
                        category_id = cat_json.get('Id')
    return category_id

# -------------------- presigned form helpers --------------------
_ORDER_TASK = [
    "Key", "ACL", "Policy", "Content-Type", "Content-Disposition",
    "X-Amz-Algorithm", "X-Amz-Credential", "X-Amz-Date", "X-Amz-Signature",
    "X-Amz-Meta-Buildium-Entity-Type", "X-Amz-Meta-Buildium-Entity-Id",
    "X-Amz-Meta-Buildium-File-Source", "X-Amz-Meta-Buildium-File-Description",
    "X-Amz-Meta-Buildium-Account-Id", "X-Amz-Meta-Buildium-File-Name",
    "X-Amz-Meta-Buildium-File-Title", "X-Amz-Meta-Buildium-Child-Entity-Id",
    "X-Amz-Meta-Buildium-Finalize-Upload-Message-Version",
]

_ORDER_LEASE = [
    "Key", "ACL", "Policy", "Content-Type", "Content-Disposition",
    "X-Amz-Algorithm", "X-Amz-Credential", "X-Amz-Date", "X-Amz-Signature",
    "X-Amz-Meta-Buildium-Entity-Type", "X-Amz-Meta-Buildium-Entity-Id",
    "X-Amz-Meta-Buildium-File-Source", "X-Amz-Meta-Buildium-File-Description",
    "X-Amz-Meta-Buildium-Account-Id", "X-Amz-Meta-Buildium-File-Name",
    "X-Amz-Meta-Buildium-File-Title", "X-Amz-Meta-Buildium-File-Category-Id",
    "X-Amz-Meta-Buildium-Finalize-Upload-Message-Version",
]

def _form_pairs_from_payload_form(form):
    """
    Yield (key, value) pairs in the best available order.
    Supports dict (unordered) and list (already ordered).
    """
    if isinstance(form, list):
        # Some tenants return an ordered list of dicts; try common key names.
        for entry in form:
            if isinstance(entry, dict):
                k = entry.get("name") or entry.get("Name") or entry.get("key") or entry.get("Key")
                v = entry.get("value") or entry.get("Value")
                if k is not None and v is not None:
                    yield k, v
    elif isinstance(form, dict):
        # Fallback: caller will decide an order (we’ll use our ORDER_* lists).
        for k, v in form.items():
            yield k, v

def _build_formdata_from_form(form, preferred_order):
    """
    Build aiohttp.FormData from presigned form, respecting preferred_order when 'form' is a dict.
    Adds each field exactly once. The caller must add the 'file' field LAST.
    """
    fd = aiohttp.FormData()
    added = set()

    if isinstance(form, dict):
        # Add in preferred order first
        for k in preferred_order:
            if k in form and k not in added:
                fd.add_field(k, form[k])
                added.add(k)
        # Then any remaining keys
        for k, v in form.items():
            if k not in added:
                fd.add_field(k, v)
                added.add(k)
    else:
        # If it's a list, preserve the given order
        for k, v in _form_pairs_from_payload_form(form):
            if k not in added:
                fd.add_field(k, v)
                added.add(k)

    return fd

def _basename_for_upload(path: str) -> str:
    """
    Return just the filename from any path (Windows or POSIX).
    """
    if not path:
        return ""
    # normalize backslashes to slashes first
    norm = path.replace("\\", "/")
    return os.path.basename(norm)

def _ensure_exists(path: str) -> bool:
    try:
        return os.path.isfile(path)
    except Exception:
        return False

# ---------- existing form builders (you already have these) ----------

async def amazondatatask(payload):
    """Build FormData for task-history file uploads (without adding the 'file' field)."""
    form = payload.get("FormData", {})
    form_data = _build_formdata_from_form(form, _ORDER_TASK)
    bucket_url = payload["BucketUrl"]
    return form_data, bucket_url

async def amazondatalease(payload):
    """Build FormData for lease file uploads (without adding the 'file' field)."""
    form = payload.get("FormData", {})
    form_data = _build_formdata_from_form(form, _ORDER_LEASE)
    bucket_url = payload["BucketUrl"]
    return form_data, bucket_url

# -------------------- PDF generation per lease (unchanged) --------------------
async def generateN1files(leaseid, leasedata):
    logging.info(f"Generating Increase Notices for lease {leaseid}")
    filepath = await generateN1notice.create(leaseid, leasedata)
    return filepath

# -------------------- upload to Lease (fixed) --------------------
async def uploadN1filestolease(headers, filepath, leaseid, session, categoryid):
    logging.info(f"Uploading N1 File to Lease {leaseid}")

    try:
        if not _ensure_exists(filepath):
            logging.error(f"Lease upload aborted: file not found at {filepath}")
            return False

        filename = _basename_for_upload(filepath)

        # 1) Get presign
        url = "https://api.buildium.com/v1/files/uploadrequests"
        body = {
            "EntityType": "Lease",
            "EntityId": leaseid,
            "FileName": filename,   # must be just the name
            "Title": filename,
            "CategoryId": categoryid
        }
        async with semaphore:
            async with session.post(url, json=body, headers=headers) as response:
                if response.status != 201:
                    logging.info(f"Error while submitting lease file metadata: {response.status} {await response.text()}")
                    return False

                payload = await response.json()
                form_data, bucket_url = await amazondatalease(payload)

        # 2) Read file and add as LAST field
        async with aiofiles.open(filepath, 'rb') as f:
            file_content = await f.read()
        form_data.add_field("file", file_content, filename=filename, content_type='application/pdf')

        # 3) Upload to S3
        async with semaphore:
            async with session.post(bucket_url, data=form_data) as upload_response:
                if upload_response.status == 204:
                    logging.info(f"Upload of Notice for {leaseid} successful.")
                    return True
                else:
                    logging.info(f"Error Uploading Notice for {leaseid}: {upload_response.status} {await upload_response.text()}")
                    return False

    except Exception as e:
        logging.info(f"An error occurred uploading N1 for lease {leaseid}: {str(e)}")
        return False

# -------------------- upload summary to Task (fixed) --------------------
async def uploadsummarytotask(headers, filepath, taskid, session, categoryid):
    logging.info(f"Uploading Summary to Task {taskid}")

    try:
        if not _ensure_exists(filepath):
            logging.error(f"Task upload aborted: file not found at {filepath}")
            return False

        filename = _basename_for_upload(filepath)

        # 1) Get latest task history id (newest first)
        urltaskhistory = f"https://api.buildium.com/v1/tasks/{taskid}/history"
        async with semaphore:
            async with session.get(urltaskhistory, headers=headers) as response:
                if response.status != 200:
                    logging.info(f"Error while getting task history: {response.status} {await response.text()}")
                    return False

                taskhistorydata = await response.json()
            try:
                taskhistorydata.sort(key=lambda h: h.get("Date") or h.get("CreatedDate") or "", reverse=True)
            except Exception:
                pass
            if not taskhistorydata:
                logging.info("No task history entries found to attach file to.")
                return False
            taskhistoryid = taskhistorydata[0]['Id']

        # 2) Presign for this history entry
        url = f"https://api.buildium.com/v1/tasks/{taskid}/history/{taskhistoryid}/files/uploadrequests"
        body = {"FileName": filename}
        async with semaphore:
            async with session.post(url, json=body, headers=headers) as response:
                if response.status != 201:
                    logging.info(f"Error while submitting task file metadata: {response.status} {await response.text()}")
                    return False

                payload = await response.json()
                form_data, bucket_url = await amazondatatask(payload)

        # 3) Read PDF + add LAST
        async with aiofiles.open(filepath, 'rb') as f:
            file_content = await f.read()
        form_data.add_field("file", file_content, filename=filename, content_type='application/pdf')

        # 4) Upload to S3
        async with semaphore:
            async with session.post(bucket_url, data=form_data) as upload_response:
                if upload_response.status == 204:
                    logging.info(f"Upload successful for Task {taskid}.")
                    return True
                else:
                    logging.info(f"Error Uploading File for Task {taskid}: {upload_response.status} {await upload_response.text()}")
                    return False

    except Exception as e:
        logging.error(f"Error Uploading Summary to task: {e}")
        return False

# -------------------- lease renewal functions disabled --------------------
'''
async def leaserenewalingored(headers, leaseid, lease, session):
    """When a lease is ignored for increases, extend LeaseToDate by +6 months."""
    ...

async def setevictionstatus(leaseid, eviction: bool, session, headers) -> bool:
    """Set IsEvictionPending to the supplied boolean; return True/False for success."""
    ...

async def leaserenewals(headers, leaseid, lease, session, max_retries: int = 3):
    logging.info(f"Processing Lease Renewal for Lease {leaseid}")
    ...
'''

# -------------------- create task per building --------------------
async def createtask(headers, buildingid, session, date_label):
    """Create a task for delivering increase notices for a given building (if needed)."""
    logging.info(f"Creating Task for Building {buildingid}")
    url_rental = f"https://api.buildium.com/v1/rentals/{buildingid}"
    url_cat = "https://api.buildium.com/v1/tasks/categories"
    url_task = "https://api.buildium.com/v1/tasks/todorequests"
    params = {'limit': 1000}

    try:
        # Get rental (for AssignedToUserId and Name)
        async with semaphore:
            async with session.get(url_rental, headers=headers) as response:
                if response.status != 200:
                    logging.error(f"Failed to fetch building data. Status: {response.status}")
                    return None
                rental = await response.json()
                userid = _safe_get(rental, ['RentalManager', 'Id'])
                userid = 352081
                buildingname = rental.get('Name')

        # Find/create task category 'Increase Notices'
        async with semaphore:
            async with session.get(url_cat, params=params, headers=headers) as response:
                if response.status != 200:
                    logging.error(f"Failed to fetch task categories. Status: {response.status}")
                    return None
                category_list = await response.json()
                category_id = None
                for c in category_list:
                    if c.get('Name') == 'Increase Notices':
                        category_id = c.get('Id')
                        break
                if category_id is None:
                    payload = {'Name': 'Increase Notices'}
                    async with semaphore:
                        async with session.post(url_cat, headers=headers, json=payload) as r2:
                            if r2.status != 201:
                                logging.error(f"Failed to create task category. Status: {r2.status}")
                                return None
                            category_id = (await r2.json()).get('Id')

        # Create the task
        payloadtask = {
            'Title': f'Deliver Notices for {date_label} Increases',
            'CategoryId': category_id,
            'Description': 'Please deliver the attached N1 Increase Notices.',
            'PropertyId': buildingid,
            'AssignedToUserId': userid,
            'TaskStatus': "New",
            'Priority': "High",
            'DueDate': datetime.now().strftime("%Y-%m-%d")
        }
        async with semaphore:
            async with session.post(url_task, json=payloadtask, headers=headers) as response:
                if response.status != 201:
                    logging.error(f"Failed to create task. Status: {response.status} {await response.text()}")
                    return None
                task_data = await response.json()
                taskid = task_data.get('Id', 0)
                logging.info(f"Task created successfully with ID: {taskid}")
                return taskid

    except Exception as e:
        logging.error(f"Error creating task: {e}")
        return None

# -------------------- summary helpers --------------------
async def add_summary_page(summary_data, summary_writer, summary_file_path, buildingname, countbuilding, date_label):
    """Generate and append summary page(s) to the active summary writer, then persist to file."""
    try:
        summary_page_buffer = await generateN1notice.create_summary_page(summary_data, buildingname, countbuilding, date_label)
        summary_pdf = PdfReader(summary_page_buffer)
        for page in summary_pdf.pages:
            summary_writer.add_page(page)

        with open(summary_file_path, 'wb') as temp_file:
            summary_writer.write(temp_file)

        logging.info(f"Summary pages added to {summary_file_path}.")
    except Exception as e:
        logging.error(f"Error adding summary pages: {e}")

async def addtosummary(summary_file_path, filepath, summary_writer):
    """Append a lease PDF's pages into the in-memory summary; write to file; enforce size < ~19MB."""
    try:
        reader = PdfReader(filepath)
        for page in reader.pages:
            summary_writer.add_page(page)

        with open(summary_file_path, 'wb') as temp_file:
            summary_writer.write(temp_file)

        current_size = await aiofiles.os.stat(summary_file_path)
        if current_size.st_size > 19 * 1024 * 1024:  # 19MB guardrail
            logging.info(f"Summary file {summary_file_path} reached the size limit.")
            return False
        return True

    except Exception as e:
        logging.error(f"Error adding to summary: {e}")
        return False

# -------------------- main entry --------------------
async def process(headers, increaseinfo, accountid):
    """Main orchestration: generate N1s, roll summaries, upload to leases & tasks."""
    countall = 0
    categoryid = None
    datelabel = None
    initialized_category = False

    try:
        async with aiohttp.ClientSession() as session:
            # increaseinfo is a list of {"building_id": { ... }} dicts
            for buildingdata in increaseinfo:
                for buildingid, data in buildingdata.items():
                    # Skip if no leases at all
                    if not data.get('lease_info'):
                        continue

                    # Extract building name (from first lease row)
                    buildingname = _safe_get(data['lease_info'][0], ['buildingname'], f"Building {buildingid}")

                    # Determine if at least one lease needs a notice
                    has_active = any(not _is_ignored(lease.get('ignored')) for lease in data['lease_info'])

                    # This block (date label + category) should run once for the overall run
                    if not initialized_category:
                        first_lease_to = _safe_get(data['lease_info'][0], ['renewal', 'LeaseToDate'])
                        if first_lease_to:
                            datelabel = datetime.strptime(first_lease_to, "%Y-%m-%d").strftime("%B %d, %Y")
                        else:
                            datelabel = datetime.utcnow().strftime("%B %d, %Y")
                        categoryid = await category(headers, session, datelabel)
                        initialized_category = True

                    # Only create a task for this building if there is at least one non-ignored lease
                    taskid = None
                    if has_active and data.get('ignorebuilding') != "Y":
                        taskid = await createtask(headers, buildingid, session, datelabel)

                    # Fresh summary state per building
                    summary_writer = PdfWriter()
                    summary_index = 1
                    summary_data = []
                    countbuilding = 0

                    # Per-lease processing
                    total_leases = len(data['lease_info'])
                    for i, lease in enumerate(data['lease_info'], 1):
                        leaseid = lease['leaseid']
                        logging.info(f"[{buildingid}] Lease {i}/{total_leases} → id={leaseid}, ignored={lease.get('ignored')!r}")

                        if not _is_ignored(lease.get('ignored')):
                            leaseincreaseinfo = lease['increasenotice']
                            leaserenewalinfo = lease['renewal']

                            # Generate individual N1
                            filepath = await generateN1files(leaseid, leaseincreaseinfo)
                            if not filepath:
                                logging.error(f"Failed N1 generation for {leaseid}")
                                continue
                        
                            # Always add to in-memory summary PDF
                            summary_file_path = os.path.join('/tmp', f"Notices for {buildingname} {datelabel} Part {summary_index}.pdf")
                            ok = await addtosummary(summary_file_path, filepath, summary_writer)
                            if not ok:
                                # Rollover to a new Part
                                summary_writer = PdfWriter()
                                summary_index += 1
                                summary_file_path = os.path.join('/tmp', f"Notices for {buildingname} {datelabel} Part {summary_index}.pdf")
                                ok = await addtosummary(summary_file_path, filepath, summary_writer)

                            # Upload individual N1 to the lease
                            if categoryid is None:
                                logging.error("No category id available for lease uploads.")
                            else:
                                confirmlease = await uploadN1filestolease(headers, filepath, leaseid, session, categoryid)
                                if confirmlease and ok:
                                    try:
                                        await aiofiles.os.remove(filepath)
                                    except Exception as e:
                                        logging.error(f"Cleanup failed for lease file {filepath}: {e}")

                            countall += 1
                            countbuilding += 1
                            summary_data.append(lease)

                            # Lease renewals disabled; skipping any lease modifications
                            # await leaserenewals(headers, leaseid, leaserenewalinfo, session)

                        else:
                            # Lease extension disabled for ignored leases
                            # await leaserenewalingored(headers, leaseid, lease, session)
                            logging.info(f"[{buildingid}] Skipped lease renewal for ignored lease {leaseid}.")

                    # Finalize & upload building summary (if any non-ignored leases and not ignoring building)
                    if summary_data and data.get('ignorebuilding') != "Y" and taskid:
                        summary_file_path = os.path.join('/tmp', f"Notices for {buildingname} {datelabel} Part {summary_index}.pdf")

                        await add_summary_page(summary_data, summary_writer, summary_file_path, buildingname, countbuilding, datelabel)

                        ok_summary = await uploadsummarytotask(headers, summary_file_path, taskid, session, categoryid)
                        if ok_summary:
                            logging.info(f"Summary for {buildingname} uploaded to task.")
                        else:
                            logging.error(f"Summary upload failed for {buildingname}.")
                    else:
                        if data.get('ignorebuilding') == "Y":
                            logging.info(f"Skipping PDF summary & task for building {buildingid} (ignorebuilding=Y)")
                        elif not summary_data:
                            logging.info(f"No non-ignored leases for building {buildingid}; no summary uploaded.")

    except Exception as e:
        logging.error(f"Error processing leases data: {e}")

    print(countall)
