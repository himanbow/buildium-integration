import aiohttp
import logging
import asyncio
import os
import io
import random
import generateN1notice
from PyPDF2 import PdfReader, PdfWriter
import aiofiles
import aiofiles.os
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

from rate_limiter import semaphore, throttle

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
        async with semaphore, throttle:
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

# -------------------- POST with retry --------------------
async def post_with_retry(session, url, *, headers=None, json=None, data=None, max_attempts: int = 5):
    """POST with basic 429 retry, exponential backoff, and jitter."""
    delay = 2
    for attempt in range(max_attempts):
        async with semaphore, throttle:
            async with session.post(url, headers=headers, json=json, data=data) as response:
                status = response.status
                retry_after_hdr = response.headers.get("Retry-After")
                if status != 429:
                    try:
                        body = await response.json()
                    except Exception:
                        body = await response.text()
                    return status, body
        try:
            wait = float(retry_after_hdr) if retry_after_hdr is not None else delay
        except ValueError:
            wait = delay
        await asyncio.sleep(wait + random.uniform(0, 0.5))
        delay *= 2
    return None, None

# -------------------- categories --------------------
async def category(headers, session, date_label: str):
    """Find or create a Files Category named 'Increases {date_label}' and return its Id."""
    url = "https://api.buildium.com/v1/files/categories"
    params = {'limit': 1000}
    async with semaphore, throttle:
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
                async with semaphore, throttle:
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

# -------------------- PDF generation per lease --------------------
async def generateN1files(leaseid, leasedata):
    """Generate an N1 notice for a lease and return (filename, pdf_bytes)."""
    logging.info(f"Generating Increase Notices for lease {leaseid}")
    filename, pdf_bytes = await generateN1notice.create(leaseid, leasedata)
    return filename, pdf_bytes

# -------------------- upload to Lease --------------------
async def uploadN1filestolease(headers, filename, file_bytes, leaseid, session, categoryid):
    """Upload an in-memory N1 PDF to the given lease."""
    logging.info(f"Uploading N1 File to Lease {leaseid}")

    try:
        # 1) Get presign
        url = "https://api.buildium.com/v1/files/uploadrequests"
        presign_body = {
            "EntityType": "Lease",
            "EntityId": leaseid,
            "FileName": filename,  # must be just the name
            "Title": filename,
            "CategoryId": categoryid,
        }
        status, body = await post_with_retry(
            session, url, headers=headers, json=presign_body
        )
        if status != 201:
            logging.info(
                f"Error while submitting lease file metadata: {status} {body}"
            )
            return False

        payload = body
        form_data, bucket_url = await amazondatalease(payload)

        # 2) Add PDF bytes as LAST field
        form_data.add_field(
            "file", file_bytes, filename=filename, content_type="application/pdf"
        )

        # 3) Upload to S3
        async with semaphore, throttle:
            async with session.post(bucket_url, data=form_data) as upload_response:
                resp_text = await upload_response.text()
                if upload_response.status == 204:
                    logging.info(f"Upload of Notice for {leaseid} successful.")
                    return True
                if upload_response.status == 403 and "Invalid according to Policy: Policy expired" in resp_text:
                    logging.warning(
                        f"Policy expired for lease {leaseid} upload; requesting new presign and retrying."
                    )
                    # Re-request presigned data
                    status, body = await post_with_retry(
                        session, url, headers=headers, json=presign_body
                    )
                    if status != 201:
                        logging.info(
                            f"Retry presign failed for lease {leaseid}: {status} {body}"
                        )
                        return False
                    payload = body
                    form_data_retry, bucket_url_retry = await amazondatalease(payload)
                    form_data_retry.add_field(
                        "file", file_bytes, filename=filename, content_type="application/pdf"
                    )
                    async with semaphore, throttle:
                        async with session.post(bucket_url_retry, data=form_data_retry) as retry_response:
                            retry_body = await retry_response.text()
                            if retry_response.status == 204:
                                logging.info(
                                    f"Retry upload of Notice for {leaseid} succeeded."
                                )
                                return True
                            logging.info(
                                f"Retry upload failed for {leaseid}: {retry_response.status} {retry_body}"
                            )
                            return False
                logging.info(
                    f"Error Uploading Notice for {leaseid}: {upload_response.status} {resp_text}"
                )
                return False

    except Exception as e:
        logging.info(
            f"An error occurred uploading N1 for lease {leaseid}: {str(e)}"
        )
        return False

# -------------------- upload summary to Task --------------------
async def uploadsummarytotask(headers, filename, file_bytes, taskid, session, categoryid):
    """Upload an in-memory summary PDF to the given task."""
    logging.info(f"Uploading Summary to Task {taskid}")

    try:
        # 1) Get latest task history id (newest first)
        urltaskhistory = f"https://api.buildium.com/v1/tasks/{taskid}/history"
        async with semaphore, throttle:
            async with session.get(urltaskhistory, headers=headers) as response:
                if response.status != 200:
                    logging.info(
                        f"Error while getting task history: {response.status} {await response.text()}"
                    )
                    return False

                taskhistorydata = await response.json()
            try:
                taskhistorydata.sort(
                    key=lambda h: h.get("Date") or h.get("CreatedDate") or "",
                    reverse=True,
                )
            except Exception:
                pass
            if not taskhistorydata:
                logging.info("No task history entries found to attach file to.")
                return False
            taskhistoryid = taskhistorydata[0]["Id"]

        # 2) Presign for this history entry
        url = (
            f"https://api.buildium.com/v1/tasks/{taskid}/history/{taskhistoryid}/files/uploadrequests"
        )
        presign_body = {"FileName": filename}
        status, body = await post_with_retry(
            session, url, headers=headers, json=presign_body
        )
        if status != 201:
            logging.info(
                f"Error while submitting task file metadata: {status} {body}"
            )
            return False

        payload = body
        form_data, bucket_url = await amazondatatask(payload)

        # 3) Add PDF bytes LAST
        form_data.add_field(
            "file", file_bytes, filename=filename, content_type="application/pdf"
        )

        # 4) Upload to S3
        async with semaphore, throttle:
            async with session.post(bucket_url, data=form_data) as upload_response:
                resp_text = await upload_response.text()
                if upload_response.status == 204:
                    logging.info(f"Upload successful for Task {taskid}.")
                    return True
                if upload_response.status == 403 and "Invalid according to Policy: Policy expired" in resp_text:
                    logging.warning(
                        f"Policy expired for task {taskid} upload; requesting new presign and retrying."
                    )
                    status, body = await post_with_retry(
                        session, url, headers=headers, json=presign_body
                    )
                    if status != 201:
                        logging.info(
                            f"Retry presign failed for task {taskid}: {status} {body}"
                        )
                        return False
                    payload = body
                    form_data_retry, bucket_url_retry = await amazondatatask(payload)
                    form_data_retry.add_field(
                        "file", file_bytes, filename=filename, content_type="application/pdf"
                    )
                    async with semaphore, throttle:
                        async with session.post(bucket_url_retry, data=form_data_retry) as retry_response:
                            retry_body = await retry_response.text()
                            if retry_response.status == 204:
                                logging.info(
                                    f"Retry upload of Summary for task {taskid} succeeded."
                                )
                                return True
                            logging.info(
                                f"Retry upload failed for task {taskid}: {retry_response.status} {retry_body}"
                            )
                            return False
                logging.info(
                    f"Error Uploading File for Task {taskid}: {upload_response.status} {resp_text}"
                )
                return False

    except Exception as e:
        logging.error(f"Error Uploading Summary to task: {e}")
        return False

# -------------------- ignored renewal helper --------------------
async def leaserenewalingored(headers, leaseid, lease, session):
    """When a lease is ignored for increases, extend LeaseToDate by +6 months."""
    try:
        url = f"https://api.buildium.com/v1/leases/{leaseid}"
        async with semaphore, throttle:
            async with session.get(url, headers=headers) as response:
                data = await response.json()
                date = datetime.strptime(data["LeaseToDate"], "%Y-%m-%d")
                date = (date + relativedelta(months=6)).strftime("%Y-%m-%d")

        payload = {
            "LeaseType": data["LeaseType"],
            "UnitId": data['UnitId'],
            "LeaseFromDate": data['LeaseFromDate'],
            "LeaseToDate": date,
            "IsEvictionPending": data['IsEvictionPending'],
        }
        async with semaphore, throttle:
            async with session.put(url, json=payload, headers=headers) as response:
                if response.status == 200:
                    logging.info(f"Extension Completed for {leaseid}.")
                else:
                    logging.error(f"Error extending {leaseid}: {response.status} {await response.text()}")
    except Exception as e:
        logging.error(f"Error in leaserenewalingored for {leaseid}: {e}")

# -------------------- eviction toggle --------------------
async def setevictionstatus(leaseid, eviction: bool, session, headers) -> bool:
    """Set IsEvictionPending to the supplied boolean; return True/False for success."""
    try:
        url = f"https://api.buildium.com/v1/leases/{leaseid}"
        async with semaphore, throttle:
            async with session.get(url, headers=headers) as response:
                if response.status != 200:
                    logging.error(f"GET lease {leaseid} failed: {response.status} {await response.text()}")
                    return False
                data = await response.json()

        payload = {
            "LeaseType": data["LeaseType"],
            "UnitId": data['UnitId'],
            "LeaseFromDate": data['LeaseFromDate'],
            "LeaseToDate": data['LeaseToDate'],
            "IsEvictionPending": eviction,
            "AutomaticallyMoveOutTenants": False
        }
        async with semaphore, throttle:
            async with session.put(url, json=payload, headers=headers) as response:
                if response.status == 200:
                    logging.info(f"Eviction flag set to {eviction} for lease {leaseid}.")
                    return True
                else:
                    logging.error(f"Error setting eviction for {leaseid}: {response.status} {await response.text()}")
                    return False
    except Exception as e:
        logging.error(f"Exception in setevictionstatus for {leaseid}: {e}")
        return False

# -------------------- non-recursive lease renewals with retries --------------------
async def leaserenewals(headers, leaseid, lease, session, max_retries: int = 3):
    logging.info(f"Processing Lease Renewal for Lease {leaseid}")

    url = f"https://api.buildium.com/v1/leases/{leaseid}/renewals"

    try:
        # Compute next LeaseToDate (one year minus a day)
        end = datetime.strptime(lease["LeaseToDate"], "%Y-%m-%d")
        new_to_date = (end + relativedelta(years=1) - timedelta(days=1)).strftime("%Y-%m-%d")

        if lease.get("RecurringChargesToStop") is None:
            payloadstr = {
                "LeaseType": lease["LeaseType"],
                "LeaseToDate": new_to_date,
                "Rent": lease["Rent"],
                "TenantIds": lease["TenantIds"],
                "SendWelcomeEmail": "false",
            }
        else:
            recurring_charges_list = [int(charge.strip()) for charge in lease["RecurringChargesToStop"].split(',')]
            payloadstr = {
                "LeaseType": lease["LeaseType"],
                "LeaseToDate": new_to_date,
                "Rent": lease["Rent"],
                "TenantIds": lease["TenantIds"],
                "SendWelcomeEmail": "false",
                "RecurringChargesToStop": recurring_charges_list
            }

        # Attempts loop (no recursion)
        for attempt in range(1, max_retries + 1):
            async with semaphore, throttle:
                async with session.post(url, json=payloadstr, headers=headers) as response:
                    if response.status == 201:
                        logging.info(f"Renewal Completed for {leaseid}.")
                        return True
                    if response.status == 409:
                        logging.warning(
                            f"Lease {leaseid} renewal 409 (attempt {attempt}/{max_retries}); toggling eviction and retrying..."
                        )
                        set_ok = await setevictionstatus(leaseid, True, session, headers)
                        if not set_ok:
                            logging.error(
                                f"Failed to set eviction flag for {leaseid}; aborting renewal."
                            )
                            return False
                        # Retry immediately with eviction set
                        async with semaphore, throttle:
                            async with session.post(url, json=payloadstr, headers=headers) as r2:
                                if r2.status == 201:
                                    logging.info(
                                        f"Renewal Completed for {leaseid} after eviction toggle."
                                    )
                                    await setevictionstatus(leaseid, False, session, headers)
                                    return True
                                else:
                                    await setevictionstatus(leaseid, False, session, headers)
                                    logging.error(
                                        f"Retry after eviction toggle failed for {leaseid}: {r2.status} {await r2.text()}"
                                    )
                    else:
                        logging.error(
                            f"Error renewing {leaseid}: {response.status} {await response.text()}"
                        )
                        return False

            # backoff before next attempt
            await asyncio.sleep(0.5 * attempt)

        logging.error(f"Lease {leaseid} renewal failed after {max_retries} attempts.")
        return False

    except Exception as e:
        logging.error(f"Exception in leaserenewals for {leaseid}: {e}")
        return False

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
        async with semaphore, throttle:
            async with session.get(url_rental, headers=headers) as response:
                if response.status != 200:
                    logging.error(f"Failed to fetch building data. Status: {response.status}")
                    return None
                rental = await response.json()
                userid = _safe_get(rental, ['RentalManager', 'Id'])
                userid = 352081
                buildingname = rental.get('Name')

        # Find/create task category 'Increase Notices'
        async with semaphore, throttle:
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
                    async with semaphore, throttle:
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
        async with semaphore, throttle:
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
async def add_summary_page(summary_data, summary_writer, buildingname, countbuilding, date_label):
    """Generate summary page(s) and prepend them to existing lease pages.

    The incoming ``summary_writer`` already holds the individual lease PDFs. This
    function creates the distribution list page(s) and returns a combined PDF
    where those pages appear *before* the lease documents.
    """
    try:
        # Generate the summary page(s)
        summary_page_buffer = await generateN1notice.create_summary_page(
            summary_data, buildingname, countbuilding, date_label
        )
        summary_pdf = PdfReader(summary_page_buffer)

        # Build a new writer that starts with the summary page(s)
        combined_writer = PdfWriter()
        for page in summary_pdf.pages:
            combined_writer.add_page(page)

        # Append all existing lease pages afterwards
        for page in summary_writer.pages:
            combined_writer.add_page(page)

        buffer = io.BytesIO()
        combined_writer.write(buffer)
        buffer.seek(0)
        return buffer.getvalue()
    except Exception as e:
        logging.error(f"Error adding summary pages: {e}")
        return b""

async def addtosummary(file_bytes, summary_writer):
    """Append a lease PDF's pages into the in-memory summary."""
    try:
        reader = PdfReader(io.BytesIO(file_bytes))
        for page in reader.pages:
            summary_writer.add_page(page)
        return True
    except Exception as e:
        logging.error(f"Error adding to summary: {e}")
        return False


# -------------------- per-building processing --------------------
async def process_building(
    buildingid,
    data,
    headers,
    session,
    datelabel,
    categoryid,
    counter,
    count_lock,
):
    """Handle all leases for a single building and update global counters."""

    # Skip if no leases at all
    if not data.get("lease_info"):
        return

    # Extract building name (from first lease row)
    buildingname = _safe_get(
        data["lease_info"][0], ["buildingname"], f"Building {buildingid}"
    )

    # Determine if at least one lease needs a notice
    has_active = any(
        not _is_ignored(lease.get("ignored")) for lease in data["lease_info"]
    )

    # Only create a task for this building if there is at least one non-ignored lease
    taskid = None
    if has_active and data.get("ignorebuilding") != "Y":
        taskid = await createtask(headers, buildingid, session, datelabel)

    # Fresh summary state per building
    summary_writer = PdfWriter()
    summary_index = 1
    summary_data = []
    countbuilding = 0
    current_summary_size = 0
    summary_parts = []  # list of (filename, bytes)
    had_split = False
    size_limit = 15 * 1024 * 1024  # ~15MB

    # Per-lease processing -- concurrently generate & upload
    total_leases = len(data["lease_info"])

    async def handle_lease(i, lease):
        leaseid = lease["leaseid"]
        logging.info(
            f"[{buildingid}] Lease {i}/{total_leases} → id={leaseid}, ignored={lease.get('ignored')!r}"
        )

        if _is_ignored(lease.get("ignored")):
            await leaserenewalingored(headers, leaseid, lease, session)
            logging.info(
                f"[{buildingid}] Processed Ignored Lease Renewal for lease {leaseid}."
            )
            return None

        leaseincreaseinfo = lease["increasenotice"]

        # Generate individual N1
        filename, file_bytes = await generateN1files(leaseid, leaseincreaseinfo)
        if not file_bytes:
            logging.error(f"Failed N1 generation for {leaseid}")
            return None

        # Upload individual N1 to the lease
        confirmlease = False
        if categoryid is None:
            logging.error("No category id available for lease uploads.")
        else:
            confirmlease = await uploadN1filestolease(
                headers, filename, file_bytes, leaseid, session, categoryid
            )
        await leaserenewals(headers, leaseid, lease["renewal"], session)

        return lease, file_bytes, confirmlease

    tasks = [handle_lease(i, lease) for i, lease in enumerate(data["lease_info"], 1)]
    results = await asyncio.gather(*tasks)

    # Integrate results sequentially for summary creation
    for res in results:
        if not res:
            continue

        lease, file_bytes, confirmlease = res

        # Determine size of this lease PDF
        lease_size = len(file_bytes or b"")

        # If adding this lease would exceed the limit, flush current summary to disk
        if (
            current_summary_size + lease_size > size_limit
            and len(summary_writer.pages) > 0
        ):
            summary_buffer = io.BytesIO()
            summary_writer.write(summary_buffer)
            had_split = True
            part_filename = f"Part {summary_index}.pdf"
            part_bytes = summary_buffer.getvalue()
            part_path = os.path.join("/tmp", part_filename)
            async with aiofiles.open(part_path, "wb") as f:
                await f.write(part_bytes)
            if await aiofiles.os.path.exists(part_path):
                summary_parts.append((part_filename, part_bytes))
            else:
                logging.error(
                    f"Failed to verify summary part {part_filename} at {part_path}"
                )
            summary_index += 1
            summary_writer = PdfWriter()
            current_summary_size = 0

        ok = await addtosummary(file_bytes, summary_writer)

        if ok:
            current_summary_size += lease_size
            async with count_lock:
                counter["countall"] += 1
            countbuilding += 1
            summary_data.append(lease)

    # Finalize & upload building summary (if any non-ignored leases and not ignoring building)
    if summary_data and data.get("ignorebuilding") != "Y" and taskid:
        if had_split:
            part_filename = f"Part {summary_index}.pdf"
        else:
            part_filename = f"Notices for {buildingname} {datelabel}.pdf"

        summary_bytes = await add_summary_page(
            summary_data,
            summary_writer,
            buildingname,
            countbuilding,
            datelabel,
        )

        part_path = os.path.join("/tmp", part_filename)
        async with aiofiles.open(part_path, "wb") as f:
            await f.write(summary_bytes)
        if await aiofiles.os.path.exists(part_path):
            summary_parts.append((part_filename, summary_bytes))
        else:
            logging.error(
                f"Failed to verify summary file {part_filename} at {part_path}"
            )

        total_parts = len(summary_parts)
        logging.info(
            f"[{buildingid}] Prepared {total_parts} summary part(s) for upload"
        )

        for idx, (fname, bytes_data) in enumerate(summary_parts, 1):
            for attempt in range(1, 3):
                ok_summary = await uploadsummarytotask(
                    headers, fname, bytes_data, taskid, session, categoryid
                )
                if ok_summary:
                    logging.info(
                        f"[{buildingid}] Uploaded summary part {idx}/{total_parts}: {fname} (attempt {attempt})"
                    )
                    break
                if attempt < 2:
                    logging.warning(
                        f"[{buildingid}] Upload failed for part {idx}/{total_parts}: {fname}; retrying..."
                    )
                    await asyncio.sleep(1)
                else:
                    logging.error(
                        f"[{buildingid}] Summary upload failed for part {idx}/{total_parts}: {fname}"
                    )
    else:
        if data.get("ignorebuilding") == "Y":
            logging.info(
                f"Skipping PDF summary & task for building {buildingid} (ignorebuilding=Y)"
            )
        elif not summary_data:
            logging.info(
                f"No non-ignored leases for building {buildingid}; no summary uploaded."
            )

# -------------------- main entry --------------------
async def process(session, headers, increaseinfo, accountid):
    """Main orchestration: generate N1s, roll summaries, upload to leases & tasks."""
    counter = {"countall": 0}
    categoryid = None
    datelabel = None
    count_lock = asyncio.Lock()

    try:
        # Determine date label & category once based on the first available lease
        for buildingdata in increaseinfo:
            for _, data in buildingdata.items():
                if data.get("lease_info"):
                    first_lease_to = _safe_get(
                        data["lease_info"][0], ["renewal", "LeaseToDate"]
                    )
                    if first_lease_to:
                        datelabel = datetime.strptime(
                            first_lease_to, "%Y-%m-%d"
                        ).strftime("%B %d, %Y")
                    else:
                        datelabel = datetime.utcnow().strftime("%B %d, %Y")
                    categoryid = await category(headers, session, datelabel)
                    break
            if datelabel:
                break

        # Process buildings sequentially to avoid overwhelming the
        # Buildium API with task creation bursts.  The per-request
        # rate limiters still apply, but spacing out buildings helps
        # keep task creation smooth.
        for buildingdata in increaseinfo:
            for buildingid, data in buildingdata.items():
                if not data.get("lease_info"):
                    continue

                await process_building(
                    buildingid,
                    data,
                    headers,
                    session,
                    datelabel,
                    categoryid,
                    counter,
                    count_lock,
                )

                # Small pause between buildings to further throttle task creation
                await asyncio.sleep(2)
    except Exception as e:
        logging.error(f"Error processing leases data: {e}")

    print(counter["countall"])
