import aiohttp
import os
import logging
import json
from datetime import datetime, UTC
from tempfile import NamedTemporaryFile
from typing import Optional

from build_prelim_increase_report import build_increase_report_pdf


def _flatten_rows_from_summary(increase_summary: dict) -> list[dict]:
    rows = []
    for b_id, data in (increase_summary or {}).items():
        for inc in data.get("increases", []):
            if not inc.get("buildingname"):
                inc["buildingname"] = f"Building {b_id}"
            rows.append(inc)
    return rows

async def _put_task_message(session: aiohttp.ClientSession, task_id: int, headers: dict,
                            title: str, assigned_to_user_id: int, taskcatid: int, msg: str) -> bool:
    url_task = f"https://api.buildium.com/v1/tasks/todorequests/{task_id}"
    payload = {
        "Title": title,
        "AssignedToUserId": assigned_to_user_id,
        "Priority": "High",
        "CategoryId": taskcatid,
        "TaskStatus": "InProgress",
        "TaskId": task_id,
        "Message": msg,
        "Date": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
    }
    async with session.put(url_task, json=payload, headers=headers) as r:
        if r.status == 200:
            return True
        logging.error(f"Task PUT failed: {r.status} {await r.text()}")
        return False

async def _get_latest_history_id(session: aiohttp.ClientSession, task_id: int, headers: dict) -> Optional[int]:
    url_hist = f"https://api.buildium.com/v1/tasks/{task_id}/history"
    async with session.get(url_hist, headers=headers) as r_hist:
        if r_hist.status != 200:
            logging.error(f"History GET failed: {r_hist.status} {await r_hist.text()}")
            return None
        hist = await r_hist.json()
        if not hist:
            return None
        # If API returns newest-first, hist[0] is fine; otherwise sort by CreatedDate/Date
        try:
            hist.sort(key=lambda h: h.get("Date") or h.get("CreatedDate") or "", reverse=True)
        except Exception:
            pass
        return hist[0].get("Id")

async def _upload_file_for_history(session: aiohttp.ClientSession, task_id: int, history_id: int,
                                   headers: dict, filename: str, file_bytes: bytes,
                                   content_type: str) -> bool:
    """
    1) POST /tasks/{taskId}/history/{historyId}/files/uploadrequests
    2) POST to BucketUrl with multipart:
       - include ALL fields from FormData in a stable order
       - add 'file' field LAST
    """
    # Step 1: presign
    url_presign = f"https://api.buildium.com/v1/tasks/{task_id}/history/{history_id}/files/uploadrequests"
    async with session.post(url_presign, json={"FileName": filename}, headers=headers) as r_pre:
        if r_pre.status != 201:
            logging.error(f"Upload request failed: {r_pre.status} {await r_pre.text()}")
            return False
        pre = await r_pre.json()

    form = pre.get("FormData", {})
    bucket_url = pre.get("BucketUrl")
    if not bucket_url or not form:
        logging.error("Upload request missing BucketUrl or FormData.")
        return False

    # Maintain expected field order; ensure file last
    ordered_keys = [
        "Key", "ACL", "Policy", "Content-Type", "Content-Disposition",
        "X-Amz-Algorithm", "X-Amz-Credential", "X-Amz-Date", "X-Amz-Signature",
        "X-Amz-Meta-Buildium-Entity-Type", "X-Amz-Meta-Buildium-Entity-Id",
        "X-Amz-Meta-Buildium-File-Source", "X-Amz-Meta-Buildium-File-Description",
        "X-Amz-Meta-Buildium-Account-Id", "X-Amz-Meta-Buildium-File-Name",
        "X-Amz-Meta-Buildium-File-Title", "X-Amz-Meta-Buildium-Child-Entity-Id",
        "X-Amz-Meta-Buildium-Finalize-Upload-Message-Version",
    ]
    form_data = aiohttp.FormData()
    for k in ordered_keys:
        if k in form:
            form_data.add_field(k, form[k])
    for k, v in form.items():
        if k not in ordered_keys:
            form_data.add_field(k, v)

    form_data.add_field("file", file_bytes, filename=filename, content_type=content_type)

    # Step 2: upload to S3 BucketUrl
    async with session.post(bucket_url, data=form_data) as upload_response:
        if upload_response.status in (204, 200, 201):
            logging.info(f"Upload successful: {filename}")
            return True
        logging.error(f"Error Uploading File {filename}: {upload_response.status} {await upload_response.text()}")
        return False

async def update_task(
    task_data: dict,
    increase_summary: dict,
    increase_effective_date,
    percentage: float | str,
    headers: dict,
    account_id: int | str,
    buildingjsonfile,                  # required
    logo_source: str | None = None,
) -> bool:
    try:
        task_id = task_data["Id"]
        taskcatid = task_data["Category"]["Id"]
        assigned_to_user_id = task_data["AssignedToUserId"]
        title = f"Increase Notices for {increase_effective_date.strftime('%B %d, %Y')} - Review"

        rows = _flatten_rows_from_summary(increase_summary)
        if not rows:
            logging.error("No rows to include in the PDF; aborting.")
            return False

        run_date = datetime.now(UTC).strftime("%Y-%m-%d")
        eff_str = increase_effective_date.strftime("%B %d, %Y")
        pdf_filename  = f"Increase Review Report {increase_effective_date.strftime('%B %d, %Y')}.pdf"
        json_filename = "data.json"

        # Generate PDF
        from tempfile import NamedTemporaryFile
        with NamedTemporaryFile(delete=False, suffix=".pdf") as tmp_pdf:
            pdf_path = tmp_pdf.name
        try:
            build_increase_report_pdf(
                pdf_path,
                run_date=run_date,
                effective_date=eff_str,
                guideline_pct=str(percentage),
                rows=rows,
                logo_source=logo_source,
            )
            with open(pdf_path, "rb") as f:
                pdf_bytes = f.read()
        finally:
            try: os.remove(pdf_path)
            except Exception: pass

        # Normalize provided JSON to bytes (guaranteed provided)
        if isinstance(buildingjsonfile, (bytes, bytearray)):
            json_bytes = bytes(buildingjsonfile)
        elif isinstance(buildingjsonfile, str):
            json_bytes = buildingjsonfile.encode("utf-8")
        else:
            json_bytes = json.dumps(buildingjsonfile, default=str, indent=2).encode("utf-8")

        async with aiohttp.ClientSession() as session:
            # 1) Create a new history entry with the message
            msg = f'Please review "{pdf_filename}" for the increase notices. A data snapshot "{json_filename}" is also attached.'
            ok_put = await _put_task_message(session, task_id, headers, title, assigned_to_user_id, taskcatid, msg)
            if not ok_put:
                return False

            # 2) Lock the HISTORY ID *now* (don’t fetch “latest” again after uploads)
            history_id = await _get_latest_history_id(session, task_id, headers)
            if not history_id:
                logging.error("Could not find latest history entry to attach files.")
                return False

            # 3) Upload BOTH files to the SAME locked history_id
            ok_pdf = await _upload_file_for_history(
                session, task_id, history_id, headers,
                pdf_filename, pdf_bytes, content_type="application/pdf"
            )
            if not ok_pdf:
                return False

            ok_json = await _upload_file_for_history(
                session, task_id, history_id, headers,
                json_filename, json_bytes, content_type="application/json"
            )
            if not ok_json:
                logging.error("JSON upload failed.")
                return False

            return True

    except Exception as e:
        logging.exception(f"Error updating task: {e}")
        return False
