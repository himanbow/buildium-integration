import aiohttp
import os
from io import BytesIO
import logging
import json
from datetime import datetime, UTC
from tempfile import NamedTemporaryFile
from typing import Optional
from pathlib import Path

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
    logging.info("Updating Task")
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
            logging.info("Task Updated")
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
    logging.info("Starting File Upload")
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
    logging.info("File Presign Completed")
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

def split_pdf_bytes(pdf_bytes: bytes, max_bytes: int = 15 * 1024 * 1024):
    """
    Split a PDF (as bytes) into multiple PDFs, each <= max_bytes, on page boundaries.
    Returns list[bytes]; each element is a complete PDF file.
    """
    try:
        from pypdf import PdfReader, PdfWriter
    except Exception:
        from PyPDF2 import PdfReader, PdfWriter  # type: ignore

    reader = PdfReader(BytesIO(pdf_bytes))
    n = len(reader.pages)
    parts: list[bytes] = []

    i = 0
    while i < n:
        writer = PdfWriter()
        # (Optional) carry metadata
        try:
            if getattr(reader, "metadata", None):
                writer.add_metadata(reader.metadata)
        except Exception:
            pass

        last_good = None
        last_end = None

        j = i
        while j < n:
            writer.add_page(reader.pages[j])
            buf = BytesIO()
            writer.write(buf)
            size_now = buf.tell()
            if size_now <= max_bytes:
                last_good = buf.getvalue()
                last_end = j + 1
                j += 1
            else:
                break

        if last_good is not None:
            parts.append(last_good)
            i = last_end
        else:
            # Single page > limit: emit it alone anyway
            writer = PdfWriter()
            writer.add_page(reader.pages[i])
            buf = BytesIO()
            writer.write(buf)
            parts.append(buf.getvalue())
            i += 1

    return parts

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
        with NamedTemporaryFile(delete=False, suffix=".pdf") as tmp_pdf:
            pdf_path = tmp_pdf.name
            logging.info(f"File Name: {pdf_path}.")

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
            try:
                os.remove(pdf_path)
            except Exception:
                pass
        
        # Split to <=15MB parts
        parts = split_pdf_bytes(pdf_bytes, max_bytes=15 * 1024 * 1024)
        
        # Name parts predictably (foo.pdf -> foo_part01.pdf, etc.; single-part keeps original)
        orig_name = Path(pdf_filename).name  # whatever you were using (e.g., "Increase Report.pdf")
        stem = Path(orig_name).stem
        suffix = Path(orig_name).suffix or ".pdf"
        
        if len(parts) == 1:
            part_names_and_bytes = [(orig_name, parts[0])]
        else:
            part_names_and_bytes = [
                (f"{stem}_part{idx+1:02d}{suffix}", b) for idx, b in enumerate(parts)
            ]


        # Normalize provided JSON to bytes (guaranteed provided)
        if isinstance(buildingjsonfile, (bytes, bytearray)):
            json_bytes = bytes(buildingjsonfile)
        elif isinstance(buildingjsonfile, str):
            json_bytes = buildingjsonfile.encode("utf-8")
        else:
            json_bytes = json.dumps(buildingjsonfile, default=str, indent=2).encode("utf-8")

        async with aiohttp.ClientSession() as session:
            # 1) Create a new history entry with the message
            msg = f'Please review "{pdf_filename}" for the increase notices. Should you required any changes, complete the changes, delete this task, and create a new task entilted "Increase Notices" with the task category set to "System Tasks'
            ok_put = await _put_task_message(session, task_id, headers, title, assigned_to_user_id, taskcatid, msg)
            if not ok_put:
                return False

            # 2) Lock the HISTORY ID *now* (don’t fetch “latest” again after uploads)
            history_id = await _get_latest_history_id(session, task_id, headers)
            if not history_id:
                logging.error("Could not find latest history entry to attach files.")
                return False

            # 3) Upload every part to the SAME history_id
            for part_filename, part_bytes in part_names_and_bytes:
                ok_pdf = await _upload_file_for_history(
                    session, task_id, history_id, headers,
                    part_filename, part_bytes, content_type="application/pdf"
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
        logging.exception(f"Error updating task: {e} for increase notices")
        return False
