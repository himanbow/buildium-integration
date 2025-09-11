import aiohttp
import os
from io import BytesIO
import logging
import json
from datetime import datetime, UTC
from tempfile import NamedTemporaryFile
from typing import Optional
from pathlib import Path
import signal

from build_prelim_increase_report import build_increase_report_pdf

# -----------------------------------------------------------------------------
# Logging (won't override if you've already configured handlers elsewhere)
# -----------------------------------------------------------------------------
if not logging.getLogger().handlers:
    logging.basicConfig(level=logging.INFO)

# -----------------------------------------------------------------------------
# API bases
#   - To-Do Requests live under /v1/tasks/todorequests
#   - Task history & file uploads live under /v1/tasks
#     (per Buildium OpenAPI; upload presign is:
#      POST /v1/tasks/{taskId}/history/{taskHistoryId}/files/uploadrequests)
# -----------------------------------------------------------------------------
BASE_API = "https://api.buildium.com/v1"
TODO_RESOURCE = "tasks/todorequests"
TASKS_RESOURCE = "tasks"
HTTP_TIMEOUT = aiohttp.ClientTimeout(
    total=120,        # whole request
    connect=15,       # DNS + TCP connect
    sock_connect=15,  # TCP handshake
    sock_read=90,     # server processing / body read
)

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------


def _flatten_rows_from_summary(increase_summary: dict) -> list[dict]:
    rows = []
    for b_id, data in (increase_summary or {}).items():
        for inc in data.get("increases", []):
            if not inc.get("buildingname"):
                inc["buildingname"] = f"Building {b_id}"
            rows.append(inc)
    return rows


def _parse_iso(dt_str: Optional[str]) -> datetime:
    if not dt_str:
        return datetime.min.replace(tzinfo=UTC)
    try:
        return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    except Exception:
        return datetime.min.replace(tzinfo=UTC)


async def _put_task_message(session: aiohttp.ClientSession, task_id: int, headers: dict,
                            title: str, assigned_to_user_id: int, taskcatid: int, msg: str) -> bool:
    logging.info("Updating task message/title/assignee...")
    url_task = f"{BASE_API}/{TODO_RESOURCE}/{task_id}"
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
        body = await r.text()
        if r.status == 200:
            logging.info("Task updated (PUT todorequests).")
            return True
        logging.error(f"Task PUT failed: {r.status} {body[:500]}")
        return False


async def _get_latest_history_id(session: aiohttp.ClientSession, task_id: int, headers: dict) -> Optional[int]:
    # History is under /v1/tasks
    url_hist = f"{BASE_API}/{TASKS_RESOURCE}/{task_id}/history"
    logging.info(f"History ID Def {task_id}, {url_hist}")
    async with session.get(url_hist, headers=headers) as r_hist:
        body = await r_hist.text()
        logging.info(f"History ID Def {body}")
        if r_hist.status != 200:
            logging.error(f"History GET failed: {r_hist.status} {body[:500]}")
            return None
        try:
            hist = json.loads(body)
        except Exception as e:
            logging.error(f"History JSON parse error: {e} body={body[:500]}")
            return None

        if not hist:
            logging.error("History list empty; cannot lock an entry.")
            return None

        try:
            hist.sort(key=lambda h: _parse_iso(h.get("Date") or h.get("CreatedDate")), reverse=True)
        except Exception:
            pass

        hid = hist[0].get("Id")
        logging.info(f"Locked history_id={hid}")
        return hid



async def _upload_file_for_history(session: aiohttp.ClientSession, task_id: int, history_id: int,
                                   headers: dict, filename: str, file_bytes: bytes,
                                   content_type: str) -> bool:
    """
    1) Presign via Buildium: POST /v1/tasks/{taskId}/history/{historyId}/files/uploadrequests
    2) POST multipart/form-data to S3 BucketUrl with EXACT fields from FormData, file LAST
    Mirrors presigned Content-Type and X-Amz-Meta-Buildium-File-Name to satisfy S3 policy and Buildium finalize.
    """
    try:
        logging.info(f"[presign] start filename='{filename}', size={len(file_bytes)} bytes")

        url_presign = f"{BASE_API}/{TASKS_RESOURCE}/{task_id}/history/{history_id}/files/uploadrequests"
        async with session.post(url_presign, json={"FileName": filename}, headers=headers) as r_pre:
            pre_text = await r_pre.text()
            if r_pre.status != 201:
                logging.error(f"[presign] FAIL {r_pre.status} {pre_text[:500]}")
                return False
            try:
                pre = json.loads(pre_text)
            except Exception as e:
                logging.error(f"[presign] JSON parse error: {e} body={pre_text[:500]}")
                return False

        form = pre.get("FormData") or {}
        bucket_url = pre.get("BucketUrl")
        if not bucket_url or not form:
            logging.error("[presign] Missing BucketUrl or FormData.")
            return False

        # Normalize + mirror presigned values
        form = {str(k): ("" if v is None else str(v)) for k, v in form.items()}
        file_ct   = form.get("Content-Type", content_type)
        file_name = form.get("X-Amz-Meta-Buildium-File-Name", filename)

        logging.info(f"[presign] ok Key={form.get('Key')} CT={file_ct} BuildiumName='{file_name}'")

        ordered_keys = [
            "Key", "ACL", "Policy", "Content-Type", "Content-Disposition",
            "X-Amz-Algorithm", "X-Amz-Credential", "X-Amz-Date", "X-Amz-Signature",
            "X-Amz-Meta-Buildium-Entity-Type", "X-Amz-Meta-Buildium-Entity-Id",
            "X-Amz-Meta-Buildium-File-Source", "X-Amz-Meta-Buildium-File-Description",
            "X-Amz-Meta-Buildium-Account-Id", "X-Amz-Meta-Buildium-File-Name",
            "X-Amz-Meta-Buildium-File-Title", "X-Amz-Meta-Buildium-Child-Entity-Id",
            "X-Amz-Meta-Buildium-Finalize-Upload-Message-Version",
            "success_action_status", "success_action_redirect",
        ]

        # Avoid surprising quoting that can break S3 policies
        form_data = aiohttp.FormData(quote_fields=False)
        for k in ordered_keys:
            if k in form:
                form_data.add_field(k, form[k])
        for k, v in form.items():
            if k not in ordered_keys:
                form_data.add_field(k, v)

        # file LAST; match presigned CT/name if present
        form_data.add_field("file", file_bytes, filename=file_name, content_type=file_ct)

        logging.info(f"[upload] POST {bucket_url}")
        async with session.post(bucket_url, data=form_data) as resp:
            body = await resp.text()
            if resp.status in (204, 200, 201):
                logging.info(f"[upload] OK {resp.status} '{file_name}' body={body[:200]}")
                return True
            logging.error(f"[upload] FAIL {resp.status} '{file_name}' body={body[:500]}")
            return False

    except Exception as e:
        logging.exception(f"[upload] Exception '{filename}': {e}")
        return False


async def _wait_for_files(session: aiohttp.ClientSession, task_id: int, history_id: int, headers: dict,
                          expected_names: list[str], timeout_s: int = 30) -> bool:
    """
    Optional: poll for attachments to appear on the history entry.
    """
    import time, asyncio
    url = f"{BASE_API}/{TASKS_RESOURCE}/{task_id}/history/{history_id}/files"
    deadline = time.monotonic() + timeout_s
    want = set([n.strip() for n in expected_names])

    logging.info(f"Polling for files to appear on history {history_id}: {sorted(want)}")
    while time.monotonic() < deadline:
        async with session.get(url, headers=headers) as r:
            body = await r.text()
            if r.status == 200:
                try:
                    items = json.loads(body) or []
                except Exception:
                    items = []
                names = { (i.get("FileName") or i.get("Title") or "").strip() for i in items }
                if want.issubset(names):
                    logging.info("All attachments visible on history.")
                    return True
        await asyncio.sleep(1.5)
    logging.warning("Attachments did not appear within timeout.")
    return False


def split_pdf_bytes(pdf_bytes: bytes, max_bytes: int = 15 * 1024 * 1024) -> list[bytes]:
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
        # carry metadata if available
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
            # Single page > limit: emit it alone
            writer = PdfWriter()
            writer.add_page(reader.pages[i])
            buf = BytesIO()
            writer.write(buf)
            parts.append(buf.getvalue())
            i += 1

    return parts


async def _do_post_put_flow(session, task_id, headers, part_names_and_bytes, json_filename, json_bytes):
    # 2) Lock HISTORY
    history_id = await _get_latest_history_id(session, task_id, headers)
    logging.info("Starting Task History ID")
    if not history_id:
        logging.error("Could not find latest history entry to attach files.")
        return False

    logging.info(f"About to upload {len(part_names_and_bytes)} PDF part(s): {[n for n,_ in part_names_and_bytes]} + {json_filename}")

    # 3) Upload PDFs
    for part_filename, part_bytes in part_names_and_bytes:
        logging.info(f"Uploading PDF: {part_filename} ({len(part_bytes)} bytes)")
        ok_pdf = await _upload_file_for_history(
            session, task_id, history_id, headers,
            part_filename, part_bytes, content_type="application/pdf"
        )
        if not ok_pdf:
            logging.error(f"PDF upload failed for {part_filename}")
            return False

    # 4) Upload JSON
    logging.info(f"Uploading JSON: {json_filename} ({len(json_bytes)} bytes)")
    ok_json = await _upload_file_for_history(
        session, task_id, history_id, headers,
        json_filename, json_bytes, content_type="application/json"
    )
    if not ok_json:
        logging.error("JSON upload failed.")
        return False

    logging.info("Post-PUT uploads finished.")
    return True

# -----------------------------------------------------------------------------
# Main entry
# -----------------------------------------------------------------------------
async def update_task(
    task_data: dict,
    increase_summary: dict,
    increase_effective_date,
    percentage: float | str,
    headers: dict,
    account_id: int | str,   # unused here but kept for signature parity
    buildingjsonfile,        # required (dict/str/bytes)
    logo_source: str | None = None,
    poll_finalize: bool = False,  # set True to verify attachments appear
) -> bool:
    try:
        task_id = int(task_data["Id"])
        taskcatid = int(task_data["Category"]["Id"])
        assigned_to_user_id = int(task_data["AssignedToUserId"])
        title = f"Increase Notices for {increase_effective_date.strftime('%B %d, %Y')} - Review"

        rows = _flatten_rows_from_summary(increase_summary)
        if not rows:
            logging.error("No rows to include in the PDF; aborting.")
            return False

        run_date = datetime.now(UTC).strftime("%Y-%m-%d")
        eff_str = increase_effective_date.strftime("%B %d, %Y")
        pdf_filename  = f"Increase Review Report {eff_str}.pdf"
        json_filename = "data.json"

        # Build the PDF to a temp file and read bytes
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
            try:
                os.remove(pdf_path)
            except Exception:
                pass

        # Split to <=15MB parts
        parts = split_pdf_bytes(pdf_bytes, max_bytes=15 * 1024 * 1024)

        # Name parts predictably (foo.pdf -> foo_part01.pdf, etc.; single-part keeps original)
        orig_name = Path(pdf_filename).name
        stem = Path(orig_name).stem
        suffix = Path(orig_name).suffix or ".pdf"

        if len(parts) == 1:
            part_names_and_bytes = [(orig_name, parts[0])]
        else:
            part_names_and_bytes = [
                (f"{stem}_part{idx+1:02d}{suffix}", b) for idx, b in enumerate(parts)
            ]

        # Normalize provided JSON to bytes
        if isinstance(buildingjsonfile, (bytes, bytearray)):
            json_bytes = bytes(buildingjsonfile)
        elif isinstance(buildingjsonfile, str):
            json_bytes = buildingjsonfile.encode("utf-8")
        else:
            json_bytes = json.dumps(buildingjsonfile, default=str, indent=2).encode("utf-8")

        # Compose task message (mention multi-part if applicable)
        if len(part_names_and_bytes) == 1:
            msg = (
                f'Please review "{orig_name}" for the increase notices. '
                'Should you require any changes, complete the changes, delete this task, '
                'and create a new task entitled "Increase Notices" with the task category set to "System Tasks".'
            )
        else:
            part_list = ", ".join([name for name, _ in part_names_and_bytes])
            msg = (
                f'Please review the report files ({part_list}) for the increase notices. '
                'The report was split into parts to keep each file under 15 MB. '
                'If changes are required, complete them, delete this task, and create a new task entitled '
                '"Increase Notices" with the task category set to "System Tasks".'
            )

        async with aiohttp.ClientSession(timeout=HTTP_TIMEOUT) as session:
            # 1) Create/Update task history message (todorequests)
            try:
                logging.info("Entering shielded post-PUT section...")
                ok_all = await asyncio.shield(
                    _do_post_put_flow(session, task_id, headers, part_names_and_bytes, json_filename, json_bytes)
                )
                if not ok_all:
                    return False
            except asyncio.CancelledError:
                logging.error("update_task cancelled during post-PUT work (server shutdown or SIGTERM).")
                raise

            # # 2) Lock the HISTORY ID *now* (tasks)
            # history_id = await _get_latest_history_id(session, task_id, headers)
            # logging.info("Starting Task History ID")
            # if not history_id:
            #     logging.error("Could not find latest history entry to attach files.")
            #     return False

            # logging.info(f"About to upload {len(part_names_and_bytes)} PDF part(s): {[n for n,_ in part_names_and_bytes]} + {json_filename}")

            # # 3) Upload each PDF part (tasks)
            # for part_filename, part_bytes in part_names_and_bytes:
            #     logging.info(f"Uploading PDF: {part_filename} ({len(part_bytes)} bytes)")
            #     ok_pdf = await _upload_file_for_history(
            #         session, task_id, history_id, headers,
            #         part_filename, part_bytes, content_type="application/pdf"
            #     )
            #     if not ok_pdf:
            #         logging.error(f"PDF upload failed for {part_filename}")
            #         return False

            # # 4) Upload the JSON payload (tasks)
            # logging.info(f"Uploading JSON: {json_filename} ({len(json_bytes)} bytes)")
            # ok_json = await _upload_file_for_history(
            #     session, task_id, history_id, headers,
            #     json_filename, json_bytes, content_type="application/json"
            # )
            # if not ok_json:
            #     logging.error("JSON upload failed.")
            #     return False

            # # 5) Optional: verify attachments appeared (Buildium finalize)
            # if poll_finalize:
            #     expected = [name for name, _ in part_names_and_bytes] + [json_filename]
            #     await _wait_for_files(session, task_id, history_id, headers, expected_names=expected, timeout_s=30)

            logging.info("Task update completed successfully.")
            return True

    except Exception as e:
        logging.exception(f"Error updating task: {e} for increase notices")
        return False
