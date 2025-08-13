import json
import os
import tempfile
from cryptography.fernet import Fernet
import logging
import aiohttp
from aiohttp import ClientTimeout

async def decode(headers, task_data, client_secret, expected_filename: str | None = None):
    """
    Find the JSON attachment on the task (not the PDF), download it, decrypt with Fernet,
    parse JSON, and return a list. Returns None on failure.

    Selection priority per history entry (newest → oldest):
      1) exact name match == expected_filename (if provided)
      2) ContentType == application/json
      3) filename endswith .json
      4) ContentType == application/octet-stream AND filename endswith .json or .json.enc
    """
    task_id = task_data["Id"]
    decrypted_list = None

    timeout = ClientTimeout(total=120, sock_connect=15, sock_read=45)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        try:
            # 1) Get history, newest first
            url_hist = f"https://api.buildium.com/v1/tasks/{task_id}/history"
            async with session.get(url_hist, headers=headers) as resp:
                if resp.status != 200:
                    logging.error(f"history GET failed: {resp.status} {await resp.text()}")
                    return None
                history = await resp.json()

            try:
                history.sort(key=lambda h: h.get("Date") or h.get("CreatedDate") or "", reverse=True)
            except Exception:
                pass

            chosen_history_id = None
            chosen_file_id = None
            chosen_file_name = None
            seen = []  # for diagnostics

            for h in history:
                hid = h.get("Id")
                # 2) Ask Buildium for *file objects* for this history entry
                url_files = f"https://api.buildium.com/v1/tasks/{task_id}/history/{hid}/files"
                async with session.get(url_files, headers=headers) as rf:
                    if rf.status != 200:
                        logging.warning(f"files GET {hid} failed: {rf.status} {await rf.text()}")
                        continue
                    files = await rf.json()  # list of { Id, FileName/Name, ContentType/MimeType, ... }

                # Normalize helper
                def _name(f):
                    return (f.get("FileName") or f.get("Name") or "").strip()
                def _ctype(f):
                    return (f.get("ContentType") or f.get("MimeType") or "").lower()
                def _fid(f):
                    return f.get("Id")

                # Keep for logs
                seen.extend([_name(f) or _fid(f) for f in files])

                # 2.1 exact filename (best)
                if expected_filename:
                    for f in files:
                        if _name(f) == expected_filename:
                            chosen_history_id = hid
                            chosen_file_id = _fid(f)
                            chosen_file_name = _name(f)
                            break
                if chosen_file_id:
                    break

                # 2.2 application/json
                for f in files:
                    if _ctype(f) == "application/json":
                        chosen_history_id = hid
                        chosen_file_id = _fid(f)
                        chosen_file_name = _name(f)
                        break
                if chosen_file_id:
                    break

                # 2.3 endswith .json
                for f in files:
                    if _name(f).lower().endswith(".json"):
                        chosen_history_id = hid
                        chosen_file_id = _fid(f)
                        chosen_file_name = _name(f)
                        break
                if chosen_file_id:
                    break

                # 2.4 octet-stream but looks like json
                for f in files:
                    ct = _ctype(f)
                    nm = _name(f).lower()
                    if ct == "application/octet-stream" and (nm.endswith(".json") or nm.endswith(".json.enc")):
                        chosen_history_id = hid
                        chosen_file_id = _fid(f)
                        chosen_file_name = _name(f)
                        break
                if chosen_file_id:
                    break

            if not chosen_file_id:
                logging.error(f"No JSON file found on task {task_id}. Seen files: {seen}")
                return None

            logging.info(f"Selected file: {chosen_file_name} (id={chosen_file_id}) from history {chosen_history_id}")

            # 3) Request a download URL for that file
            url_dl_req = f"https://api.buildium.com/v1/tasks/{task_id}/history/{chosen_history_id}/files/{chosen_file_id}/downloadrequest"
            async with session.post(url_dl_req, headers=headers, json={}) as dr:
                if dr.status not in (200, 201):
                    logging.error(f"downloadrequest POST failed: {dr.status} {await dr.text()}")
                    return None
                dl = await dr.json()
                download_url = dl.get("DownloadUrl")
                if not download_url:
                    logging.error(f"No DownloadUrl in response: {dl}")
                    return None

            # 4) Download bytes
            async with session.get(download_url) as df:
                if df.status != 200:
                    logging.error(f"download GET failed: {df.status} {await df.text()}")
                    return None
                content_type = df.headers.get("Content-Type", "")
                data_bytes = await df.read()
                logging.info(f"Downloaded {len(data_bytes)} bytes (content-type={content_type})")

            # 5) Decrypt + parse
            tmp_dir = tempfile.gettempdir()
            tmp_path = os.path.join(tmp_dir, "temp_Increase_Notice_Data.json.enc")
            try:
                with open(tmp_path, "wb") as f:
                    f.write(data_bytes)

                cipher = Fernet(client_secret)
                with open(tmp_path, "rb") as f:
                    enc = f.read()
                dec = cipher.decrypt(enc)

                obj = json.loads(dec.decode("utf-8"))
                decrypted_list = obj if isinstance(obj, list) else [obj]
                logging.info("Json File Successfully Downloaded and Decrypted")
            finally:
                try:
                    if os.path.exists(tmp_path):
                        os.remove(tmp_path)
                except Exception:
                    pass

        except Exception:
            logging.exception("Error downloading/decrypting JSON file")
            return None

    return decrypted_list
