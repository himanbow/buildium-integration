import json
import os
import tempfile
from cryptography.fernet import Fernet
import logging
import aiohttp
from aiohttp import ClientTimeout

async def decode(headers, task_data, client_secret):
    """
    Find the JSON attachment on the task history (not the PDF), download it via the
    task-history downloadrequest endpoint, decrypt with Fernet, parse as JSON list,
    and return it. Returns None on failure.
    """
    task_id = task_data['Id']
    decrypted_list = None

    timeout = ClientTimeout(total=120, sock_connect=15, sock_read=45)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        try:
            # 1) Get task history, newest first
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

            # 2) Walk histories; for each FileIds, pick the JSON file by metadata
            chosen_history_id = None
            chosen_file_id = None
            seen_files = []

            for h in history:
                file_ids = h.get("FileIds") or []
                if not file_ids:
                    continue

                # probe each file id for metadata to decide if it's JSON
                for fid in file_ids:
                    # file metadata endpoint
                    url_file_meta = f"https://api.buildium.com/v1/files/{fid}"
                    async with session.get(url_file_meta, headers=headers) as mf:
                        if mf.status != 200:
                            # can't read metadata; remember we saw it and move on
                            seen_files.append(fid)
                            continue
                        meta = await mf.json()
                        fname = (meta.get("FileName") or meta.get("Name") or "").strip()
                        ctype = (meta.get("ContentType") or meta.get("MimeType") or "").lower()

                        # pick rules: prefer explicit JSON content-type, else .json name
                        if ctype == "application/json" or fname.lower().endswith(".json"):
                            chosen_history_id = h.get("Id")
                            chosen_file_id = fid
                            logging.info(f"Selected JSON file from history: id={fid}, name='{fname}', content-type='{ctype}'")
                            break
                        else:
                            seen_files.append(fname or fid)

                if chosen_file_id:
                    break

            if not chosen_file_id or not chosen_history_id:
                logging.error(f"No JSON file found on task {task_id}. Seen files: {seen_files}")
                return None

            # 3) Request a download URL for the chosen file (task-history scoped)
            url_dl_req = f"https://api.buildium.com/v1/tasks/{task_id}/history/{chosen_history_id}/files/{chosen_file_id}/downloadrequest"
            async with session.post(url_dl_req, headers=headers, json={}) as dr:
                if dr.status not in (200, 201):
                    logging.error(f"downloadrequest POST failed: {dr.status} {await dr.text()}")
                    return None
                dl_payload = await dr.json()
                download_url = dl_payload.get("DownloadUrl")
                if not download_url:
                    logging.error(f"No DownloadUrl in downloadrequest payload: {dl_payload}")
                    return None

            # 4) Download the JSON bytes
            async with session.get(download_url) as df:
                if df.status != 200:
                    logging.error(f"download GET failed: {df.status} {await df.text()}")
                    return None
                content_type = df.headers.get("Content-Type", "")
                data_bytes = await df.read()
                logging.info(f"Downloaded {len(data_bytes)} bytes (content-type={content_type})")

            # (Optional sanity) If server mislabeled but we expect JSON, allow anyway.

            # 5) Write to a temp file, decrypt, parse
            tmp_dir = tempfile.gettempdir()
            tmp_path = os.path.join(tmp_dir, "temp_Increase_Notice_Data.json.enc")
            try:
                with open(tmp_path, "wb") as f:
                    f.write(data_bytes)

                cipher = Fernet(client_secret)
                with open(tmp_path, "rb") as f:
                    enc = f.read()
                dec = cipher.decrypt(enc)

                # Parse JSON; normalize to list
                obj = json.loads(dec.decode("utf-8"))
                decrypted_list = obj if isinstance(obj, list) else [obj]

                logging.info("Json File Successfully Downloaded and Decrypted")
            finally:
                try:
                    if os.path.exists(tmp_path):
                        os.remove(tmp_path)
                except Exception:
                    pass

        except Exception as e:
            logging.exception("Error downloading/decrypting JSON file")
            return None

    return decrypted_list
