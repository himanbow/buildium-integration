import json
import os
import tempfile
from cryptography.fernet import Fernet
import logging
import aiohttp
from aiohttp import ClientTimeout

from rate_limiter import semaphore

def _best_name(meta: dict) -> str:
    """Return the best-available filename from assorted schemas."""
    return (
        (meta.get("FileName") or "").strip()
        or (meta.get("Name") or "").strip()
        or (meta.get("OriginalFileName") or "").strip()
        or (meta.get("Title") or "").strip()
    )

def _ctype(meta: dict) -> str:
    return (meta.get("ContentType") or meta.get("MimeType") or "").lower()

async def decode(headers, task_data, client_secret):
    """
    Find 'data.json' attached to the task history, download, decrypt, parse, return list.
    Returns None if not found or on failure.
    """
    task_id = task_data["Id"]
    decrypted_list = None

    timeout = ClientTimeout(total=120, sock_connect=15, sock_read=45)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        try:
            # 1) Get history (newest first)
            url_hist = f"https://api.buildium.com/v1/tasks/{task_id}/history"
            async with semaphore:
                async with session.get(url_hist, headers=headers) as resp:
                    if resp.status != 200:
                        logging.error(f"history GET failed: {resp.status} {await resp.text()}")
                        return None
                    history = await resp.json()
            try:
                history.sort(key=lambda h: h.get("Date") or h.get("CreatedDate") or "", reverse=True)
            except Exception:
                pass

            chosen_hid = None
            chosen_fid = None
            chosen_name = None
            seen = []

            # 2) Walk entries; use /history/{hid}/files for names, else fall back to /files/{id}
            for h in history:
                hid = h.get("Id")
                files_url = f"https://api.buildium.com/v1/tasks/{task_id}/history/{hid}/files"
                async with semaphore:
                    async with session.get(files_url, headers=headers) as rf:
                        if rf.status != 200:
                            logging.warning(f"files GET {hid} failed: {rf.status} {await rf.text()}")
                            continue
                        files = await rf.json()  # list of dicts; may or may not include names

                # First pass: try to match data.json by any name field present
                for f in files:
                    name = _best_name(f)
                    seen.append(name or f.get("Id"))
                    if name.lower() == "data.json":
                        chosen_hid = hid
                        chosen_fid = f.get("Id")
                        chosen_name = name
                        break
                if chosen_fid:
                    break

                # Second pass: some schemas omit names; fall back to metadata per file id
                for f in files:
                    fid = f.get("Id")
                    if not fid:
                        continue
                    meta_url = f"https://api.buildium.com/v1/files/{fid}"
                    async with semaphore:
                        async with session.get(meta_url, headers=headers) as mf:
                            if mf.status != 200:
                                continue
                            meta = await mf.json()
                    name = _best_name(meta)
                    ctype = _ctype(meta)
                    seen.append(name or fid)
                    if name.lower() == "data.json" or (ctype == "application/json" and name.lower().endswith(".json")):
                        chosen_hid = hid
                        chosen_fid = fid
                        chosen_name = name or "data.json"
                        break
                if chosen_fid:
                    break

            if not chosen_fid:
                logging.error(f"No JSON file found on task {task_id}. Seen files: {seen}")
                return None

            logging.info(f"Selected file: {chosen_name} (id={chosen_fid}) from history {chosen_hid}")

            # 3) Get download URL
            url_dl_req = f"https://api.buildium.com/v1/tasks/{task_id}/history/{chosen_hid}/files/{chosen_fid}/downloadrequest"
            async with semaphore:
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
            async with semaphore:
                async with session.get(download_url) as df:
                    if df.status != 200:
                        logging.error(f"download GET failed: {df.status} {await df.text()}")
                        return None
                    data_bytes = await df.read()

            # 5) Decrypt & parse
            tmp_dir = tempfile.gettempdir()
            tmp_path = os.path.join(tmp_dir, "data.json.enc")
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
