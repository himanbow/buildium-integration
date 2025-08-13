import json
import os
from cryptography.fernet import Fernet
import logging
import aiohttp

async def decode(headers, task_data, client_secret):
    task_id = task_data['Id']

    async with aiohttp.ClientSession() as session:
        try:
            # Fetch task history
            urltaskhistory = f"https://api.buildium.com/v1/tasks/{task_id}/history"
            async with session.get(urltaskhistory, headers=headers) as response:
                if response.status != 200:
                    logging.error("Error while getting task history: %s", await response.text())
                    return
                taskhistorydata = await response.json()

                # Search for file named data.json
                taskhistoryid = None
                taskfileid = None
                seen_files = []

                for entry in taskhistorydata:
                    file_ids = entry.get('FileIds', [])
                    if not file_ids:
                        continue

                    # Check file names for data.json
                    for fid in file_ids:
                        file_meta_url = f"https://api.buildium.com/v1/files/{fid}"
                        async with session.get(file_meta_url, headers=headers) as meta_resp:
                            if meta_resp.status != 200:
                                logging.error("Error getting file metadata for %s: %s", fid, await meta_resp.text())
                                continue
                            file_meta = await meta_resp.json()
                            seen_files.append(file_meta.get("Name"))

                            if file_meta.get("Name") == "data.json":
                                taskhistoryid = entry['Id']
                                taskfileid = fid
                                break
                    if taskfileid:
                        break

                if not taskfileid:
                    logging.error(f"No JSON file found on task {task_id}. Seen files: {seen_files}")
                    return

            # Request download URL
            urltaskdownload = f"https://api.buildium.com/v1/tasks/{task_id}/history/{taskhistoryid}/files/{taskfileid}/downloadrequest"
            async with session.post(urltaskdownload, headers=headers) as response:
                if response.status != 201:
                    logging.error("Error requesting file download: %s", await response.text())
                    return
                downloadfileurldata = await response.json()
                downloadfileurl = downloadfileurldata['DownloadUrl']

            # Download file
            full_file_path = '/tmp/temp_Increase_Notice_Data.json'
            async with session.get(downloadfileurl) as file_response:
                if file_response.status != 200:
                    logging.error("Error downloading file: %s", await file_response.text())
                    return
                with open(full_file_path, 'wb') as f:
                    f.write(await file_response.read())
            logging.info(f"data.json downloaded to {full_file_path}")

        except Exception as e:
            logging.error(f"Error downloading Json File: {e}")
            return

        # Decrypt file
        try:
            cipher = Fernet(client_secret)
            with open(full_file_path, 'rb') as file:
                encrypted_data = file.read()
                decrypted_data = cipher.decrypt(encrypted_data)
                decrypted_list = json.loads(decrypted_data.decode())
            os.remove(full_file_path)

        except Exception as e:
            logging.error(f"Error decrypting or processing the file: {e}")
            return

    logging.info("Json File Successfully Downloaded and Decrypted")
    return decrypted_list
