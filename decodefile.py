import json
import os
from cryptography.fernet import Fernet
import logging
import aiohttp

async def decode(headers, task_data, client_secret):
    
    task_id = task_data['Id']

    async with aiohttp.ClientSession() as session:
        try:
            try:
                # Fetch task history
                urltaskhistory = f"https://api.buildium.com/v1/tasks/{task_id}/history"

                async with session.get(urltaskhistory, headers=headers) as response:
                    if response.status != 200:
                        print("Error while getting task history", await response.text())
                        return
                    taskhistorydata = await response.json()
                    value = 0
                    for value in range(len(taskhistorydata)):
                        file_ids = taskhistorydata[value]['FileIds']
                        if file_ids:  # Check if FileIds list is not empty
                            taskhistoryid = taskhistorydata[value]['Id']
                            taskfileid = file_ids[0]  # Get the first file ID, adjust if needed
                            break
                    else:
                        print("No files found in task history.")
                        return  # Assuming the first file in FileIds
            except Exception as e:
                logging.error(f"Error getting task history: {e}")

            # Download request URL
            urltaskdownload = f"https://api.buildium.com/v1/tasks/{task_id}/history/{taskhistoryid}/files/{taskfileid}/downloadrequest"
            try:
                async with session.post(urltaskdownload, headers=headers) as response:
                    if response.status != 201:
                        print(f"Error requesting file download: {await response.text()}")
                        return

                    downloadfileurldata = await response.json()
                    downloadfileurl = downloadfileurldata['DownloadUrl']
            except Exception as e:
                logging.error(f"Error getting download url: {e}")

            # Now download the actual file from downloadfileurl
            async with session.get(downloadfileurl) as file_response:
                if file_response.status != 200:
                    print(f"Error downloading file: {await file_response.text()}")
                    return

                # Define the file path in /tmp
                full_file_path = '\\tmp\\temp_Increase_Notice_Data.json'

                # Write the downloaded file to /tmp
                with open(full_file_path, 'wb') as f:
                    f.write(await file_response.read())

                print(f"File downloaded and saved to {full_file_path}")
        
        except Exception as e:
            logging.error(f"Error downloading Json File: {e}")
            return

        # Now, decrypt the file
        try:
            cipher = Fernet(client_secret)
            
            with open(full_file_path, 'rb') as file:
                encrypted_data = file.read()
                decrypted_data = cipher.decrypt(encrypted_data)
                decrypted_list = json.loads(decrypted_data.decode())
                
            
            os.remove(full_file_path)

        except Exception as e:
            logging.error(f"Error decrypting or processing the file: {e}")
    logging.info("Json File Successfully Downloaded and Decrypted")
    return decrypted_list
