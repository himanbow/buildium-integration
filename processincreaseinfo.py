### TEMP Items for debugging summary issues
#1) uploadN1filestolease - Commented
#2) leaserenewalingored - Commented
#3) leaserenewals - Commented
#4) Added Lucas's Userid to createtask
#5) Added check in logging.info(f"Summary data count: {len(summary_data)}")
                        ###logging.info("Calling add_summary_page")

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

semaphore = asyncio.Semaphore(2)

async def fetch_data(session, url, headers, type):
    """Fetch data asynchronously with rate limiting and semaphore control."""
    logging.info("Fetching Data From Buildium")
    try:
        # Limit concurrent requests using semaphore
        async with semaphore:
            while True:
                async with session.request(type, url, headers=headers) as response:
                    status_code = response.status
                    data = await response.json()

                    if status_code == 429:
                        logging.info("Rate limit reached, sleeping for 0.201 seconds")
                        await asyncio.sleep(0.201)  # Rate limit sleep for 429 status
                        continue  # Retry the request after sleeping

                    # Handle both dict and list responses
                    if isinstance(data, (dict, list)):
                        return data
                    else:
                        logging.info(f"Unexpected response format: {data}")
                        return {}

    except Exception as e:
        logging.info(f"Error fetching data from {url}: {e}")
        return {}
async def category(headers, session, date):
    url = "https://api.buildium.com/v1/files/categories"
    params = {
        'limit' : 1000
    }
    async with session.get(url, headers=headers) as response:
        

        category_list = await response.json()
        category_id = None
        for category in category_list:
            if category['Name'] == f'Increases {date}':
                category_id = category['Id']
                break

        if category_id is None:
            payload = {
            'Name': f'Increases {date}'
        }
            async with session.request("POST", url, headers=headers, json=payload) as response:
                category_id = await response.json()['Id']
    return category_id

async def amazondatatask(payload):
    # Prepare the form data for the S3-like upload
    form_data = aiohttp.FormData()
    form_data.add_field("Key", payload["FormData"]["Key"])
    form_data.add_field("ACL", payload["FormData"]["ACL"])
    form_data.add_field("Policy", payload["FormData"]["Policy"])
    form_data.add_field("Content-Type", payload["FormData"]["Content-Type"])
    form_data.add_field("Content-Disposition", payload["FormData"]["Content-Disposition"])
    form_data.add_field("X-Amz-Algorithm", payload["FormData"]["X-Amz-Algorithm"])
    form_data.add_field("X-Amz-Credential", payload["FormData"]["X-Amz-Credential"])
    form_data.add_field("X-Amz-Date", payload["FormData"]["X-Amz-Date"])
    form_data.add_field("X-Amz-Signature", payload["FormData"]["X-Amz-Signature"])
    form_data.add_field("X-Amz-Meta-Buildium-Entity-Type", payload["FormData"]["X-Amz-Meta-Buildium-Entity-Type"])
    form_data.add_field("X-Amz-Meta-Buildium-Entity-Id", payload["FormData"]["X-Amz-Meta-Buildium-Entity-Id"])
    form_data.add_field("X-Amz-Meta-Buildium-File-Source", payload["FormData"]["X-Amz-Meta-Buildium-File-Source"])
    form_data.add_field("X-Amz-Meta-Buildium-File-Description", payload["FormData"]["X-Amz-Meta-Buildium-File-Description"])
    form_data.add_field("X-Amz-Meta-Buildium-Account-Id", payload["FormData"]["X-Amz-Meta-Buildium-Account-Id"])
    form_data.add_field("X-Amz-Meta-Buildium-File-Name", payload["FormData"]["X-Amz-Meta-Buildium-File-Name"])
    form_data.add_field("X-Amz-Meta-Buildium-File-Title", payload["FormData"]["X-Amz-Meta-Buildium-File-Title"])
    form_data.add_field("X-Amz-Meta-Buildium-Child-Entity-Id", payload["FormData"]["X-Amz-Meta-Buildium-Child-Entity-Id"])
    form_data.add_field("X-Amz-Meta-Buildium-Finalize-Upload-Message-Version", payload["FormData"]["X-Amz-Meta-Buildium-Finalize-Upload-Message-Version"])
    bucket_url = payload["BucketUrl"]
    
    return form_data, bucket_url


async def amazondatalease(payload):
    # Prepare the form data for the S3-like upload
    form_data = aiohttp.FormData()
    form_data.add_field("Key", payload["FormData"]["Key"])
    form_data.add_field("ACL", payload["FormData"]["ACL"])
    form_data.add_field("Policy", payload["FormData"]["Policy"])
    form_data.add_field("Content-Type", payload["FormData"]["Content-Type"])
    form_data.add_field("Content-Disposition", payload["FormData"]["Content-Disposition"])
    form_data.add_field("X-Amz-Algorithm", payload["FormData"]["X-Amz-Algorithm"])
    form_data.add_field("X-Amz-Credential", payload["FormData"]["X-Amz-Credential"])
    form_data.add_field("X-Amz-Date", payload["FormData"]["X-Amz-Date"])
    form_data.add_field("X-Amz-Signature", payload["FormData"]["X-Amz-Signature"])
    form_data.add_field("X-Amz-Meta-Buildium-Entity-Type", payload["FormData"]["X-Amz-Meta-Buildium-Entity-Type"])
    form_data.add_field("X-Amz-Meta-Buildium-Entity-Id", payload["FormData"]["X-Amz-Meta-Buildium-Entity-Id"])
    form_data.add_field("X-Amz-Meta-Buildium-File-Source", payload["FormData"]["X-Amz-Meta-Buildium-File-Source"])
    form_data.add_field("X-Amz-Meta-Buildium-File-Description", payload["FormData"]["X-Amz-Meta-Buildium-File-Description"])
    form_data.add_field("X-Amz-Meta-Buildium-Account-Id", payload["FormData"]["X-Amz-Meta-Buildium-Account-Id"])
    form_data.add_field("X-Amz-Meta-Buildium-File-Name", payload["FormData"]["X-Amz-Meta-Buildium-File-Name"])
    form_data.add_field("X-Amz-Meta-Buildium-File-Title", payload["FormData"]["X-Amz-Meta-Buildium-File-Title"])
    form_data.add_field("X-Amz-Meta-Buildium-File-Category-Id", payload["FormData"]["X-Amz-Meta-Buildium-File-Category-Id"])
    form_data.add_field("X-Amz-Meta-Buildium-Finalize-Upload-Message-Version", payload["FormData"]["X-Amz-Meta-Buildium-Finalize-Upload-Message-Version"])
    bucket_url = payload["BucketUrl"]
    
    return form_data, bucket_url

async def generateN1files(leaseid, leasedata):
    logging.info(f"Generating Increase Notices for lease {leaseid}")
    filepath = await generateN1notice.create(leaseid, leasedata)
    return filepath

async def uploadN1filestolease(headers, filepath, leaseid, session, categoryid):
    logging.info(f"Uploading N1 File to Lease {leaseid}")

    base_path, filename = filepath.split("\\tmp\\", 1)

    # # Prepare the URL to request the file upload instructions
    # try:
    #     url = f"https://api.buildium.com/v1/files/uploadrequests"
    #     data = {'FileName': filename}

    #     body = {
    #         "EntityType": "Lease",
    #         "EntityId": leaseid,
    #         "FileName": filename,
    #         "Title": filename,
    #         "CategoryId": categoryid
    #         }

    #     # Post the file metadata to get upload instructions
    #     async with session.post(url, json=body, headers=headers) as response:
    #         if response.status != 201:
    #             logging.info(f"Error while submitting file metadata: {await response.text()}")
    #             return False  # Indicate failure

    #         # Get the response payload with upload instructions
    #         payload = await response.json()
    #         form_data, bucket_url = await amazondatalease(payload)

    #         # Read the file content asynchronously
    #         async with aiofiles.open(filepath, 'rb') as file:
    #             file_content = await file.read()

    #         # Add the file content to the form data
    #         form_data.add_field("file", file_content, filename=filename, content_type='application/pdf')

    #         # Upload the file to the S3 bucket
    #         try:
    #             async with session.post(bucket_url, data=form_data) as upload_response:
    #                 if upload_response.status == 204:  # S3 usually responds with 204 No Content for successful uploads
    #                     logging.info(f"Upload of Notice for {leaseid} successful.")
    #                     return True  # Indicate success
    #                 else:
    #                     logging.info(f"Error Uploading Notice for {leaseid} failed: {upload_response.status} {await upload_response.text()}")
    #                     return False  # Indicate failure

    #         except Exception as e:
    #             logging.info(f"An error occurred uploading: {str(e)}")
    #             return False  # Indicate failure

    # except Exception as e:
    #     logging.error(f"Error Uploading Summary to task: {e}")
    #     return False  # Indicate failure




    # Placeholder for actual upload logic
    confirm = True  # Assume the upload is successful
    return confirm

async def uploadsummarytotask(headers, filepath, taskid, session, categoryid):
    logging.info(f"Uploading Summary to Task {taskid}")

    try:
        # Split the filepath to extract the filename after '\\tmp\\'
        base_path, filename = filepath.split("\\tmp\\", 1)

        # Fetch task history to get the latest task history ID
        urltaskhistory = f"https://api.buildium.com/v1/tasks/{taskid}/history"
        async with session.get(urltaskhistory, headers=headers) as response:
            if response.status != 200:
                logging.info(f"Error while getting task history: {await response.text()}")
                return False  # Indicate failure

            taskhistorydata = await response.json()
            taskhistoryid = taskhistorydata[0]['Id']

            # Prepare the URL to request the file upload instructions
            url = f"https://api.buildium.com/v1/tasks/{taskid}/history/{taskhistoryid}/files/uploadrequests"
            data = {'FileName': filename}

            # Post the file metadata to get upload instructions
            async with session.post(url, json=data, headers=headers) as response:
                if response.status != 201:
                    logging.info(f"Error while submitting file metadata: {await response.text()}")
                    return False  # Indicate failure

                # Get the response payload with upload instructions
                payload = await response.json()
                form_data, bucket_url = await amazondatatask(payload)

                # Read the file content asynchronously
                async with aiofiles.open(filepath, 'rb') as file:
                    file_content = await file.read()

                # Add the file content to the form data
                form_data.add_field("file", file_content, filename=filename, content_type='application/pdf')

                # Upload the file to the S3 bucket
                try:
                    async with session.post(bucket_url, data=form_data) as upload_response:
                        if upload_response.status == 204:  # S3 usually responds with 204 No Content for successful uploads
                            logging.info(f"Upload successful for Task {taskid}.")
                            return True  # Indicate success
                        else:
                            logging.info(f"Error Uploading File for Task {taskid}: {upload_response.status} {await upload_response.text()}")
                            return False  # Indicate failure

                except Exception as e:
                    logging.info(f"An error occurred uploading: {str(e)}")
                    return False  # Indicate failure

    except Exception as e:
        logging.error(f"Error Uploading Summary to task: {e}")
        return False  # Indicate failure

    return True  # Default return for successful execution

async def leaserenewalingored(headers, leaseid, lease, session):
    logging.info(f"Processing ignored leases")

        # url = f"https://api.buildium.com/v1/leases/{leaseid}"
        # async with session.get(url, headers=headers) as response:
        #     data = await response.json()
        #     date = datetime.strptime(data["LeaseToDate"], "%Y-%m-%d")
        #     date = date + relativedelta(months=6)
        #     date = date.strftime("%Y-%m-%d")
        #     payloadstr = {
        #             "LeaseType": data["LeaseType"],
        #             "UnitId" : data['UnitId'],
        #             "LeaseFromDate" : data['LeaseFromDate'],
        #             "LeaseToDate": date,
        #             "IsEvictionPending": data['IsEvictionPending'],
        #         }
            
        #     async with session.put(url, json=payloadstr, headers=headers) as response:
        #                         if response.status == 200:  # S3 usually responds with 204 No Content for successful uploads
        #                             logging.info(f"Extention Completed for {leaseid}.")
        #                         else:
        #                             logging.error(f"Error extending {leaseid}: {response.status} {await response.text()}")

async def setevictionstatus(leaseid, eviction, session, headers):
    print("test")
    url = f"https://api.buildium.com/v1/leases/{leaseid}"
    async with session.get(url, headers=headers) as response:
        data = await response.json()

        payload = {
            "LeaseType": data["LeaseType"],
            "UnitId" : data['UnitId'],
            "LeaseFromDate" : data['LeaseFromDate'],
            "LeaseToDate": data['LeaseToDate'],
            "IsEvictionPending": eviction,
            "AutomaticallyMoveOutTenants": False
            }
        async with session.put(url, json=payload, headers=headers) as response:
            if response.status == 200:  # S3 usually responds with 204 No Content for successful uploads
                logging.info(f"Extention Completed for {leaseid}.")
                eviction = True
            else:
                logging.error(f"Error extending {leaseid}: {response.status} {await response.text()}")
    return eviction

async def leaserenewals(headers, leaseid, lease, session):
    logging.info(f"Processing Lease Renewal for Lease {leaseid}")

    # url = f"https://api.buildium.com/v1/leases/{leaseid}/renewals"

    # date = datetime.strptime(lease["LeaseToDate"], "%Y-%m-%d")
    # date = date + relativedelta(years=1) - timedelta(days=1)
    # date = date.strftime("%Y-%m-%d")
    
    
    # if lease["RecurringChargesToStop"] is None:
    #     payloadstr = {
    #             "LeaseType": lease["LeaseType"],
    #             "LeaseToDate": date,
    #             "Rent": lease["Rent"],
    #             "TenantIds": lease["TenantIds"],
    #             "SendWelcomeEmail": "false",
    #         }
    # else:
    #     recurring_charges_list = [int(charge.strip()) for charge in lease["RecurringChargesToStop"].split(',')]
    #     payloadstr = {
    #             "LeaseType": lease["LeaseType"],
    #             "LeaseToDate": date,
    #             "Rent": lease["Rent"],
    #             "TenantIds": lease["TenantIds"],
    #             "SendWelcomeEmail": "false",
    #             "RecurringChargesToStop": recurring_charges_list
    #         }
    # print(payloadstr)
        
    # async with session.post(url, json=payloadstr, headers=headers) as response:
    #                 if response.status == 201:
    #                     logging.info(f"Renewal Completed for {leaseid}.")
    #                 if response.status == 409:
    #                     check = False
    #                     check = await setevictionstatus(leaseid, check, session, headers)
    #                     if check:  # No need to use await since 'check' is a boolean
    #                         await leaserenewals(headers, leaseid, lease, session)
    #                         # Reset eviction status after successful renewal
    #                         await setevictionstatus(leaseid, False, session, headers)

                            

    #                 else:
    #                     logging.error(f"Error renewing {leaseid}: {response.status} {await response.text()}")

async def createtask(headers, buildingid, session, date):
    """Create a task for delivering increase notices for a given building."""
    logging.info(f"Creating Task for Building {buildingid}")
    url = f"https://api.buildium.com/v1/rentals/{buildingid}"
    urlcat = "https://api.buildium.com/v1/tasks/categories"
    urltask = "https://api.buildium.com/v1/tasks/todorequests"
    params = {'limit': 1000}

    try:
        # Fetch building information
        async with session.get(url, headers=headers) as response:
            if response.status != 200:
                logging.error(f"Failed to fetch building data. Status code: {response.status}")
                return None

            useriddata = await response.json()
            userid = useriddata.get('RentalManager', {}).get('Id')

### Added Lucas's User ID
            userid = 352081
            buildingname = useriddata.get('Name')

            # Fetch task categories
            async with session.get(urlcat, params=params, headers=headers) as response:
                if response.status != 200:
                    logging.error(f"Failed to fetch task categories. Status code: {response.status}")
                    return None

                category_list = await response.json()
                category_id = None

                # Find the category ID for 'Increase Notices'
                for category in category_list:
                    if category.get('Name') == 'Increase Notices':
                        category_id = category.get('Id')
                        break

                # If the category does not exist, create it
                if category_id is None:
                    payload = {'Name': 'Increase Notices'}
                    async with session.post(urlcat, headers=headers, json=payload) as response:
                        if response.status != 201:
                            logging.error(f"Failed to create task category. Status code: {response.status}")
                            return None

                        category_data = await response.json()
                        category_id = category_data.get('Id')
                
                # 

                # Create the task for delivering notices
                payloadtask = {
                    'Title': f'Deliver Notices for {date} Increases',
                    'CategoryId': category_id,
                    'Description' : f'Please deliver the attached N1 Increase Notices.',
                    'PropertyId': buildingid,
                    'AssignedToUserId': userid,
                    'TaskStatus': "New",
                    'Priority': "High",
                    'DueDate': datetime.now().strftime("%Y-%m-%d")
                }
                
                async with session.post(urltask, json=payloadtask, headers=headers) as response:
                    if response.status != 201:
                        logging.error(f"Failed to create task. Status code: {response.status}")
                        return None

                    task_data = await response.json()
                    taskid = task_data.get('Id', 0)
                    logging.info(f"Task created successfully with ID: {taskid}")
                    return taskid

    except Exception as e:
        logging.error(f"Error creating task: {e}")

    return None  # Return None if task creation fails

async def add_summary_page(summary_data, summary_writer, summary_file_path, buildingname, countbuilding, date):
    """Generate and add summary pages to the current summary writer."""
    try:
        # Create the summary page and get the BytesIO object
        summary_page_buffer = await generateN1notice.create_summary_page(summary_data, buildingname, countbuilding, date)

        # Read the pages from the BytesIO object
        summary_pdf = PdfReader(summary_page_buffer)

        # Add each page from the generated summary to the writer
        for page in summary_pdf.pages:
            summary_writer.add_page(page)

        # Save the updated summary file with all summary pages
        with open(summary_file_path, 'wb') as temp_file:
            summary_writer.write(temp_file)

        logging.info(f"Summary pages added to {summary_file_path}.")

    except Exception as e:
        logging.error(f"Error adding summary pages: {e}")

async def addtosummary(summary_file_path, filepath, summary_writer):
    """Add PDF to summary and check size constraints."""
    try:
        # Add pages from the current file to the summary
        reader = PdfReader(filepath)
        for page in reader.pages:
            summary_writer.add_page(page)

        # Save summary temporarily
        with open(summary_file_path, 'wb') as temp_file:
            summary_writer.write(temp_file)

        # Check if the summary file size exceeds 20MB
        current_size = await aiofiles.os.stat(summary_file_path)
        if current_size.st_size > 19 * 1024 * 1024:  # 19MB limit
            logging.info(f"Summary file {summary_file_path} reached the size limit.")
            return False  # Indicate the need to start a new file

        return True  # Summary added successfully

    except Exception as e:
        logging.error(f"Error adding to summary: {e}")
        return False

import aiohttp
import aiofiles
import os

async def download_file(headers, file_id):
    try:
        # 1. Request the download URL
        url = f"https://api.buildium.com/v1/files/{file_id}/downloadrequest"

        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers) as response:
                if response.status != 200:
                    raise Exception(f"Failed to get download URL: {response.status} {await response.text()}")
                data = await response.json()
                download_url = data.get("DownloadUrl")
                if not download_url:
                    raise Exception("DownloadUrl missing in response")

            # 2. Download the file from the returned URL
            async with session.get(download_url) as file_response:
                if file_response.status != 200:
                    raise Exception(f"Failed to download file: {file_response.status} {await file_response.text()}")
                content = await file_response.read()

        # 3. Save the file as \tmp\N1.pdf
        os.makedirs("\\tmp", exist_ok=True)
        file_path = os.path.join("\\tmp", "N1.pdf")
        async with aiofiles.open(file_path, 'wb') as out_file:
            await out_file.write(content)

        print(f"Downloaded file saved to {file_path}")
        return file_path

    except Exception as e:
        print(f"Download failed: {e}")
        return None


async def process(headers, increaseinfo, accountid):
    countall = 0
    check = False
    datelabel = ""
    logging.info("Downloading N1 File")
    file_id = 5422945
    await download_file(headers, file_id)
    
    logging.info("Starting to process increase renewals")

    """Main process handling the creation, merging, uploading, and cleanup of PDFs."""
    try:
        async with aiohttp.ClientSession() as session:

            for buildingdata in increaseinfo:
                for buildingid, data in buildingdata.items():
                    logging.info(f"Processing Building ID {buildingid} increase renewals.")

                    # pull the ignore‐building flag
                    ignore_building = (data.get('ignorebuilding') == "Y")

                    # start fresh for this building
                    summary_writer = PdfWriter()
                    summary_index = 1
                    summary_data = []
                    countbuilding = 0

                    if not data['lease_info']:
                        # nothing at all to do
                        logging.info(f"No Renewals for Building ID {buildingid}.")
                        continue

                    # grab the human name
                    buildingname = data['lease_info'][0]['buildingname']
                    (f"Data grabbed for Building ID {buildingid} increase renewals.")

                    # determine if there is at least one lease NOT ignored
                    has_active = any(lease['ignored'] != "Y"
                                     for lease in data['lease_info'])

                    # only create a new task if at least one lease needs notice
                    # AND we're not skipping the whole building
                    if has_active and not ignore_building:
                        taskid = await createtask(
                            headers, buildingid, session, datelabel
                        )
                        logging.info(f"Task created for Building ID {buildingid}.")

                    # this bit only needs to run once (to get category + datelabel)
                    if not check:
                        date = data['lease_info'][0]['renewal']['LeaseToDate']
                        datelabel = datetime.strptime(date, "%Y-%m-%d") \
                                         .strftime("%B %d, %Y")
                        categoryid = await category(
                            headers, session, datelabel
                        )
                        check = True
                        logging.info(f"Category ID set to {categoryid}.")

                    # now per‐lease work
                    for lease in data['lease_info']:
                        leaseid = lease['leaseid']
                        logging.info(f"Processing Renewal for {leaseid}.")

                        if lease['ignored'] != "Y":
                            logging.info(f"Processing Increase Renewal for {leaseid}.")
                            # active‐lease path → generate & upload N1 + summary
                            leaseincreaseinfo = lease['increasenotice']
                            leaserenewalinfo  = lease['renewal']

                            filepath = await generateN1files(
                                leaseid, leaseincreaseinfo
                            )
                            if not filepath:
                                logging.error(f"Failed N1 for {leaseid}")
                                continue

                            # add to in‐memory summary
                            ok = await addtosummary(
                                os.path.join(
                                    '\\tmp',
                                    f"Notices for {buildingname} {datelabel} Part {summary_index}.pdf"
                                ),
                                filepath,
                                summary_writer
                            )
                            if not ok:
                                # summary rolled over
                                summary_writer = PdfWriter()
                                summary_index += 1
                                ok = await addtosummary(
                                    os.path.join(
                                        '\\tmp',
                                        f"Notices for {buildingname} {datelabel}.pdf"
                                    ),
                                    filepath,
                                    summary_writer
                                )

                            # upload N1 to lease
                            confirmlease = await uploadN1filestolease(
                                headers, filepath, leaseid, session, categoryid
                            )
                            countall += 1
                            countbuilding += 1
                            summary_data.append(lease)

                            if confirmlease and ok:
                                try:
                                    await aiofiles.os.remove(filepath)
                                except Exception as e:
                                    logging.error(f"Cleanup failed: {e}")

                            # always do the renewal call
                            await leaserenewals(
                                headers, leaseid, leaserenewalinfo, session
                            )
                        else:
                            # ignored‐lease path → only renewal‐ignored hook
                            logging.info(f"Processing Non-Increase Renewal for {leaseid}.")
                            await leaserenewalingored(
                                headers, leaseid, lease, session
                            )
                            logging.info("Processing Ignored Lease Renewal.")

                    # once all leases done, finish & upload your summary if appropriate
                    if summary_data and not ignore_building:
                        # recompute the exact filename of our PDF
                        summary_file_path = os.path.join(
                            '\\tmp',
                            f"Notices for {buildingname} {datelabel} Part {summary_index}.pdf"
                        )

                        # add the final summary page
                        logging.info(f"Summary data count: {len(summary_data)}")
                        logging.info("Calling add_summary_page")
                        await add_summary_page(
                            summary_data,
                            summary_writer,
                            summary_file_path,
                            buildingname,
                            countbuilding,
                            datelabel
                        )

                        # upload it against the task we created
                        await uploadsummarytotask(
                            headers,
                            summary_file_path,
                            taskid,
                            session,
                            categoryid
                        )
                        logging.info(f"Summary for {buildingname} uploaded to task.")
                    else:
                        if ignore_building:
                            logging.info(
                                f"Skipping PDF summary & task for building {buildingid} "
                                "(ignorebuilding=Y)"
                            )

    except Exception as e:
        logging.error(f"Error processing leases data: {e}")

    print(countall)


# To execute the process function, call it with necessary arguments as per your requirements
