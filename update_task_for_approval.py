import aiohttp
from datetime import datetime
import os
import logging

# Keep track of buildings that have been updated across all calls
message2check = True
buildingnamecheck = ""



def format_unit(value):
    if len(value) == 1:
        return '     ' + value
    elif len(value) == 2:
        return '   ' + value
    elif value == 111:
        return f' {value} '
    elif len(value) == 3:
        return ' ' + value
    else:
        return value

def format_currency(value):
    """Format the value to two decimal places for currency display with a $ sign and add a space if there are less than three '5' or '9' characters."""
    try:
        value = float(value)
        # Format the value based on the given conditions
        # if value < 100:
        #     formatted_value = '     {:,.2f}'.format(value)
        # elif value < 1000:
        #     formatted_value = '   {:,.2f}'.format(value)
        # else:
        #     formatted_value = '{:,.2f}'.format(value)
        formatted_value = '{:,.2f}'.format(value)
        
        # Count occurrences of '5' and '9'
        # count_large_chars = formatted_value.count('2') + formatted_value.count('5') + formatted_value.count('6') + formatted_value.count('9') + formatted_value.count('0') - formatted_value.count('1')
        
        # # Add a space if there are less than three occurrences of '5' or '9'
        # if count_large_chars < 4:
        #     formatted_value += ' '

        return formatted_value
    except ValueError:
        return f"${value}"
    
async def messageforcases(percentage, increase, increase_effective_date_formatted, today, tenant_name, message2, buildingname):
    global message2check
    global buildingnamecheck
    try:
        if message2check is False:
            message2 = f"\nRun Date: {today}\n"
            message2 += f"Increase Effective Date: {increase_effective_date_formatted}\n"
            message2 += f"Guideline Increase Rate: {percentage}%\n"
            message2 += f"Ignored Leases\n\n\n\n"
            message2 += "{:<12}\t{:<12}\t{:<12}\t\t{:<12}\t{:<12}\t{:<12}\t{:<22}\t{:>12}\t{:>12}\t{:>12}\n".format(
                "Current ", "New", "AGI", "Market", "Guideline", "AGI", "Notice and Calculation", "Unit", "Tenant", "Ignore"
                )
            message2 += "{:<12}\t{:<12}\t{:<12}\t{:<12}\t{:<12}\t{:<12}\t{:<12}\t{:>12}\t{:>12}\t{:>12}\n".format(
                    "Rent", "Rent", "Rent", "Rent", "Increase", "Increase", "Percentage", "Number", "Name", "Reason"
                )
            message2 += "-" * 200 + "\n"

        agiincrease = increase['agiincrease']
        agirent = increase['agirent']
        if agiincrease is None:
            agiincrease = "\t"
            agirent = "\t"
            agipercentage = "\t"
            
        else:
            agiincrease = str(format_currency(agiincrease))
            agirent = str(format_currency(agirent))
            agipercentage = increase['calculationpercentage']
        
        tenant_name = increase['tenantname'][:12]  # Limit to 30 characters
        if buildingnamecheck != buildingname:
            buildingnamecheck = buildingname
            if message2check is True:
                message2 += f"\n\n{buildingname}\n"
            else:
                message2 += f"{buildingname}\n"
        message2 += "{:<1}{:<7}{:<3}\t{:<1}{:<7}{:<3}\t{:<1}{:<7}{:<3}\t{:<1}{:<7}{:<3}\t{:<1}{:<7}{:<5}\t{:<1}{:<3}\t{:<2}{:<4}\t{:<2}{:<4}\t{:<1}{:>5}\t{:<3}\t{:<15}\t{:<2}{:<10}\n".format(
            "$",
            format_currency(increase['current_rent']).ljust(7),
            "   ",
            "$",
            format_currency(increase['guidelinerent']).ljust(7),
            "   ",
            "$",
            agirent.ljust(8),
            "   ",
            "$",
            format_currency(increase['marketrent']).ljust(7),
            "   ",
            "$",
            format_currency(increase['guidelineincrease']),
            "     ",
            "$",
            agiincrease.ljust(3),
            "  ",
            increase['percentage'],
            "  ",
            agipercentage,
            "  ",
            format_unit(increase['unitnumber']),
            "   ",
            tenant_name.ljust(15," "),
            "   ",
            increase['reason']
        )
        message2check = True
        return message2
    except Exception as e:
        logging.info(f"Error processing Special Case Message: {e}")
        return None

async def build_task_update_message(building_name, increase_effective_date, percentage, increases, message2, numberofincreases, totalincrease, ignoredcount):
    """Build the task update message for a specific building."""
    try:
        today = datetime.today().strftime('%Y-%m-%d')
        increase_effective_date_formatted = increase_effective_date.strftime('%B %d, %Y')
        totalincrease = format_currency(totalincrease)

        # Header
        message = f"\nRun Date: {today}\n"
        message += f"Increase Effective Date: {increase_effective_date_formatted}\n"
        message += f"Guideline Increase Rate: {percentage}%\n"
        message += f"Building: {building_name}\n\n"
        message += f"Number of Inceases: {numberofincreases}\n"
        message += f"Total Increase: ${totalincrease}\n\n"

        # Add headers using tabs for alignment
        message += "{:<12}\t{:<12}\t{:<12}\t\t{:<12}\t{:<12}\t{:<12}\t{:<22}\t{:>12}\t{:>12}\t{:>12}\n".format(
            "Current ", "New", "AGI", "Market", "Guideline", "AGI", "Notice and Calculation", "Ignored", "Unit", "Tenant"
        )
        message += "{:<12}\t{:<12}\t{:<12}\t{:<12}\t{:<12}\t{:<12}\t{:<12}\t{:>12}\t{:>12}\t{:>12}\n".format(
            "Rent", "Rent", "Rent", "Rent", "Increase", "Increase", "Percentage", "(\"Y\")", "Number", "Name"
        )
        message += "-" * 200 + "\n"

        # Format the data for each lease, aligning using tabs
        for increase in increases:

            agiincrease = increase['agiincrease']
            agirent = increase['agirent']
            if agiincrease is None:
                agiincrease = "\t"
                agirent = "\t"
                agipercentage = "\t"
                
            else:
                agiincrease = str(format_currency(agiincrease))
                agirent = str(format_currency(agirent))
                agipercentage = increase['calculationpercentage']
                
            
            tenant_name = increase['tenantname'][:15]  # Limit to 30 characters
            message += "{:<1}{:<7}{:<3}\t{:<1}{:<7}{:<3}\t{:<1}{:<7}{:<3}\t{:<1}{:<7}{:<3}\t{:<1}{:<7}{:<5}\t{:<1}{:<3}\t{:<2}{:<4}\t{:<2}{:<4}\t{:<2}{:<1}\t\t{:<1}{:>5}\t{:<3}\t{:<15}\n".format(
                "$",
                format_currency(increase['current_rent']).ljust(7),
                "   ",
                "$",
                format_currency(increase['guidelinerent']).ljust(7),
                "   ",
                "$",
                agirent.ljust(8),
                "   ",
                "$",
                format_currency(increase['marketrent']).ljust(7),
                "   ",
                "$",
                format_currency(increase['guidelineincrease']),
                "     ",
                "$",
                agiincrease.ljust(3),
                "  ",
                increase['percentage'],
                "  ",
                agipercentage,
                "  ",
                increase['ignored'],
                "  ",
                format_unit(increase['unitnumber']),
                "   ",
                tenant_name
            )
            if increase['ignored'] == "Y":
                ignoredcount += 1
                message2 = await messageforcases(percentage, increase, increase_effective_date_formatted, today, tenant_name, message2, building_name)

        
        return message, message2, ignoredcount
    except Exception as e:
            logging.info(f"Error processing Normal Message: {e}")
            return None




async def createuploadjson(buildingjsonfile, headers, increase_effective_date, task_id, account_id, session):
    try:
        urltaskhistory = f"https://api.buildium.com/v1/tasks/{task_id}/history"

        async with session.get(urltaskhistory, headers=headers) as response:
            if response.status != 200:
                logging.info("Error while getting task history", await response.text())
                return
            taskhistorydata = await response.json()
            taskhistoryid = taskhistorydata[0]['Id']

            # Define file path for temp storage
            file_path = '/tmp'  # Use /tmp for cross-platform compatibility
            filename = f"{account_id}_Increase_Notice_Data.json"
            full_file_path = os.path.join(file_path, filename)

            # Write the JSON data to a file in /tmp folder
            with open(full_file_path, 'wb') as file:
                file.write(buildingjsonfile)  # Dump the buildingjsonfile data to the file
                logging.info(f"JSON file written to {full_file_path}")

            # Step 1: Send the file metadata request to Buildium
            url = f"https://api.buildium.com/v1/tasks/{task_id}/history/{taskhistoryid}/files/uploadrequests"
            data = {'FileName': filename}

            try:
                async with session.post(url, json=data, headers=headers) as response:
                    if response.status != 201:
                        logging.info("Error while submitting file metadata:", await response.text())
                        return

                    # Get the response payload with upload instructions
                    payload = await response.json()

                    try:
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

                    except Exception as e:
                        logging.info(f"An error occurred processing Amazon Data: {str(e)}")

                    # Read the file content into memory (prevents the closed file issue)
                    try:
                        with open(full_file_path, 'rb') as file:
                            file_content = file.read()  # Read the file data into memory

                        # Add file content to form data (pass bytes directly)
                        form_data.add_field("file", file_content, filename=filename, content_type='application/json')

                    except Exception as e:
                        logging.info(f"An error occurred adding file to form data: {str(e)}")

                    # Step 2: Upload the file to the S3 bucket (BucketUrl)
                    bucket_url = payload["BucketUrl"]
                    try:
                        async with session.post(bucket_url, data=form_data) as upload_response:
                            if upload_response.status == 204:  # S3 usually responds with 204 No Content for successful uploads
                                logging.info(f"Upload successful for Task {task_id}.")
                            else:
                                logging.info(f"Error Uploading File for Task {task_id}: {upload_response.status} {await upload_response.text()}")

                    except Exception as e:
                        logging.info(f"An error occurred uploading: {str(e)}")

            except Exception as e:
                logging.info(f"An error occurred getting Amazon metadata: {str(e)}")

    except Exception as e:
        logging.info(f"An error occurred: {str(e)}")
    return full_file_path




async def update_task(task_data, increase_summary, increase_effective_date, percentage, headers, buildingjsonfile, account_id, numberofincreases, totalincrease):
    """Update the task in Buildium with the increase information asynchronously."""
    global message2check, message2
    message2check = False  # Reset global variables at the start
    message2 = ""          # Clear the message2 variable
    ingoredcount = 0

    task_id = task_data['Id']
    taskcatid = task_data['Category']['Id']
    assigned_to_user_id = task_data['AssignedToUserId']
    title = f"Increase Notices for {increase_effective_date.strftime('%B %d, %Y')} - Review"

    url = f"https://api.buildium.com/v1/tasks/todorequests/{task_id}"

    logging.info(f"Starting update_task for Task ID: {task_id}")

    async with aiohttp.ClientSession() as session:
        for building_id in increase_summary:
            increases = increase_summary[building_id]['increases']
            
            building_namedata = increases[0]['buildingname']
            building_name = f"Property: {building_namedata}"
            buildingnumberofincrease = increase_summary[building_id]['additionalinfo']['numberofincreases']
            buildingtotalincrease = increase_summary[building_id]['additionalinfo']['totalincrease']

            message, message2, ingoredcount = await build_task_update_message(building_name, increase_effective_date, percentage, increases, message2, buildingnumberofincrease, buildingtotalincrease, ingoredcount)
            payload = {
                "Title": title,
                "AssignedToUserId": assigned_to_user_id,
                "Priority": "High",
                "CategoryId": taskcatid,
                "TaskStatus": "InProgress",
                "TaskId": task_id,
                "Message": message,
                "Date": datetime.utcnow().isoformat() + 'Z'
            }

            async with session.put(url, json=payload, headers=headers) as response:
                if response.status == 200:
                    logging.info(f"Task {task_id} updated successfully for Building ID {building_id}.")
                else:
                    logging.info(f"Failed to update task {task_id} for Building ID {building_id}: {response.status} {await response.text()}")

        if message2check:
            message2 += "\n\n\n"
            message2 += f"Ingored Count: {ingoredcount}"
        else:
            message2 += "No Ignored Leases"
        payload = {
            "Title": title,
            "AssignedToUserId": assigned_to_user_id,
            "Priority": "High",
            "CategoryId": taskcatid,
            "TaskStatus": "InProgress",
            "TaskId": task_id,
            "Message": message2,
            "Date": datetime.utcnow().isoformat() + 'Z'
        }

        async with session.put(url, json=payload, headers=headers) as response2:
            if response2.status == 200:
                logging.info(f"Task {task_id} updated successfully for special cases.")
            else:
                logging.info(f"Failed to update task {task_id} for special cases: {response2.status} {await response2.text()}")
        filepath = await createuploadjson(buildingjsonfile, headers, increase_effective_date, task_id, account_id, session)
        os.remove(filepath)

    return True

