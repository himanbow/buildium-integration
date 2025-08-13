import get_tasks
import get_eligible_leases
import calculate_increase
import update_task_for_approval
import asyncio
from google.cloud import secretmanager, firestore
import logging
import build_increase_json
import decodefile
import processincreaseinfo




# Initialize Google Cloud clients
secret_client = secretmanager.SecretManagerServiceClient()
db = firestore.Client(project="buildium-integration-v1")

# Set up basic logging
logging.basicConfig(level=logging.INFO)

def get_secret(secret_name):
    """Retrieve the API secret from Google Secret Manager."""
    logging.info(f"Retrieving secret for {secret_name}")
    project_id = "buildium-integration-v1"
    name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    response = secret_client.access_secret_version(name=name)
    secret = response.payload.data.decode("UTF-8")
    return secret

# def get_account_info(account_id):
#     """Retrieve the API client ID and API secret name from Firestore based on AccountId."""
#     logging.info(f"Fetching account info for AccountId: {account_id}")
#     doc_ref = db.collection('buildium_accounts').document(str(account_id))
#     doc = doc_ref.get()
#     if doc.exists():
#         return doc.to_dict()
#     else:
#         logging.error(f"Account information not found for AccountId: {account_id}")
#         return None

async def process_task(task_id, task_type, account_id, event_name, account_info):
    """Process the task based on the task type and category."""
    logging.info(f"Processing Task: {task_id}, Task Type: {task_type}, Event: {event_name}")

    if not account_info:
        logging.error(f"Account information not found for AccountId: {account_id}")
        return

    client_id = account_info['api_client_id']
    secret_name = account_info['api_secret_name']
    client_secret = get_secret(secret_name)
    guideline_percentage = account_info['guideline_increase']

    # Prepare headers for API requests as expected by Buildium API
    headers = {
        'x-buildium-client-id': client_id,
        'x-buildium-client-secret': client_secret,
        'Content-Type': 'application/json'
    }

    logging.info(f"Retrieved headers for Task: {task_id}")

    # Retrieve task data from get_tasks.py (assuming get_task_data is synchronous)
    task_data = get_tasks.get_task_data(task_id, headers)

    if not task_data:
        logging.error(f"Task data not found for TaskId: {task_id}")
        return

    if task_data['Category']['Name'] != "System Tasks":
        logging.info(f"Task {task_id} is not a System Task. Stopping processing.")
        return

    task_title = task_data['Title']
    logging.info(f"Task title: {task_title}")

    # Process the task based on event type and title
    if event_name == 'Task.Created':
        if "Increase Notices" in task_title:
            await process_increase_notices(task_data, headers, guideline_percentage, client_secret, account_id)
        elif "Increase Letters" in task_title:
            await process_increase_letters(task_data, headers)
        elif "LMR Interest" in task_title:
            await process_lmr_interest(task_data, headers)
        else:
            logging.error(f"Unknown task type for TaskId: {task_id}")
    elif event_name == 'Task.History.Created':
        logging.info(f"Task {task_id} History Update Detected")
        if "Increase Notices" in task_title:
            await process_generate_notices(task_data, headers, guideline_percentage, client_secret, account_id)

async def process_increase_notices(task_data, headers, guideline_percentage, client_secret, account_id):
    """Handle Increase Notices task."""
    logging.info("Processing Increase Notices")

    # Call gather_leases_for_increase asynchronously
    try:
        leases_by_building, increase_effective_date = await get_eligible_leases.gather_leases_for_increase(headers, guideline_percentage)
        logging.info(f"Fetched leases for increase.")
    except Exception as e:
        logging.error(f"Error fetching leases: {e}")
        return

    if not leases_by_building:
        logging.info("No eligible leases found for rent increases.")
        return

    # Calculate the increases (assuming generate_increases is synchronous)
    try:
        increase_summary, numberofincreases, totalincrease = calculate_increase.generate_increases(leases_by_building, increase_effective_date, guideline_percentage)
    except Exception as e:
        logging.error(f"Error calculating increases: {e}")
    logging.info(f"Increase summary created")
    try:
        buildingjsonfile = await build_increase_json.buildincreasejson(increase_summary, increase_effective_date, client_secret)
    except Exception as e:
        logging.error(f"Error building Json File {e}")
    # Update the task with the increase summary
    try:
        await update_task_for_approval.update_task(
            task_data,
            increase_summary,
            increase_effective_date,
            guideline_percentage,
            headers,
            account_id,
            buildingjsonfile,
            logo_source="https://assets.rentsync.com/mantler_management/images/logos/1645623885805_mantler-01.png" #optional
        )
        logging.info("Task updated with increase summary.")
    except Exception as e:
        logging.error(f"Error updating task: {e}")

async def process_increase_letters(task_data, headers):
    """Handle Increase Letters task."""
    logging.info("Processing Increase Letters")

async def process_lmr_interest(task_data, headers):
    """Handle LMR Interest task."""
    logging.info("Processing LMR Interest")

async def process_generate_notices(task_data, headers, guideline_percentage, client_secret, account_id):
    if task_data['TaskStatus'] == "Completed":
        logging.info("Processing Generation of Increase Notices")
        increaseinfo = await decodefile.decode(headers, task_data, client_secret)
        if not increaseinfo:
            logging.error("decode() returned no data; aborting this task.")
            return
        await processincreaseinfo.process(headers, increaseinfo, account_id)

    else:
        logging.info("Task Update Not a Completed Task")
