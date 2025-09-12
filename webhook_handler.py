from quart import Quart, request, jsonify
import hmac
import hashlib
import base64
import time
import task_processor
from google.cloud import secretmanager
from google.cloud import firestore
from google.cloud import tasks_v2
from google.api_core.exceptions import NotFound
import logging
import asyncio
import json
import os
from functools import lru_cache

app = Quart(__name__)

# Set up logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s:%(message)s')
logging.getLogger('quart.app').setLevel(logging.DEBUG)
logging.getLogger('quart.serving').setLevel(logging.DEBUG)

# Initialize Google Cloud clients
secret_client = secretmanager.SecretManagerServiceClient()
tasks_client = tasks_v2.CloudTasksClient()

PROJECT_ID = os.environ.get("GCP_PROJECT", "buildium-integration-v1")
QUEUE_LOCATION = os.environ.get("TASK_QUEUE_LOCATION", "us-central1")
QUEUE_NAME = os.environ.get("TASK_QUEUE_NAME", "buildium-webhook")

db = firestore.Client(project=PROJECT_ID)

@lru_cache(maxsize=128)
def get_secret(secret_name):
    """Retrieve the secret key from Google Secret Manager."""
    project_id = "buildium-integration-v1"
    name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    response = secret_client.access_secret_version(name=name)
    secret = response.payload.data.decode("UTF-8")
    return secret


@lru_cache(maxsize=128)
def get_account_info(account_id):
    """Retrieve account information from Firestore based on AccountId."""
    try:
        logging.info(f"Fetching account info for Account ID: {account_id}")
        doc_ref = db.collection('buildium_accounts').document(str(account_id))
        doc = doc_ref.get()  # Fetch the document

        if doc.exists:
            logging.info(f"Account info found for Account ID: {account_id}")
            return doc.to_dict()
        else:
            logging.info(f"No account info found for Account ID: {account_id}")
            return None
    except Exception as e:
        if "PERMISSION_DENIED" in str(e):
            logging.error(
                f"Permission denied when fetching account info for Account ID {account_id}: {e}"
            )
        else:
            logging.error(f"Error fetching account info for Account ID {account_id}: {e}")
        return None




def verify_signature(request_body, signature, timestamp, secret_key):
    """Verify the webhook signature using the secret key."""
    current_timestamp = int(time.time())
    time_diff = abs(current_timestamp - int(timestamp))
    logging.info(f"Time difference: {time_diff} seconds")

    if time_diff > 300:
        logging.error("Request rejected due to timestamp")
        return False

    # Prepare the message to be signed
    message = f'{timestamp}.{request_body}'.encode('utf-8')
    logging.info(f"Message to sign: {message}")

    # Create the HMAC-SHA256 signature using the secret key
    computed_hash = hmac.new(secret_key.encode('utf-8'), message, hashlib.sha256).digest()
    expected_signature = base64.b64encode(computed_hash).decode('utf-8')
    logging.info(f"Expected signature: {expected_signature}")
    logging.info(f"Received signature: {signature}")

    if not hmac.compare_digest(expected_signature, signature):
        logging.error("Signature mismatch!")
        return False

    logging.info("Signature verified successfully.")
    return True

@app.route('/webhook', methods=['POST'])
async def handle_webhook():
    logging.info("Webhook received")
    logging.debug("Request received: method=%s path=%s", request.method, request.path)
    try:
        # Log request headers and body
        signature = request.headers.get('buildium-webhook-signature')
        timestamp = request.headers.get('buildium-webhook-timestamp')
        request_body = await request.get_data(as_text=True)
        logging.info(f"Received signature: {signature}")
        logging.info(f"Received timestamp: {timestamp}")
        logging.info(f"Request body: {request_body}")

        if not signature or not timestamp:
            logging.error("Missing signature or timestamp")
            return jsonify({'error': 'Missing signature or timestamp'}), 400

        payload = await request.get_json()  # Correct usage of request.json
        account_id = payload.get('AccountId')
        logging.info(f"Account ID: {account_id}")

        # Run Firestore lookup in a thread to avoid blocking the event loop
        account_info = await asyncio.to_thread(get_account_info, account_id)
        if not account_info:
            logging.error("Account not found")
            return jsonify({'error': 'Account not found'}), 400

        secret_name = account_info['secret_name']
        # Secret Manager access can also block, so run it in a thread
        secret_key = await asyncio.to_thread(get_secret, secret_name)

        if not verify_signature(request_body, signature, timestamp, secret_key):
            logging.error("Invalid signature")
            return jsonify({'error': 'Invalid signature'}), 403

        task_id = payload.get('TaskId')
        task_type = payload.get('TaskType')
        event_name = payload.get('EventName')
        logging.info(f"Task ID: {task_id}, Task Type: {task_type}, Event Name: {event_name}")

        parent = tasks_client.queue_path(PROJECT_ID, QUEUE_LOCATION, QUEUE_NAME)
        task_payload = {
            'task_id': task_id,
            'task_type': task_type,
            'account_id': account_id,
            'event_name': event_name,
            'account_info': account_info,
        }
        task = {
            'http_request': {
                'http_method': tasks_v2.HttpMethod.POST,
                'url': request.host_url.rstrip('/') + '/tasks/process',
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps(task_payload).encode(),
            }
        }

        try:
            await asyncio.to_thread(
                tasks_client.create_task, request={"parent": parent, "task": task}
            )
            logging.info("Task enqueued to Cloud Tasks")
        except NotFound as e:
            logging.error(
                f"Cloud Tasks queue not found: {e}. "
                "Ensure the queue exists and environment variables are configured correctly."
            )
            return (
                jsonify({"error": "Task queue not found"}),
                500,
            )

        return jsonify({'status': 'success'}), 200
    except Exception as e:
        logging.error(f"Error handling webhook: {e}")
        return jsonify({'error': 'Internal Server Error'}), 500

@app.route('/tasks/process', methods=['POST'])
async def process_task_request():
    queue_header = request.headers.get('X-Cloud-Tasks-QueueName')
    if queue_header != QUEUE_NAME:
        logging.error(f"Invalid Cloud Tasks queue header: {queue_header}")
        return "Forbidden", 403

    payload = await request.get_json()
    await task_processor.process_task(
        payload.get('task_id'),
        payload.get('task_type'),
        payload.get('account_id'),
        payload.get('event_name'),
        payload.get('account_info'),
    )
    return '', 204

@app.route('/', methods=['GET', 'POST'])
async def index():
    return "Buildium Webhook Handler is running!", 200

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8080)
