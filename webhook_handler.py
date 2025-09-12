"""Quart application that validates Buildium webhooks and enqueues tasks.

This module exposes routes for receiving webhook events and for Cloud Tasks to
dispatch work. Adding clear docstrings here helps future contributors navigate
the overall flow of the application.
"""

from quart import Quart, request, jsonify
import hmac
import hashlib
import base64
import time
import task_processor
from google.cloud.secretmanager_v1 import SecretManagerServiceAsyncClient
from google.cloud.firestore_v1 import AsyncClient as FirestoreAsyncClient
from google.cloud import tasks_v2
from google.api_core.exceptions import NotFound
import logging
import asyncio
import json
import os
from session_manager import session_manager

app = Quart(__name__)

# Set up logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s:%(message)s')
logging.getLogger('quart.app').setLevel(logging.DEBUG)
logging.getLogger('quart.serving').setLevel(logging.DEBUG)

PROJECT_ID = os.environ.get("GCP_PROJECT", "buildium-integration-v1")
QUEUE_LOCATION = os.environ.get("TASK_QUEUE_LOCATION", "us-central1")
QUEUE_NAME = os.environ.get("TASK_QUEUE_NAME", "Worker")


@app.before_serving
async def create_clients():
    """Instantiate Google Cloud clients before handling requests."""
    app.secret_client = SecretManagerServiceAsyncClient()
    app.tasks_client = tasks_v2.CloudTasksAsyncClient()
    app.db = FirestoreAsyncClient(project=PROJECT_ID)

_secret_cache = {}
async def get_secret(secret_name):
    """Retrieve the secret key from Google Secret Manager."""
    if secret_name in _secret_cache:
        return _secret_cache[secret_name]
    name = f"projects/{PROJECT_ID}/secrets/{secret_name}/versions/latest"
    response = await app.secret_client.access_secret_version(request={"name": name})
    secret = response.payload.data.decode("UTF-8")
    _secret_cache[secret_name] = secret
    return secret


_account_info_cache = {}
async def get_account_info(account_id):
    """Retrieve account information from Firestore based on AccountId."""
    if account_id in _account_info_cache:
        return _account_info_cache[account_id]
    try:
        logging.info(f"Fetching account info for Account ID: {account_id}")
        doc_ref = app.db.collection('buildium_accounts').document(str(account_id))
        doc = await doc_ref.get()
        if doc.exists:
            logging.info(f"Account info found for Account ID: {account_id}")
            data = doc.to_dict()
            _account_info_cache[account_id] = data
            return data
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
    """Validate webhook payload, verify signature, and enqueue task."""
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

        account_info_task = asyncio.create_task(get_account_info(account_id))

        async def secret_lookup():
            info = await account_info_task
            if not info:
                raise ValueError("Account not found")
            return await get_secret(info['secret_name'])

        secret_task = asyncio.create_task(secret_lookup())
        try:
            account_info, secret_key = await asyncio.gather(account_info_task, secret_task)
        except Exception as e:
            for t in (account_info_task, secret_task):
                if not t.done():
                    t.cancel()
            logging.error(f"Error retrieving account info or secret: {e}")
            return jsonify({'error': 'Account lookup failed'}), 400

        if not account_info:
            logging.error("Account not found")
            return jsonify({'error': 'Account not found'}), 400

        if not verify_signature(request_body, signature, timestamp, secret_key):
            logging.error("Invalid signature")
            return jsonify({'error': 'Invalid signature'}), 403

        task_id = payload.get('TaskId')
        task_type = payload.get('TaskType')
        event_name = payload.get('EventName')
        logging.info(f"Task ID: {task_id}, Task Type: {task_type}, Event Name: {event_name}")

        parent = app.tasks_client.queue_path(PROJECT_ID, QUEUE_LOCATION, QUEUE_NAME)
        task_payload = {
            'task_id': task_id,
            'task_type': task_type,
            'account_id': account_id,
            'event_name': event_name,
            'account_info': account_info,
        }
        base_url = request.url_root.replace("http://", "https://").rstrip("/")
        task = {
            "http_request": {
                "http_method": tasks_v2.HttpMethod.POST,
                "url": f"{base_url}/tasks/process",
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps(task_payload).encode(),
            }
        }
        try:
            await app.tasks_client.create_task(request={"parent": parent, "task": task})
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
    """Handle Cloud Tasks callbacks by delegating work to task_processor."""
    # Cloud Tasks may include different queue name headers depending on the
    # environment (Cloud Run vs. App Engine). Check all known variants.
    queue_header = (
        request.headers.get("X-Cloud-Tasks-QueueName")
        or request.headers.get("X-CloudTasks-QueueName")
        or request.headers.get("X-AppEngine-QueueName")
    )
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
    """Simple health check endpoint for Cloud Run."""
    return "Buildium Webhook Handler is running!", 200


@app.after_serving
async def close_clients():
    """Close Google Cloud clients when the app stops."""
    await app.secret_client.transport.close()
    await app.tasks_client.close()
    await app.db.close()


@app.after_serving
async def shutdown_session_manager():
    """Ensure all aiohttp sessions are closed when the app stops."""
    await session_manager.close_all()

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8080)
