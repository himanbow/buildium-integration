import requests

def get_task_data(task_id, headers):
    """Retrieve task data from Buildium API."""
    # Replace with the actual Buildium API base URL
    base_url = "https://api.buildium.com/v1"

    # Construct the URL for retrieving a specific task
    url = f"{base_url}/tasks/{task_id}"

    try:
        response = requests.get(url, headers=headers)

        # Check if the request was successful
        if response.status_code == 200:
            print(f"Retrieved task {task_id}: {response.status_code}")
            return response.json()  # Return the task data as a dictionary
        else:
            print(f"Failed to retrieve task {task_id}: {response.status_code} - {response.text}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Error retrieving task {task_id}: {e}")
        return None
