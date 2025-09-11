import aiohttp


async def get_task_data(task_id, headers):
    """Retrieve task data from Buildium API asynchronously."""
    # Replace with the actual Buildium API base URL
    base_url = "https://api.buildium.com/v1"

    # Construct the URL for retrieving a specific task
    url = f"{base_url}/tasks/{task_id}"

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    print(f"Retrieved task {task_id}: {response.status}")
                    return await response.json()
                else:
                    text = await response.text()
                    print(f"Failed to retrieve task {task_id}: {response.status} - {text}")
                    return None
    except aiohttp.ClientError as e:
        print(f"Error retrieving task {task_id}: {e}")
        return None
