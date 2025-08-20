from datetime import date, timedelta, datetime, UTC
import logging
import aiohttp
from collections import defaultdict


async def getdates():
    # Get today's date
    logging.info("Setting LMR Interest Dates")
    today = date.today()

    # First day of the current month
    first_day = today.replace(day=1)

    # Find the last day of the month
    if today.month == 12:  # December case → next year
        next_month = today.replace(year=today.year + 1, month=1, day=1)
    else:
        next_month = today.replace(month=today.month + 1, day=1)

    last_day = next_month - timedelta(days=1)

    # Month and year in full (e.g., "August 2025")
    month_year = today.strftime("%B %Y")

    days_in_year = (date(today.year + 1, 1, 1) - date(today.year, 1, 1)).days

    return(first_day, last_day, month_year, days_in_year)

        
async def get_leases(session, headers):
    """Fetch leases asynchronously with pagination using offset."""
    url = "https://api.buildium.com/v1/leases"
    all_leases = []
    offset = 0
    limit = 1000

    while True:
        params = {
            'leasestatuses': "Active",
            'leasetypes' : "Fixed, FixedWithRollover",
            'limit': limit,
            'offset': offset,
        }
        
        async with session.get(url, headers=headers, params=params) as response:
            leases = await response.json()
            if not leases:
                break

            all_leases.extend(leases)
            offset += limit

        

    logging.info(f"Fetched {len(all_leases)} leases")
    return all_leases

async def lmrbalance(headers, leases, session):

    idsandlmrs = []
    LMRGLID = 191645
    leaseurl = "https://api.buildium.com/v1/leases/"
    transactionsportion = "/transactions"
    all_lmrs = []
    offset = 0
    limit = 1000

    params = {
        'limit' : limit
    }

    for item in leases:
        leaseid = item['Id']
        propertyid = item['PropertyId']
        

        leaseidlink = str(leaseid)
        lmrbalanceurl = leaseurl + leaseidlink + transactionsportion
        while True:
            async with session.get(lmrbalanceurl, headers=headers, params=params) as response:
                data = await response.json()
            if not data:
                break

            all_lmrs.extend(data)
            offset += limit
            

        currentLMRBalance = 0
        payment_amount = 0.0
        credit_amount = 0.0
        applied_deposit_amount = 0.0
        amount = 0
        for item in all_lmrs:
            print(item['TransactionType'])
            transaction_type = item['TransactionType']
            for line in item['Journal']['Lines']:
                gl_account_id = line['GLAccount']['Id']
                amount = line['Amount']
                
                # Check if the GLAccount ID matches the target GLAccount ID
                if gl_account_id == LMRGLID:
                    if transaction_type == 'Payment':
                        payment_amount -= amount
                    elif transaction_type == 'Credit':
                        credit_amount -= amount
                elif transaction_type == 'Applied Deposit':
                    if item['Journal']['Memo'] != "Last Month's Rent Interest Applied to Balances":
                        applied_deposit_amount += amount  # Subtract for 'Applied Deposit'

        currentLMRBalance = payment_amount + credit_amount + applied_deposit_amount
        currentLMRBalance = round(currentLMRBalance,2)
        

        label1 = "leaseid"
        label2 = "lmrbalance"
        label3 = "propertyid"
        
        entry = {label1: leaseid, label2: currentLMRBalance, label3: propertyid}
        idsandlmrs.append(entry)

    return idsandlmrs

async def calculate(lmrbalance, percentage, date_1, date_2, days_in_year):
    label1 = "leaseid"
    label2 = "interest"
    label3 = "propertyid"
    idsandinterest = []

    ### Calculate daily interest
    interestrate = float(percentage) / 100
    daily_interest = interestrate / days_in_year

    ### Calculate number of days
    date_1 = datetime.datetime.strptime(date_1, "%Y-%m-%d")
    date_2 = datetime.datetime.strptime(date_2, "%Y-%m-%d")
    difference = date_2 - date_1
    numberofdays = difference.days + 1


    for entry in lmrbalance:
        leaseid = 0
        lmr = 0
        leaseid = entry.get("leaseid")
        lmr = entry.get("lmrbalance")
        propertyid = entry.get("propertyid")

        interestoweddaily = lmr * daily_interest
        interestowedtotal = round(interestoweddaily * numberofdays,2)

        if interestowedtotal > 0:
            listentry = {label1: leaseid, label2: interestowedtotal, label3: propertyid}

            idsandinterest.append(listentry)
        


    return idsandinterest


async def reportbuildingtotals(session, interest_and_ids, headers):
    # Dictionary to accumulate totals
    totals_by_property = defaultdict(float)
    totals_by_property_report = defaultdict(float)
    url = "https://api.buildium.com/v1/rentals/"

    # Loop through entries and sum by propertyid
    for entry in interest_and_ids:
        prop_id = entry["propertyid"]
        interest = entry["interestowedtotal"]
        totals_by_property[prop_id] += interest

    for entry in totals_by_property:
        prop_id = entry["propertyid"]
        url_full = url + prop_id
        async with session.get(url_full, headers=headers) as response:
            data = await response.json()
            prop_name = data['Name']

        interest = entry["interestowedtotal"]
        totals_by_property[prop_name] += interest

    
    return totals_by_property_report

async def _put_task_message(session, task_id: int, headers: dict,
                            title: str, assigned_to_user_id: int, taskcatid: int, msg: str) -> bool:
    url_task = f"https://api.buildium.com/v1/tasks/todorequests/{task_id}"
    payload = {
        "Title": title,
        "AssignedToUserId": assigned_to_user_id,
        "Priority": "High",
        "CategoryId": taskcatid,
        "TaskStatus": "InProgress",
        "TaskId": task_id,
        "Message": msg,
        "Date": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
    }
    async with session.put(url_task, json=payload, headers=headers) as r:
        if r.status == 200:
            return True
        logging.error(f"Task PUT failed: {r.status} {await r.text()}")
        return False

async def updatetask(task_data, headers, session, lmr_report_data, month_label):
    try:
        task_id = task_data["Id"]
        taskcatid = task_data["Category"]["Id"]
        assigned_to_user_id = task_data["AssignedToUserId"]
        title = f"Last Month's Interest for {month_label} - Review"
       
        # Add formating for the task message
        # msg = ""
        # for item in lmr_report_data:
        message = str(lmr_report_data)

        status = await _put_task_message(session, task_id, headers, title, assigned_to_user_id, taskcatid, message)
        return status


    except Exception as e:
        logging.exception(f"Error updating task: {e} for LMR Interest")
        return False



async def lmrinterestprogram(task_data, headers, guideline_percentage):
    ## Set first and last dates for interest calculation
    async with aiohttp.ClientSession() as session:
        first_day, last_day, month_label, days_in_year = await getdates()

        leases = await get_leases(headers, session)
        lmr_and_ids = await lmrbalance(headers, leases, session)
        interest_and_ids = await calculate(lmr_and_ids, guideline_percentage, first_day, last_day, days_in_year)
        lmr_report_data = await reportbuildingtotals(session, interest_and_ids, headers)
        taskupdated = await updatetask(task_data, headers, session, lmr_report_data, month_label)
        if taskupdated is True:
            logging.info(f"LMR Interest Task {task_data} updated.")




