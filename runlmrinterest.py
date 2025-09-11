from datetime import date as _date, timedelta, datetime, UTC
import logging
import aiohttp
from collections import defaultdict

from rate_limiter import semaphore

# ---------------- dates ----------------
async def getdates():
    logging.info("Setting LMR Interest Dates")
    today = _date.today()

    first_day = today.replace(day=1)
    # first day of next month
    if today.month == 12:
        next_month_1 = _date(today.year + 1, 1, 1)
    else:
        next_month_1 = _date(today.year, today.month + 1, 1)
    last_day = next_month_1 - timedelta(days=1)

    month_year = today.strftime("%B %Y")
    days_in_year = (_date(today.year + 1, 1, 1) - _date(today.year, 1, 1)).days
    return first_day, last_day, month_year, days_in_year

# ---------------- leases ----------------
async def get_leases(session: aiohttp.ClientSession, headers: dict):
    """Fetch active Fixed / FixedWithRollover leases with offset pagination."""
    url = "https://api.buildium.com/v1/leases"
    all_leases = []
    offset = 0
    limit = 1000

    while True:
        params = {
            "leasestatuses": "Active",
            "leasetypes": "Fixed,FixedWithRollover",
            "limit": limit,
            "offset": offset,
        }
        async with semaphore:
            async with session.get(url, headers=headers, params=params) as resp:
                if resp.status != 200:
                    txt = await resp.text()
                    logging.error(f"get_leases failed: {resp.status} {txt}")
                    break
                batch = await resp.json()
        if not batch:
            break
        all_leases.extend(batch)
        offset += limit

    logging.info(f"Fetched {len(all_leases)} leases")
    return all_leases

# ---------------- LMR balance per lease ----------------
async def lmrbalance(headers: dict, leases: list, session: aiohttp.ClientSession):
    """
    For each lease, sum LMR-related transactions to compute current LMR balance.
    """
    LMRGLID = 191645
    results = []

    for lease in leases:
        leaseid = lease["Id"]
        propertyid = lease["PropertyId"]
        logging.info(f"Grabbing all transactions for {leaseid}")

        # fetch transactions with pagination (if supported)
        url = f"https://api.buildium.com/v1/leases/{leaseid}/transactions"
        offset = 0
        limit = 1000
        all_tx = []

        while True:
            params = {"limit": limit, "offset": offset}
            async with semaphore:
                async with session.get(url, headers=headers, params=params) as resp:
                    if resp.status != 200:
                        logging.error(f"Tx fetch {leaseid} failed: {resp.status} {await resp.text()}")
                        break
                    batch = await resp.json()
            if not batch:
                break
            all_tx.extend(batch)
            offset += limit

        payment_amount = 0.0
        credit_amount = 0.0
        applied_deposit_amount = 0.0

        for tx in all_tx:
            ttype = tx.get("TransactionType")
            journal = tx.get("Journal") or {}
            memo = (journal.get("Memo") or "").strip()
            for line in (journal.get("Lines") or []):
                gl_id = ((line.get("GLAccount") or {}).get("Id"))
                amount = float(line.get("Amount") or 0.0)

                if gl_id == LMRGLID:
                    if ttype == "Payment":
                        payment_amount -= amount
                    elif ttype == "Credit":
                        credit_amount -= amount
                elif ttype == "Applied Deposit":
                    # exclude the periodic interest application line itself
                    if memo != "Last Month's Rent Interest Applied to Balances":
                        applied_deposit_amount += amount

        current_lmr = round(payment_amount + credit_amount + applied_deposit_amount, 2)
        results.append({"leaseid": leaseid, "lmrbalance": current_lmr, "propertyid": propertyid})
    logging.info("Retrieved LMR Balances")

    return results

# ---------------- interest calc ----------------
async def calculate(lmr_rows: list, percentage: float, date_1: _date, date_2: _date, days_in_year: int):
    """
    lmr_rows: list of {leaseid, lmrbalance, propertyid}
    returns: list of {leaseid, interest, propertyid}
    """
    interestrate = float(percentage) / 100.0
    daily_interest = interestrate / float(days_in_year)

    numberofdays = (date_2 - date_1).days + 1
    out = []

    for row in lmr_rows:
        lmr = float(row.get("lmrbalance") or 0.0)
        if lmr <= 0:
            continue
        interest_total = round(lmr * daily_interest * numberofdays, 2)
        if interest_total > 0:
            out.append({
                "leaseid": row["leaseid"],
                "interest": interest_total,           # <-- use key 'interest'
                "propertyid": row["propertyid"],
            })
    logging.info("Calculated LMR Interest for all leases")
    return out

# ---------------- aggregate per building ----------------
async def reportbuildingtotals(session: aiohttp.ClientSession, interest_and_ids: list, headers: dict):
    """
    Return dict { property_name: total_interest }.
    """
    logging.info("Running Building LMR Interest Breakdown")
    totals_by_property_id = defaultdict(float)
    for row in interest_and_ids:
        totals_by_property_id[row["propertyid"]] += float(row["interest"])

    # resolve names
    result = {}
    for prop_id, total in totals_by_property_id.items():
        url = f"https://api.buildium.com/v1/rentals/{prop_id}"
        async with semaphore:
            async with session.get(url, headers=headers) as resp:
                if resp.status != 200:
                    logging.error(f"rental GET {prop_id} failed: {resp.status} {await resp.text()}")
                    name = f"Property {prop_id}"
                else:
                    data = await resp.json()
                    name = data.get("Name") or f"Property {prop_id}"
        result[name] = round(total, 2)
    logging.info("Completed Building LMR Interest Breakdown")
    return result

# ---------------- task message ----------------
async def _put_task_message(session, task_id: int, headers: dict,
                            title: str, assigned_to_user_id: int, taskcatid: int, msg: str) -> bool:
    logging.info("Updating Task")
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
    async with semaphore:
        async with session.put(url_task, json=payload, headers=headers) as r:
            if r.status == 200:
                return True
            logging.error(f"Task PUT failed: {r.status} {await r.text()}")
            return False

async def updatetask(task_data, headers, session, lmr_report_data: dict, month_label: str):
    try:
        task_id = task_data["Id"]
        taskcatid = task_data["Category"]["Id"]
        assigned_to_user_id = task_data["AssignedToUserId"]
        title = f"Last Month's Interest for {month_label} - Review"

        # simple readable message
        if not lmr_report_data:
            message = "No LMR interest due this month."
        else:
            lines = [f"{name}: ${total:,.2f}" for name, total in sorted(lmr_report_data.items())]
            message = "LMR Interest Totals by Property:\n" + "\n".join(lines)

        status = await _put_task_message(session, task_id, headers, title, assigned_to_user_id, taskcatid, message)
        return status
    except Exception as e:
        logging.exception(f"Error updating task: {e} for LMR Interest")
        return False

# ---------------- orchestrator ----------------
async def lmrinterestprogram(task_data, headers, guideline_percentage: float):
    async with aiohttp.ClientSession() as session:
        first_day, last_day, month_label, days_in_year = await getdates()

        # FIX: correct arg order
        leases = await get_leases(session, headers)

        lmr_and_ids = await lmrbalance(headers, leases, session)

        # FIX: pass date objects directly
        interest_and_ids = await calculate(lmr_and_ids, guideline_percentage, first_day, last_day, days_in_year)

        lmr_report_data = await reportbuildingtotals(session, interest_and_ids, headers)

        taskupdated = await updatetask(task_data, headers, session, lmr_report_data, month_label)
        if taskupdated:
            logging.info(f"LMR Interest Task {task_data['Id']} updated.")



