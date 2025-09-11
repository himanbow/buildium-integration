import aiohttp
import asyncio
from datetime import datetime, timedelta
from collections import defaultdict
import logging
import re

from rate_limiter import semaphore

building_notes_cache = {}

async def fetch_data(session, url, headers):
    """Fetch data asynchronously with rate limiting and semaphore control."""
    logging.info("Fetching Data From Buildium")
    try:
        # Limit concurrent requests using semaphore
        async with semaphore:
            while True:
                async with session.get(url, headers=headers) as response:
                    status_code = response.status
                    data = await response.json()

                    if status_code == 429:
                        logging.info("Rate limit reached, sleeping for 0.201 seconds")
                        await asyncio.sleep(0.201)  # Rate limit sleep for 429 status
                        continue  # Retry the request after sleeping

                    # Handle both dict and list responses
                    if isinstance(data, (dict, list)):
                        return data  # Return data if it's a dict or list
                    else:
                        logging.info(f"Unexpected response format: {data}")
                        return {}  # Return an empty dict if the format is invalid

    except Exception as e:
        logging.info(f"Error fetching data from {url}: {e}")
        return {}



async def get_building_notes(session, building_id, headers):
    """Retrieve notes for a building to check for AGI status asynchronously, with caching."""
    # Check if the building notes are already in the cache
    if building_id in building_notes_cache:
        logging.info(f"Using cached notes for building {building_id}")
        return building_notes_cache[building_id]

    # If not cached, fetch the notes from the API
    url = f"https://api.buildium.com/v1/rentals/{building_id}/notes"
    notes = await fetch_data(session, url, headers)

    # Cache the result to avoid redundant API calls
    building_notes_cache[building_id] = notes

    return notes


# Example of how to handle the response in other functions
async def get_lease_notes(session, lease_id, headers):
    """Retrieve notes for a specific lease asynchronously."""
    url = f"https://api.buildium.com/v1/leases/{lease_id}/notes"
    notes = await fetch_data(session, url, headers)
    if not notes:  # Handle empty or invalid responses
        logging.info(f"No notes found for lease {lease_id}")
    return notes

async def get_unit_details(session, unit_id, headers):
    """Retrieve details of the rental unit, including market rent asynchronously."""
    url = f"https://api.buildium.com/v1/rentals/units/{unit_id}"
    return await fetch_data(session, url, headers)

async def get_leases(session, headers, increase_effective_date):
    """Fetch leases asynchronously with pagination using offset."""
    url = "https://api.buildium.com/v1/leases"
    all_leases = []
    offset = 0
    limit = 1000

    while True:
        params = {
            'leasestatuses': "Active",
            'leasedateto': increase_effective_date.strftime('%Y-%m-%d'),
            'limit': limit,
            'offset': offset,
        }
        
        async with semaphore:
            async with session.get(url, headers=headers, params=params) as response:
                leases = await response.json()
                if not leases:
                    break

                all_leases.extend(leases)
                offset += limit

    logging.info(f"Fetched {len(all_leases)} leases")
    return all_leases

def parse_date(date_str):
    """Parse date strings into datetime objects, handling potential errors."""
    # Check if the input is already a datetime object
    if isinstance(date_str, datetime):
        return date_str
    
    try:
        # Attempt to parse the date assuming DD/MM/YYYY format
        return datetime.strptime(date_str, "%d/%m/%Y")
    except ValueError:
        # Handle invalid date formats
        logging.error(f"Warning: Unable to parse date '{date_str}'")
        return None

def parse_building_agi_notes(note_dict):
    """Parse AGI notes for a building and extract relevant details."""
    agi_info = []

    if isinstance(note_dict, list):
        for note_item in note_dict:
            note = note_item.get('Note', '')
            if note:
                lines = note.strip().splitlines()
                agi_data = {
                    'approval_status': '',
                    'date_of_completion': None,
                    'date_of_first_increase': None,
                    'yearly_increases': []  # List to hold all year-specific increases
                }

                for line in lines:
                    line = line.strip()
                    if line.startswith("AGI:"):
                        # Extract AGI approval status
                        agi_data['approval_status'] = line.split(":")[1].strip()
                    
                    elif line.startswith("Date of Completion:"):
                        # Extract date of completion
                        date_str = line.split(":")[1].strip()
                        agi_data['date_of_completion'] = parse_date(date_str)
                    
                    elif line.startswith("Date of First Increase:"):
                        # Extract date of the first increase
                        date_str = line.split(":")[1].strip()
                        agi_data['date_of_first_increase'] = parse_date(date_str)
                    
                    # Regex to match any year-specific increase like "First Year Increase: 3%" or "Fourth Year Increase: 1.5%"
                    elif re.match(r"^\w+ Year Increase:", line):
                        # Extract the increase percentage
                        percentage_str = line.split(":")[1].strip().replace('%', '')
                        increase_percentage = float(percentage_str)
                        agi_data['yearly_increases'].append(increase_percentage)

                # Check if agi_data has relevant data before appending
                if agi_data['approval_status'] or agi_data['date_of_completion'] or agi_data['date_of_first_increase'] or agi_data['yearly_increases']:
                    agi_info.append(agi_data)

 
    logging.info("Building AGI Information Parsed")


    return agi_info

def parse_lease_agi_notes(notes):
    """Parse lease AGI notes and return a list of AGI years."""
    agi_years = []
    Noincrease = False
    for note in notes:
        note_content = note.get('Note', '')
        if note_content.startswith("AGI"):
            try:
                agi_year = int(note_content.split()[1])
                agi_years.append(agi_year)
            except (IndexError, ValueError):
                logging.error(f"Error parsing AGI year from note: {note_content}")
        if note_content.startswith("No AGI"):
            Noincrease = True

    return agi_years, Noincrease

async def getrecurringcharges(leaseid, session, headers):
    try:
        """Retrieve details of the recurring charges asynchronously."""
        url = f"https://api.buildium.com/v1/leases/{leaseid}/recurringtransactions"
        return await fetch_data(session, url, headers)
    except Exception as e:
            logging.error(f"Error fetching recurring transactions: {e}")
            return None

async def processrecurringcharges(recurringchargesinfo):
    try:
        total = 0
        chargeslist = []
        for charge in recurringchargesinfo:
            type = charge['TransactionType']
            frequency = charge['Frequency']
            duration = charge['Duration']
            if type == "Charge" and frequency == "Monthly" and duration == "UntilEndOfTerm":
                amount = float(charge['Amount'])
                total += amount

                charge_info = {
                    'Id': charge['Id'],
                    'Amount': charge['Amount'],
                    'Gl' : charge['Lines'][0]['GLAccountId'],
                    'PostDaysInAdvance': charge['PostDaysInAdvance'],
                    'Memo': charge['Memo'],
                    'RentId' : charge['RentId']
                    }

                chargeslist.append(charge_info)
        return chargeslist, total
    except Exception as e:
        logging.error(f"Error processing recurring transactions: {e}")
        return None, None

def calculate_total_increase(building_agi_info, guideline_increase, lease_agi_info, increase_effective_date):
    """Calculate total increase percentage label for a lease, considering both guideline and AGI increases."""
    total_percentage = float(guideline_increase)
    calculationpercentage = float(guideline_increase)  # Start with the guideline rate for notice_percentage
    increase_effective_date = parse_date(increase_effective_date)  # Convert the effective date to datetime

    cumulative_agi_percentage = 0  # To track cumulative AGI percentages up to the current year

    for agi in building_agi_info:
        first_increase_date = parse_date(agi.get('date_of_first_increase'))
        yearly_increases = agi.get('yearly_increases', [])  # Get the list of yearly increases

        if first_increase_date and yearly_increases:
            # Calculate the number of years between the first increase date and the effective date
            years_difference = (increase_effective_date.year - first_increase_date.year) - \
                               ((increase_effective_date.month, increase_effective_date.day) < (first_increase_date.month, first_increase_date.day))

            # Correctly accumulate the AGI percentage for each applicable year
            cumulative_agi_percentage = sum(yearly_increases[:min(years_difference + 1, len(yearly_increases))])

            if 0 <= years_difference < len(yearly_increases):
                # Get the AGI percentage for the current year
                current_year_agi_percentage = yearly_increases[years_difference]
                total_percentage += current_year_agi_percentage
                # Add the cumulative AGI and guideline increase to get the calculation percentage
            calculationpercentage = round(cumulative_agi_percentage + float(guideline_increase),2)

            # Ensure the multi-year AGI adjustment is added correctly
            if years_difference > 0:
                calculationpercentage += 0.25

    return total_percentage, calculationpercentage

async def process_single_lease(session, lease, headers, increase_effective_date, guideline_increase, building_agi_info):
    """Process a single lease asynchronously, checking eligibility and fetching required details."""
    eligible = True
    reason = ""
    AGItype = None
    logging.info(f"Processing Lease Id: {lease['Id']}")
    try:
        lease_end_date = datetime.strptime(lease['LeaseToDate'], '%Y-%m-%d')

        if lease_end_date <= increase_effective_date - timedelta(days=1) and lease['AccountDetails']['Rent'] > 0:
            notes = await get_lease_notes(session, lease['Id'], headers)
            logging.info(f"Processing Notes for Lease Id: {lease['Id']}")
            calculationpercentage = guideline_increase
            lease_agi_info, Noincrease = parse_lease_agi_notes(notes)
            try:
                if lease_agi_info:
                    total_increase_percentage, calculationpercentage = calculate_total_increase(building_agi_info, guideline_increase, lease_agi_info, increase_effective_date)
                    agi = "Yes"
                    if any(agi_info['approval_status'] == "Not Approved" for agi_info in building_agi_info):
                        AGItype = "Not Approved"
                    else:
                        AGItype = "Approved"
                else:
                    total_increase_percentage = guideline_increase
                    agi = None
            except Exception as e:
                logging.error(f"Error processing AGI: {e}")
            logging.info(f"Processing Unit for Lease Id: {lease['Id']}")
            unit_details = await get_unit_details(session, lease['UnitId'], headers)

            try:
                market_rent = unit_details['MarketRent']
            except Exception as e:
                logging.error(f"Error processing marketrent {e}")
            recurringchargesinfo = await getrecurringcharges(lease['Id'], session, headers)
            recurringcharges, rent = await processrecurringcharges(recurringchargesinfo)
            

            try:
                if Noincrease is True:
                    eligible = False
                    reason = "No Increase Note"
                elif lease['MoveOutData'] != [] and len(lease["Tenants"]) == len(lease['MoveOutData']):
                    try:
                        eligible = False
                        datetest = "2019-08-24"
                        datemove = ""  # Initialize datemove with a default value
                        for date in lease['MoveOutData']:
                            if date['MoveOutDate'] > datetest:
                                datetest = date['MoveOutDate']
                            else:
                                datemove = date['MoveOutDate']
                        reason = f"Moving Out {datemove}" if datemove else f"Moving Out {datetest}"
                    except Exception as e:
                        logging.error(f"Error processing moveout data {e}")
                else:
                    eligible = True       
                tenant_namesdata = []
                tenantidslist = []
            except Exception as e:
                logging.error(f"Error processing eligibility: {e}")
            # eligible = rent <= market_rent or bool(lease_agi_info) if market_rent != 0 else True
            logging.info(f" Finished Processing Lease Id: {lease['Id']}")
            for tenant in lease['CurrentTenants']:
                tenant_namesdata.append(f"{tenant['FirstName']} {tenant['LastName']}")
                tenant_names = str(tenant_namesdata).removeprefix("['").removesuffix("']").replace("'","")
                tenantids = tenant['Id']
                tenantidslist.append(tenantids)



            tenant_address_line1 = lease['CurrentTenants'][0]['Address']['AddressLine1']
            tenant_address_city = lease['CurrentTenants'][0]['Address']['City']
            tenant_address_state = lease['CurrentTenants'][0]['Address']['State']
            tenant_address_postalcode = lease['CurrentTenants'][0]['Address']['PostalCode']
            address = f"{tenant_address_line1}, {tenant_address_city}, {tenant_address_state} {tenant_address_postalcode}"

            return {
                'leaseid': lease['Id'],
                'buildingid': unit_details['PropertyId'],
                'buildingname' : unit_details['BuildingName'],
                'unitnumber': unit_details['UnitNumber'],
                'address' : address,
                'tenantname': lease['CurrentTenants'][0]['FirstName'] + ' ' + lease['CurrentTenants'][0]['LastName'],
                'alltenantnames' : tenant_names,
                'tenantids' : tenantidslist,
                'rent': rent,
                'recurringinfo' : recurringcharges,
                'marketrent': market_rent,
                'eligible': eligible,
                'total_increase_percentage': total_increase_percentage,
                'agi': agi,
                'agiinfo' : building_agi_info,
                'agitype' : AGItype,
                'reason' : reason,
                'calculationpercentage' : calculationpercentage
            }
        else:
            return None

    except Exception as e:
        logging.error(f"Error processing lease {lease['Id']}: {e}")
        return None

async def gather_leases_for_increase(headers, guideline_increase):
    """Main function to gather and process leases asynchronously."""
    today = datetime.today()
    buildingidnotetest = 0
    building_agi_info = {}
    effective_date = datetime(today.year, today.month, 1) + timedelta(days=125)
    effective_date = datetime(effective_date.year, effective_date.month, 1)
    increase_effective_date = effective_date
    guideline_increase = float(guideline_increase)

    async with aiohttp.ClientSession() as session:
        leases = await get_leases(session, headers, increase_effective_date)
        logging.info("Leases Fetched")

        leases_by_building = defaultdict(list)

        if leases:
            tasks = []
            for lease in leases:
                building_id = lease['PropertyId']
                if building_id != buildingidnotetest:
                    building_notes = await get_building_notes(session, building_id, headers)
                    building_agi_info = parse_building_agi_notes(building_notes)
                    buildingidnotetest = building_id
                
                tasks.append(process_single_lease(session, lease, headers, increase_effective_date, guideline_increase, building_agi_info))
            
            lease_results = await asyncio.gather(*tasks)

            for result in lease_results:
                if result:
                    leases_by_building[result['buildingid']].append(result)
            leases_by_building = {k: leases_by_building[k] for k in sorted(leases_by_building, reverse=True)}

        return leases_by_building, increase_effective_date


# # recurringchargesinfo = [{"Id":295957,"TransactionType":"Charge","IsExpired":False,"RentId":122220,"OffsettingGLAccountId":None,"Lines":[{"GLAccountId":3,"Amount":1633.99}],"Amount":1633.99,"Memo":"Rent","NextOccurrenceDate":"2024-11-01","PostDaysInAdvance":11,"Frequency":"Monthly","Duration":"UntilEndOfTerm"},{"Id":295958,"TransactionType":"Charge","IsExpired":False,"RentId":None,"OffsettingGLAccountId":None,"Lines":[{"GLAccountId":144077,"Amount":69.11}],"Amount":69.11,"Memo":"Parking","NextOccurrenceDate":"2024-11-01","PostDaysInAdvance":11,"Frequency":"Monthly","Duration":"UntilEndOfTerm"},{"Id":295959,"TransactionType":"Charge","IsExpired":False,"RentId":None,"OffsettingGLAccountId":None,"Lines":[{"GLAccountId":144073,"Amount":57.81}],"Amount":57.81,"Memo":"Garage Parking","NextOccurrenceDate":"2024-11-01","PostDaysInAdvance":11,"Frequency":"Monthly","Duration":"UntilEndOfTerm"}]

# # data, rent = asyncio.run(processrecurringcharges(recurringchargesinfo))

# # print(data)
