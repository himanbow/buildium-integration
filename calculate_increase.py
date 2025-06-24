from datetime import datetime
from collections import defaultdict
import logging
from dateutil.relativedelta import relativedelta

def calculate_rent_increase(amount, percentage):
    """Calculate the new rent based on the percentage increase, rounded to the nearest cent."""
    return round(amount * (1 + percentage / 100), 2)

def format_currency(value):
    """Format the value to two decimal places for currency display."""
    return '{:,.2f}'.format(value)

def processcharges(recurringinfo, percentage, increasedate, agicheck, secondpercentage):
    newcharge_info = []
    chargestostoplist = []
    total_current_rent = 0
    total_current_other = 0
    increasedate = increasedate.strftime('%Y-%m-%d')
    new_total_other = 0
    percentage = float(percentage)

    # First pass to calculate the totals for rent (GL 3) and other charges
    for item in recurringinfo:
        if item['Gl'] != 3:
            total_current_other += item['Amount']
        total_current_rent += item['Amount']

    # Calculate the total new amount using the percentage increase
    new_total_rent = calculate_rent_increase(total_current_rent, percentage)

    # Track rounding difference
    rent_difference = new_total_rent

    # Second pass to calculate the new individual charges, except rent (GL 3)
    for item in recurringinfo:
        newamount = calculate_rent_increase(item['Amount'], percentage)

        if item['Gl'] != 3:
            rent_difference -= newamount  # Adjust the difference for "other" charges
            if item['RentId'] is None:
                chargestostoplist.append(item['Id'])  # Append only the ID
            newcharge_data = {
                'Amount': newamount,
                'GlAccountId': item['Gl'],
                'NextDueDate': increasedate,
                'Memo': item['Memo'],
            }
            newcharge_info.append(newcharge_data)
            new_total_other += newamount

    # Finally, process the rent charge (GL 3) last to balance out the difference
    for item in recurringinfo:
        if item['Gl'] == 3:
            newamount = calculate_rent_increase(item['Amount'], percentage)
            rent_difference -= newamount  # Adjust the difference for rent charges

            # Adjust the last rent charge by the remaining difference (rent_difference)
            adjusted_rent = round(newamount + round(rent_difference, 2), 2)

            newcharge_data = {
                'Amount': adjusted_rent,  # Use the adjusted amount
                'GlAccountId': item['Gl'],
                'NextDueDate': increasedate,
                'Memo': item['Memo'],
            }
            newcharge_info.append(newcharge_data)
            break  # Since rent is done, we can stop here
    if chargestostoplist == []:
        chargestostoplist = None

    increase = new_total_rent - total_current_rent

    if agicheck is True:
        agipercentage = percentage - secondpercentage
        agirent = round(total_current_rent * (1 + agipercentage / 100), 2)
        increase = new_total_rent - agirent

    # Return chargestostoplist as a list of IDs
    return new_total_rent, increase, chargestostoplist, newcharge_info, total_current_rent


def generate_increases(leases_by_building, increasedate, guidelinerate):
    increase_summary = {}
    totalincrease = 0
    numberofincreases = 0

    for building_id, leases in leases_by_building.items():
        logging.info(f"Processing Building ID: {building_id}")
        building_increases = []
        buildingtotalincrease = 0
        buildingnumberofincreases = 0
        

        for lease in leases:
            logging.info(f"Processing Lease ID: {lease['leaseid']} - Unit: {lease['unitnumber']}")
            percentage = lease['total_increase_percentage']
            agipercentage = lease['calculationpercentage']
        
            if lease['agiinfo'] != []:
                yearcheck = lease['agiinfo'][0]['date_of_first_increase']
                yearcheck = yearcheck + relativedelta(years=1)
                if increasedate > yearcheck:
                    percentage += 0.25
            agirent = None
            agiincrease = None
            agicheck = False


            # summaryfileinfo = summaryfilepro
            guidelinerent, guidelineincrease, chargestostop, recurringinfo, currentrent = processcharges(lease['recurringinfo'], guidelinerate, increasedate, agicheck, percentage)
            logging.info("Finished Processing Guideline Rent")
            rentcheck = guidelinerent + 50
            if chargestostop is not None:
                chargestostop = ', '.join(map(str, chargestostop))

            if lease['agi'] is not None: ### We do nothing with agichargestostop, agirecurringinfo and agicurrentrent
                agicheck = True
                agirent, agiincrease, agichargestostop, agirecurringinfo, agicurrentrent = processcharges(lease['recurringinfo'], agipercentage, increasedate, agicheck, percentage)
                logging.info("Finished Processing AGIRent")
            reason = lease['reason']
            # Calculate the new rent
            if lease['eligible'] == True:
                
                if rentcheck > lease['marketrent']  and lease['marketrent'] != 0:
                    ignored = "Y"
                    reason = "Above Market" 
                else:
                    ignored = " "
            if lease['eligible'] == False:
                ignored = "Y"

            if lease['agiinfo'] != []:
            
                if increasedate > yearcheck:
                    agipercentage -= 0.25
            
            # Prepare the summary data for this lease, formatted as currency
            lease_info = {
                'leaseid': lease['leaseid'],
                'unitnumber': lease['unitnumber'],
                'address' : lease['address'],
                'tenantname': lease['tenantname'],
                'alltenantnames' : lease['alltenantnames'],
                'tenantids' : lease['tenantids'],
                'buildingname' : lease['buildingname'],
                'newrecurringinfo' : recurringinfo,
                'current_rent': currentrent,
                'guidelinerent' : guidelinerent,
                'guidelineincrease': guidelineincrease,
                'agirent' : agirent,
                'agiincrease': agiincrease,
                'percentage' : lease['total_increase_percentage'],
                'calculationpercentage' : agipercentage,
                'marketrent': lease['marketrent'],
                'eligible': lease['eligible'],
                'agitype' : lease['agitype'],
                'ignored' : ignored,
                'reason' : reason,
                'RecurringChargesToStop' : chargestostop
            }
                # 'agi' : lease['agi'],
            building_increases.append(lease_info)
            logging.info(f"New Rent for Lease ID {lease['leaseid']} Processed")
            numberofincreases += 1
            totalincrease += guidelineincrease
            buildingnumberofincreases += 1
            buildingtotalincrease += guidelineincrease
        additionalinfo = {
            'numberofincreases' : buildingnumberofincreases,
            'totalincrease' :   buildingtotalincrease
        }
        increase_summary[building_id] = {
            'increases' : building_increases,
            'additionalinfo' : additionalinfo
        }
    

    return increase_summary, numberofincreases, totalincrease

# recurringinfo = [{'Id': 295957, 'Amount': 1633.99, 'Gl': 3, 'PostDaysInAdvance': 11, 'Memo': 'Rent', 'RentId': 122220}, {'Id': 295958, 'Amount': 69.11, 'Gl': 144077, 'PostDaysInAdvance': 11, 'Memo': 'Parking', 'RentId': None}, {'Id': 295959, 'Amount': 57.81, 'Gl': 144073, 'PostDaysInAdvance': 11, 'Memo': 'Garage Parking', 'RentId': None}]
# percentage = 2.5
# date = "2025-01-01"
# increasedate = datetime.strptime(date, "%Y-%m-%d")
# agicheck = False
# secondpercentage = 2.5


# new_total_rent, increase, chargestostoplist, newcharge_info, total_current_rent = processcharges(recurringinfo, percentage, increasedate, agicheck, secondpercentage)
# temp = ', '.join(map(str, chargestostoplist))
# data = {
# 'RecurringChargesToStop' : str(temp)
# }
# print(data)