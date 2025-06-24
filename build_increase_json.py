import json
from cryptography.fernet import Fernet
import logging



def increaseportion(lease, increase_effective_date):
    if lease['agirent'] is None:
        rent = lease['guidelinerent']
        increase = lease['guidelineincrease']

    else:
        rent = lease['agirent']
        increase = lease['agiincrease']
         
         

    increaseinfo = {
    'alltenantnames' : lease['alltenantnames'],
    'address' : lease['address'],
    'increasedate' : increase_effective_date,
    'newrent' : rent,
    'increase' : increase,
    'percentage' : lease['percentage'],
    'agitype' : lease['agitype'],
    'ignored' : lease['ignored'],
    'unit' : lease['unitnumber']
    }
    return increaseinfo
def renewalportion(lease, increase_effective_date):
    # tenantids = ', '.join(map(str, lease['tenantids']))

    renewalinfo = {
            'LeaseType' : 'FixedWithRollover',
            'LeaseToDate' : increase_effective_date,
            'Rent' : {
                 'Cycle': 'Monthly',
                 'Charges' :lease['newrecurringinfo'],
                },
            'TenantIds' : lease['tenantids'],
            'SendWelcomeEmail' : False,
            'RecurringChargesToStop' : lease['RecurringChargesToStop'],
            'ignored' : lease['ignored']
      }
    return renewalinfo
def jsoncreation(perbuildinglist, client_secret):
    cipher = Fernet(client_secret)
    infoasstr = json.dumps(perbuildinglist)
    encrypted_list = cipher.encrypt(infoasstr.encode())

    return encrypted_list

async def buildincreasejson(increase_summary, increase_effective_date, client_secret):
    increase_effective_date = increase_effective_date.strftime('%Y-%m-%d')
    perbuildinglist = []

    for building_id, data in increase_summary.items():
        increases = data['increases']
        perleaseinfolist = []

        # build per‐lease info
        for lease in increases:
            if lease['reason'].startswith("Moving"):
                continue

            try:
                increasenotice = increaseportion(lease, increase_effective_date)
                leaserenwal    = renewalportion(lease,   increase_effective_date)
            except Exception as e:
                logging.error(f"Error building lease sections for building {building_id}: {e}")
                continue

            perleaseinfolist.append({
                'leaseid'       : lease['leaseid'],
                'increasenotice': increasenotice,
                'renewal'       : leaserenwal,
                'buildingname'  : lease['buildingname'],
                'ignored'       : lease['ignored']
            })

        # now decide if the whole building is ignored
        # (only if we actually have leases, and ALL of them are flagged “Y”)
        if perleaseinfolist and all(l['ignored'] == "Y" for l in perleaseinfolist):
            ignorebuilding = "Y"
        else:
            ignorebuilding = "N"

        perbuildinglist.append({
            building_id: {
                'lease_info'     : perleaseinfolist,
                'effective_date' : increase_effective_date,
                'ignorebuilding' : ignorebuilding
            }
        })

    # encrypt & return
    try:
        return jsoncreation(perbuildinglist, client_secret)
    except Exception as e:
        logging.error(f"Error encrypting JSON payload: {e}")
        raise




