import pandas as pd
import requests
import time
from .helper_functions import check_status_error
from .constants import SLEEP_TIME

def getAllFacilities(auth_token: str, url: str) -> pd.DataFrame:
    print('Getting all facilities')
    time.sleep(SLEEP_TIME)

    headers = {
        'Authorization': auth_token,
        'Content-Type': 'application/json'
    }

    payload = {
        "limit": 5000,
        "offset": 0,
        "filters": {
            "cities": [],
            "status": ["On", "Archived", "Off"]
        },
        "facility_ids": "",
        "operator_email": ""
    }

    res = requests.post(url, json=payload, headers=headers)
    check_status_error(res, "facilities")

    results = res.json()['data']['results']['canonical_facilities']

    all_facilities = {
        'id': [],
        'title': [],
        'eventTieringEnabled': [],
        'physicalAddress': []
    }

    for facility in results:
        for spot in facility.get('parking_spots', []):
            all_facilities['id'].append(spot['id'])
            all_facilities['title'].append(spot['title'])
            all_facilities['eventTieringEnabled'].append(spot['event_tiering_enabled'])

            address = facility.get('address', {})
            address_parts = [
                address.get('street_address', ''),
                address.get('city', ''),
                address.get('state', ''),
                address.get('zipcode', '')
            ]
            physical_address = " ".join(filter(None, address_parts))
            all_facilities['physicalAddress'].append(physical_address)

    df = pd.DataFrame(all_facilities)
    df = df.sort_values(by='title')
    print("Successfully got all facilities")
    return df

