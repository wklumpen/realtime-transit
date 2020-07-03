import urllib.request
import json
from datetime import datetime

import pandas as pd

def pull_bay_area():
    api_key = '79eaf36f-796b-4e32-92b6-42fa6ef8e70e'
    url = f'http://api.511.org/transit/operators?Format=JSON&api_key={api_key}'
    r = urllib.request.urlopen(url)
    data = json.loads(r.read())
    df = pd.DataFrame(data)
    df = df[df['Montiored'] == True].copy()
    for idx, agency in df.iterrows():
        status_url = f'http://api.511.org/transit/StopMonitoring?Format=JSON&api_key={api_key}&agency={agency.Id}'
        r = urllib.request.urlopen(status_url)
        data = json.loads(r.read())
        # with open("out.json", 'w') as outfile:
        #     outfile.write(json.dumps(data, indent=4))
        # print(data.keys())
        # timestamp = data['ResponseTimestamp']
        states = data['ServiceDelivery']['StopMonitoringDelivery']['MonitoredStopVisit']
        deltas = []
        for s in states:
            vehicle = s['MonitoredVehicleJourney']
            if vehicle['MonitoredCall']['ExpectedArrivalTime'] is not None and vehicle['MonitoredCall']['AimedArrivalTime'] is not None:
                expected = datetime.strptime(vehicle['MonitoredCall']['ExpectedArrivalTime'], "%Y-%m-%dT%H:%M:%SZ")
                aimed = datetime.strptime(vehicle['MonitoredCall']['AimedArrivalTime'], "%Y-%m-%dT%H:%M:%SZ")
                tDelta = expected - aimed

                # print(vehicle['PublishedLineName'], tdelta.total_seconds()/60)
                deltas.append(tDelta.total_seconds()/60)
        if len(deltas) > 0:
            print(agency.Name)
            print("Total Deltas:", len(deltas))
            print("Average Delay:", sum(deltas)/len(deltas))
            print("Total > 5 min late:", len([i for i in deltas if i > 5.0]))
            print("Percent Delayed:", 100*len([i for i in deltas if i > 5.0])/len(deltas))

if __name__ == "__main__":
    pull_bay_area()