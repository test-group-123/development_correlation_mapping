#filters development data and compares against eviction rate
###### Burnin' Q's ######
# 1. How to define affordable? 
# 2. Developing criterion for what to classify as a "bad" development

# Dictionary format #######################################
# 1. Address (ID for 311) --> Row mapping ################# 
# 2. Row = {Address: add, 
import csv
import requests
import gmplot
import matplotlib.pyplot as plt
from geopy.geocoders import Nominatim
from haversine import haversine
from datetime import datetime as dt
from datetime import timedelta

completed_developments_file = "/home/husayn/Projects/eviction_mapping/data/FINAL_2014_datasf-1.csv"
pipeline_file = "/home/husayn/Projects/eviction_mapping/data/San_Francisco_Development_Pipeline_2016_Quarter_1.csv"
three_one_one_file = "/home/husayn/Projects/eviction_mapping/data/Case_Data_from_San_Francisco_311__SF311_.csv"
police_arrests_file = "/home/husayn/Projects/eviction_mapping/data/SFPD_Incidents_-_from_1_January_2003.csv"
sf_evictions_file = "/home/husayn/Projects/eviction_mapping/data/no_fault_evictions_sf.csv"

month_dict = {'Jan': '1', 'Feb':'2', 'Mar':'3', 'Apr':'4', 'May':'5', 'Jun':'6', 'Jul':'7', 'Aug':'8', 'Sep':'9', 'Oct':'10', 'Nov':'11', 'Dec':'12'}

#take out all developments that don't have "erect" in descriptor
def getLargeDevelopments():
    print "Getting large developments completed or near completion"
    errors = 0
    bad_dev = {}
    with open(completed_developments_file, 'rb') as f:
        geolocator = Nominatim()
        reader = csv.DictReader(f)
        for (i,row) in enumerate(reader):
            if i==1:
                print row.keys()
            units_net = int(row['NETUNITS'])
            affordable_units = row['AFFHSG']
            if affordable_units.strip() == "-":
                affordable_units = 0
            affordable_units = int(affordable_units)
            if affordable_units > units_net:
                errors+=1
            if (float(affordable_units)/int(units_net)) < .3 and int(units_net) > 20 and "erect" in row['DESCRIPT'].lower():
                longitude = 0
                latitude = 0
                address_list = row['STDADD'].split()
                final_address = address_list[0] + " " + address_list[1].strip("0") + " " + " ".join(address_list[2:])
                print final_address
                try:
                    location = geolocator.geocode(final_address + ' San Francisco')
                    latitude = location.latitude
                    longitude = location.longitude
                except:
                    print "failed to get lat/long for address " + row['STDADD']
                relevant_row = {}
                date = str(row['ACTDATE'].split(' ')[0])
                print date
                descript = row['DESCRIPT']
                id_num = row ['APPL_NO']
                use = row['PROPUSE']
                relevant_row = {'id':id_num, 'descript':descript, 'lat':latitude, 'lon':longitude, 'net': units_net, 'aff':affordable_units, 'date':date, 'use':use}
                if (len(str(longitude))>3 and str(longitude)[0:3]=="-12" and len(str(latitude))>3 and str(latitude)[0:2]=="37"):
                    bad_dev[id_num] = relevant_row
        print 'num errors: ', errors
    print len(bad_dev)
    return bad_dev

#this takes from the pipeline dataset --> could potentially combine this function and the one above with a boolean argument Pipeline that takes True/False --> takes dict and adds proposal date to value dict within the passed dict
def getPipelineData(filteredDict):
    print "getting proposed developments"
    bad_proposed_dev = {}
    with open(pipeline_file, 'rb') as f:
        reader = csv.DictReader(f)
        for (i,row) in enumerate(reader):
            address = row['NAMEADDR']
            affordable_unit

#reads from SFPD_Incidents_-_from_1_January_2003.csv and populates dictionary w/ address --> row mapping (this needs to be the standard in all files, btw)
def getPoliceArrestData():
    print "getting police arrest data"
    shrinked_dict = {}
    with open(police_arrests_file, 'rb') as f:
        reader = csv.DictReader(f)
        for (i,row) in enumerate(reader):            
            id_num = int(row['Incident'])
            descript = row['Descript']
            date = row['Date']
            resolution = row['Resolution']
            lat = row['Y']
            lon = row['X']
            relevant_row = {'id':id_num, 'category': descript, 'date':date, 'resolution':resolution, 'lat':lat, 'lon':lon}
            shrinked_dict[id_num] = relevant_row
    print "done getting police arrest data"
    return shrinked_dict

def get311Data():
    print "getting 311 data"
    shrink_dict = {}
    with open(three_one_one_file, 'rb') as f:
        reader = csv.DictReader(f)
        for (i,row) in enumerate(reader):
            if (len(row['Point']) > 3 and row['Point'][0]=='('): #just checking if there are valid coordinates
                id_num = int(row['CaseID'])
                category = row['Category']
                opened = row['Opened']
                closed = row['Closed']
                lat = row['Point'].split(',')[0][1:-1].strip()
                lon = row['Point'].split(',')[1][:-1].strip()
                relevant_row = {'id':id_num, 'category':category, 'date':opened, 'closed':closed, 'lat':lat, 'lon':lon}
                shrink_dict[id_num] = relevant_row
    print "done getting 311 data"
    return shrink_dict

def getEvictionData():
    eviction_dict = {}
    geolocator = Nominatim()
    with open(sf_evictions_file, 'rb') as f:
        reader=csv.DictReader(f)
        for (i,row) in enumerate(reader):
            if (row['address_1'] not in eviction_dict):
                try:
                    location = geolocator.geocode(row['address_1'] + ' San Francisco')
                    print(location.address)
                    split_location = location.address.split(',')
                    zipcode = ""
                    for part in split_location:
                        if len(part)==6 and part[0:3]=="941":
                            more_dict = row
                            zipcode = part
                            more_dict['zipcode']=zipcode
                            eviction_dict['cartodb_id'] = more_dict
                except:
                    pass
    print "got the eviction data"
    #with open("eviction_data_zips.csv", 'wb') as f:
    return eviction_dict

def getEvictionDataStandard():
    eviction_dict = {}
    with open(sf_evictions_file, 'rb') as f:
        reader = csv.DictReader(f)
        for (i,row) in enumerate(reader):
            eviction_dict['cartodb_id'] = row
    return eviction_dict

#takes the development dictionary with the zips of interest and a second dictionary (police, 311, or evictions) and makes a new dictionary with only rows of the larger dictionary that have zip field equal to a zip in filteredDevDict. So this returns a subset of largeDict
def filterByAddress(filteredDevDict, largeDict):
    pass

#takes dictionary (address --> row map) and a zipcode and returns dictionary (address --> row map) whose rows contain a zipcode field that contains the passed zipcode to the filter function --> this should filter any address --> row mapping ->- basically function above
def filterDevByZip(devDict, zipcode):
    filteredDict = {}
    for k,v in devDict.iteritems():
        if 'zipcode' in v and v['zipcode']==zipcode: #checks if the row has a field 'zipcode' and if its the same as the one we want to choose for
            print 'ADDED'
            filteredDict[k]=v
    return filteredDict

#only works for the SFPD data because of how I index the row for lat and long (the fields are different for each dataset) --> should be able to filter by date as well
#purpose is to take the coordinates of a large development and find all the arrests within a certain radius of that development
def filterByRadius(dictionary, development_center, radius):
    filtered = {}
    for (k,v) in dictionary.iteritems():
        eviction_location = (float(v['lat']), float(v['lon']))
        #print eviction_location
        if(haversine(eviction_location, development_center, miles=True) <= radius): #should vary this circle
            filtered[v['id']] = v
    print "done filtering by radius"
    return filtered

#takes a dictionary and a date and returns a filtered dictionary only with rows that contain date fields that are *after* the passed date
def filterByDate(dictionary, date, interval, keyword='cleaning'):
    filtered = {}
    for (k,v) in dictionary.iteritems():
        date_initial = v['date'].split('/') 
        yr = date_initial[2][2:4]
        final_date_str = date_initial[0]+'/'+date_initial[1]+'/'+yr
        date_obj = dt.strptime(final_date_str, "%m/%d/%y")
        #if ((date.strftime('%m/%d/%y') == date_obj.strftime('%m/%d/%y')) and keyword in v['category'].lower()):
        #if (abs((date_obj-date).days) < 5): #and (keyword in v['category'].lower() or 'graffiti' in v['category'].lower())):
        if (abs((date_obj-date).days) < interval): #and ('marijuana' in v['descript'].lower() or 'possession' in v['descript'].lower())):
            filtered[k] = v
    return filtered
    
def filterByDateAndKeyword(dictionary, date, interval, keywords=None):
    filtered = {}
    for (k,v) in dictionary.iteritems():
        date_initial = v['date'].split('/')
        yr = date_initial[2][2:4] # this should be done when populating the data dictionaries. like v['date'] should already be a dateitem
        final_date_str = date_initial[0]+'/'+date_initial[1]+'/'+yr
        date_obj = dt.strptime(final_date_str,"%m/%d/%y")
        if keywords==None:
            if (abs((date_obj-date).days) < interval):
                filtered[k] = v
        else:
            if (abs((date_obj-date).days) < interval and any(word in v['category'].lower() for word in keywords)):
                filtered[k] = v
    return filtered

#looking at where largest developments are --> then we can focus on the rates of policing in those areas before/after/during the development process --> this, again, should take any dictionary (standard format w/ address-->row (also dict) mapping) w/ standard row format!!  
"""
def plot(addDict):
    url = 'https://maps.googleapis.com/maps/api/geocode/json'
    lats = []
    longs = []
    zips = []
    for k,v in addDict.iteritems():
        params = {'sensor':'false', 'address':v['STDADD'] + ' San Francisco'}
        r = requests.get(url, params=params) #TODO: why InsecurePlatformWarning?
        results = r.json()['results'] 
        print 'results len: ', len(results)
        location= results[0]['geometry']['location'] #if this is out of range, we've made too many requests
        #print results[0]['formatted_address']
        zipcode = results[0]['formatted_address'].split(',')[2].split()[-1]
        if len(zipcode)==5:
            zips.append(zipcode)
            addDict[k]['zipcode']=zipcode #just add zipcode to the row
        #for key in results[0].keys():
        #    print '\n',key, ' ', results[0][key], '\n'
        lats.append(location['lat'])
        longs.append(location['lng'])
    gmap=gmplot.GoogleMapPlotter(37.7749, -122.4194, 16)
    gmap.scatter(lats, longs, '#000000', size=1.5, marker=False)
    gmap.heatmap(lats,longs)
    gmap.draw("developments.html")
    #print(len(set(zips)))
    return (zips, addDict)
"""

def plot(plotDict):
    lats = []
    longs = []
    for k,v in plotDict.iteritems():
        if ('lat' in v.keys() and 'lon' in v.keys()):
            lats.append(v['lat'])
            longs.append(v['lon'])
    gmap=gmplot.GoogleMapPlotter(37.7749, -122.4194, 16)
    gmap.scatter(lats, longs, '#000000', size=1.5, marker=False)
    gmap.heatmap(lats,longs)
    gmap.draw("developments.html")
    
def zipSorted(zipcodes):
    counts = {}
    for zipcode in zipcodes:
        counts[zipcode] = counts.get(zipcode, 0)+1
    return sorted(counts.items(), key=lambda x:x[1])

"""
zips, addDict = plot(getLargeDevelopments())
zipSort = zipSorted(zips)
developmentsInterest = filterDevByZip(addDict, zipSort[-1][0])
print zipSort
print developmentsInterest
"""

# EXECUTION INFO #:
# 1) Use 2175 Market St. (37.76628605, -122.430041249368) as the test case
# 2) Start with a radius of 2 miles
# 3) Make graph of the developments in this region over time (red for completed/close-to-completed developments and blue for proposed) 
# 4) Make graph of the 311 cleaning, graffiti, noise, 
# 5) Make graph of SFPD substance possession, garbage, homelessness, sitting/obstructing sidewalks 

### Quick look-through of development dictionary ###
developments = getLargeDevelopments()
print len(developments)
num_apartments = 0
num_fam = 0
for k,v in developments.iteritems():
    if v['use'] == "APARTMENTS":
        num_apartments +=1
    if "FAMILY" in v['use']:
        num_fam +=1
print "num apartments: ", num_apartments, "num family units: ", num_fam
plot(developments)
####################################################

filtered_developments = filterByRadius(developments, (37.76628605, -122.430041249368), 1.0)
print 'there are', len(filtered_developments), 'filtered developments'
dates = []
for (k,development) in filtered_developments.iteritems():
    print('date:', development['date'], 'net units: ' , development['net'], 'affordable:', development['aff'])
    date = development['date'].split("/") 
    yr = date[2][2:4]
    final_date = date[0] + "/" + date[1] + "/" + yr
    date_obj = dt.strptime(final_date, "%m/%d/%y")
    dates.append(date_obj)
y = [1]*len(dates)
fig = plt.figure()
ax = plt.subplot(111)
ax.bar(dates, y, width=2)
ax.set_ylim(ymax=2.0)
ax.xaxis_date()
plt.show()

# need to sort by date *and* keyword
# get 311 data b/w the dates
dates.sort()
threeOneOne = filterByRadius(get311Data(), (37.76628605, -122.430041249368), 1.0) #filter 311 dictionary so it has requests only within a mile of 2175 Market St.
start_date = dates[0] - timedelta(10) 
end_date = dates[-1] + timedelta(10)
running_date = start_date
lengths = []
new_dates = []
print 'difference', (end_date-running_date).days
i=0
while (end_date - running_date).days > 10: #is this interval actually correct
    print i
    new_dates.append(running_date)
    lengths.append(len(filterByDate(threeOneOne, running_date, 0)))
    running_date = running_date + timedelta(1)
    i+=1
fig = plt.figure()
ax = fig.add_subplot(111)
#ax2 = ax.twinx()
#ax2.plot(ax.get_xticks(),  [new_dates, lengths], linestyle='-', marker = 'o', linewidth=2.0)
ax.plot(new_dates, lengths)
ax.grid(True)
fig.autofmt_xdate()
plt.show()


