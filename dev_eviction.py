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

completed_developments_file = "/home/husayn/Projects/eviction_mapping/data/FINAL_2014_datasf-1.csv"
pipeline_file = "/home/husayn/Projects/eviction_mapping/data/San_Francisco_Development_Pipeline_2016_Quarter_1.csv"
three_one_one_file = "/home/husayn/Projects/eviction_mapping/data/Case_Data_from_San_Francisco_311__SF311_.csv"
police_arrests_file = "SFPD_Incidents_-_from_1_January_2003.csv"
sf_evictions_file = "no_fault_evictions_sf.csv"

def getLargeDevelopments():
    print "Getting large developments completed or near completion"
    errors = 0
    bad_dev = {}
    with open(completed_developments_file, 'rb') as f:
        reader = csv.DictReader(f)
        for (i,row) in enumerate(reader):
            units_net = int(row['NETUNITS'])
            affordable_units = row['AFFHSG']
            if affordable_units.strip() == "-":
                affordable_units = 0
            affordable_units = int(affordable_units)
            if affordable_units > units_net:
                #print 'error -- num affordable units is ', affordable_units, ' and net units is ', units_net
                errors+=1
            if (float(affordable_units)/int(units_net)) < .3 and int(units_net) > 30:
                #these thresholds should be learned, not hardcoded
                bad_dev[row['STDADD']] = row
        print 'num errors: ', errors
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
            id_num = row['IncidntNum']
            category = row['Category']
            descript = row['Descript']
            time = row['DayOfWeek'] + row['Time']
            address = row['address']
            relevant_row = {'category':category, 'descript':descript, 'time':time, 'address':address}
            shrinked_dict[id_num] = relevant_row
    print "done getting police arrest data"
    return shrink_dict
            
def get311Data():
    print "getting 311 data"
    url = 'https://maps.googleapis.com/maps/api/geocode/json'
    shrink_dict = {}
    writer = csv.writer(open('shrinked_geocoded.csv', 'wb'))
    with open(three_one_one_file, 'rb') as f:
        reader = csv.DictReader(f)
        for (i,row) in enumerate(reader):
            print 'ITERATION #', i
            if (len(row['Point']) > 3): #just checking if there are valid coordinates
                #params = {'sensor':'false', 'lat':row['Point'].split(',')[0][:-1], 'long':row['Point'].split(',')[1][:-1]}
                id_num = int(row['CaseID'])
                params = {'sensor':'false', 'address':row['Address']}
                r = requests.get(url, params)
                results = r.json()['results']
                print results
                if len(results) > 3: #actually contains shit
                    print 'FORMATTED ADDRESS: ', results[0]['formatted_address']
                    zipcode = results[0]['formatted_address'].split(',')[2].split()[-1]
                    category = row['Category']
                    request_type = row['Request Type']
                    request_details = row['Request Details']
                    neighborhood = row['Neighborhood']
                    time_of_request = row['Opened']
                    relevant_row = {'zipcode':zipcode, 'category':category, 'request_type':request_type, 'request_details':request_details, 'neighborhood':neighborhood, 'time':time_of_request}
                    shrink_dict[id_num] = relevant_row
                    writer.writerow([id_num, relevant_row])
            if i==100:
                return 0
    return shrink_dict

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

#looking at where largest developments are --> then we can focus on the rates of policing in those areas before/after/during the development process --> this, again, should take any dictionary (standard format w/ address-->row (also dict) mapping) w/ standard row format!!  
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

def zipSorted(zipcodes):
    counts = {}
    for zipcode in zipcodes:
        counts[zipcode] = counts.get(zipcode, 0)+1
    return sorted(counts.items(), key=lambda x:x[1])

zips, addDict = plot(getLargeDevelopments())
zipSort = zipSorted(zips)
developmentsInterest = filterDevByZip(addDict, zipSort[-1][0])
print zipSort
print developmentsInterest

### Quick look-through of development dictionary ###
developments = getLargeDevelopments()
print len(developments)
num_apartments = 0
num_fam = 0
for k,v in developments.iteritems():
    if v['PROPUSE'] == "APARTMENTS":
        num_apartments +=1
    if "FAMILY" in v['PROPUSE']:
        num_fam +=1
print "num apartments: ", num_apartments, "num family units: ", num_fam
####################################################
