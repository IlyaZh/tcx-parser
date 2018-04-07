#! python3

'''

Parse tcx files

'''


import os, pandas as pd, logging, glob
from lxml import etree


def process_trackpoint(trackpoint,ns1,df):
    temp_ts = None
    temp_hr = None
    temp_lat = None
    temp_long = None
    for elem in trackpoint.iter():
        if elem.tag == '{%s}Time'%ns1:
            temp_ts = elem.text
        elif elem.tag == '{%s}HeartRateBpm'%ns1:
            for subelem in elem.iter():
                if subelem.tag == '{%s}Value'%ns1:
                    temp_hr = subelem.text
        elif elem.tag == '{%s}Position'%ns1:
            for subelem in elem.iter():
                if subelem.tag == '{%s}LatitudeDegrees'%ns1:
                    temp_lat = subelem.text
                elif subelem.tag == '{%s}LongitudeDegrees'%ns1:
                    temp_long = subelem.text
    temp_d = {'Timestamp': [temp_ts], 'HeartRateBpm': [temp_hr]
        , 'Latitide':[temp_lat], 'Longitude':[temp_long], 'Activity':[None]}
    temp_df = pd.DataFrame(data=temp_d)
    #print(temp_df)
    df = df.append(temp_df)
    return df

def process_tcx_file(tcx_file):
    ns1 = 'http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2'
    tree = etree.parse(tcx_file)
    root = tree.getroot()
    dd = {'Timestamp': [None], 'HeartRateBpm': [None], 'Latitide': [None]
        , 'Longitude': [None], 'Activity': [None], 'tcx_file': [None]}
    tcx_df = pd.DataFrame(data=dd)
    for element in root.iter():
        if element.tag == '{%s}Trackpoint'%ns1:
            tcx_df = process_trackpoint(element,ns1,tcx_df)
        if element.tag == '{%s}Plan'%ns1:
            for subelem in element.iter():
                if subelem.tag == '{%s}Name'%ns1:
                    tcx_df.loc[:, 'Activity'] = subelem.text
    tcx_df.loc[:, 'tcx_file'] = tcx_file
    logging.debug(str(tcx_df))
    return tcx_df


def process_folder(folder_path=os.getcwd(), output_csv='polar_flow.csv'):
    fl = glob.glob(os.path.join(folder_path, '*.tcx'))
    tcx_table = None
    for this_tcx in fl:
        temp = process_tcx_file(this_tcx)
        tcx_table = temp.append(tcx_table)
    tcx_table.to_csv(output_csv,index=False)

