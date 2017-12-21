#!/usr/bin/python
# -*- mode: python; c-basic-offset: 2; indent-tabs-mode: nil; coding: utf-8; -*-
# vim:expandtab:shiftwidth=2:tabstop=2:smarttab:

"""
author: lukaimin
update: 20171222
"""

import sys
from itertools import groupby 
from operator import itemgetter 
from collections import Counter, OrderedDict 
from rugis import dbscan, Distance, GPS2GCJ
import json
import argparse as parser
sys.path.append("./scipy")
from sklearn.cluster import MeanShift

parser = parser.ArgumentParser()
parser.add_argument("-a", "--act", type=str, required = True)
parser.add_argument("-d", "--date", type=str, help="run date")
parser.add_argument("-w", "--window", type=int, default=90, help="date window")
args = parser.parse_args()

"""
offline version, after mapper, you need groupby by the key
useage:
    1. [calculate all location]
      cat training_data.TXT|python aoi_loc_cluster_model_local.py --act "m"|sort -n |python aoi_loc_cluster_model_local.py --act "runModel" --date "20160516" --window 90 

    2. [calculate weekend location]
      cat training_data.TXT|python aoi_loc_cluster_model_local.py --act "mWeekend"|sort -n |python aoi_loc_cluster_model_local.py --act "runModel" --date "20160516" --window 90 

    3. [calculate weekday location] 
      cat training_data.TXT|python aoi_loc_cluster_model_local.py --act "mWeekday"|sort -n |python aoi_loc_cluster_model_local.py --act "runModel" --date "20160516" --window 90 

"""

def findCluster(dataSet, labels, level=5, debug=False):
    clstr_info = dict(Counter(labels))
    if debug:
        print "##### dataset labels Freq info:"
        print clstr_info
    labels_val = sorted(set(clstr_info.values()), reverse=True)
    if debug:
        print "##### uniq labels sorted:"
        print labels_val
    candidate_clstr_labels = []
    clusters = [[],[],[],[],[]]
    try:
        if len(labels_val) >= 1:
            max1_clstr_pos_cnt = labels_val[0]
            max2_clstr_pos_cnt,max3_clstr_pos_cnt,max4_clstr_pos_cnt, \
            max5_clstr_pos_cnt = ["","","",""]
            if len(labels_val) >= 5:
                max2_clstr_pos_cnt = labels_val[1]
                max3_clstr_pos_cnt = labels_val[2]
                max4_clstr_pos_cnt = labels_val[3]
                max5_clstr_pos_cnt = labels_val[4]
            elif len(labels_val) == 4:
                max2_clstr_pos_cnt = labels_val[1]
                max3_clstr_pos_cnt = labels_val[2]
                max4_clstr_pos_cnt = labels_val[3]
            elif len(labels_val) == 3:
                max2_clstr_pos_cnt = labels_val[1]
                max3_clstr_pos_cnt = labels_val[2]
            elif len(labels_val) == 2:
                max2_clstr_pos_cnt = labels_val[1]
    
            for c_idx, pos_cnt in clstr_info.iteritems():
                if len(candidate_clstr_labels) >= level:
                    break
    
                if pos_cnt in [max1_clstr_pos_cnt,max2_clstr_pos_cnt,max3_clstr_pos_cnt,max4_clstr_pos_cnt,max5_clstr_pos_cnt]:
                    candidate_clstr_labels.append(c_idx)
            
            if debug:
                print "##### dbscan labels candidate set:"
                print candidate_clstr_labels
            
            for i in xrange(len(labels)):
                idx = 0
                for j in candidate_clstr_labels:
                    if labels[i] == j:
                        clusters[idx].append(dataSet[i])
                        break
                    idx += 1
            
            if debug:
                print "##### candidate clusters:"
                print clusters
    
            return clusters
        else:
        return []
   except Exception,e:
      print e
      return []

def dbscanParameter(dataSize):
    if isinstance(dataSize, int) and dataSize > 0:
        if dataSize < 20:
            minPts, radius, mgfactor = 5, 50, 1.5
        elif dataSize < 50:
            minPts, radius, mgfactor = 5, 40, 1.5
        elif dataSize < 100:
            minPts, radius, mgfactor = 5, 20, 1.5
        elif dataSize < 200:
            minPts, radius, mgfactor = 5, 15, 2
        elif dataSize < 300:
            minPts, radius, mgfactor = 6, 15, 2
        elif dataSize < 400:
            minPts, radius, mgfactor = 7, 15, 2
        elif dataSize < 500:
            minPts, radius, mgfactor = 8, 15, 2
        elif dataSize < 600:
            minPts, radius, mgfactor = 8, 13, 2.5
        elif dataSize < 700:
            minPts, radius, mgfactor = 9, 13, 2.5
        elif dataSize < 800:
             minPts, radius, mgfactor = 9, 12, 2.5
        elif dataSize < 1000:
            minPts, radius, mgfactor = 9, 12, 2.5
        elif dataSize < 2000:
            minPts, radius, mgfactor = 10, 10, 3
        elif dataSize < 4000:
            minPts, radius, mgfactor = 10, 10, 3
        else:
           minPts, radius, mgfactor = 10, 10, 3 
    return minPts, radius, mgfactor

def evaluateRadiusMin(dataSet):
    ''' the radius is the dataSet's inner circle, maxVal is 200m '''
    if len(dataSet) <= 1:
        return 50
    else:
        lat_set, lng_set = zip(*dataSet)
        lat_min, lat_max = min(lat_set), max(lat_set)
        lng_min, lng_max = min(lng_set), max(lng_set) 
        lat_dis = Distance(lat_min,lng_min,lat_max,lng_min)
        lng_dis = Distance(lat_min,lng_min,lat_min,lng_max)
        radius = min(int(lat_dis/2), int(lng_dis/2), 200)
        return radius

def evaluateRadiusMax(dataSet):
    ''' the radius is the dataSet's inner circle, maxVal is 200m '''
    if len(dataSet) <= 1:
        return 50
    else:
        lat_set, lng_set = zip(*dataSet)
        lat_min, lat_max = min(lat_set), max(lat_set)
        lng_min, lng_max = min(lng_set), max(lng_set) 
        dis = int(Distance(lat_min,lng_min,lat_max,lng_max))
        radius = min(dis, 200)
        return radius

def getScore(x):
    import math
    def sigMoid(x, base = 7):
        if x < 0:
            return 1
        else:
            return 1/(1 + math.exp(-x/float(base))) 
    score = 2*(sigMoid(x) - .5)
    return float('%.4f' % score)

def getWeekdistri(dateLists):
    from numpy import array
    dt_perc = {}
    dt_tags = [isWeekday(dt) and (not isHoliday(dt)) for dt in dateLists]
    dt_tags = array(dt_tags, dtype=str)
    counter = Counter(dt_tags)
    weight  = sum(counter.values()) 
    if "True" in  counter.keys():
        weekday_cnt = counter.get("True")
    else:
        if "False" in counter.keys():
            weekday_cnt = 0
        else:
            return dtPerc
    dt_perc.update({"weekday":weekday_cnt, "weekend":weight-weekday_cnt}) 
    return dt_perc

def getBetweenDay(end_date, date_window):  
    import datetime, time
    date_list = []  
    end_date = datetime.datetime.strptime(end_date, "%Y%m%d")  
    begin_date = end_date + datetime.timedelta(-date_window)
    while begin_date < end_date:  
        date_str = begin_date.strftime("%Y%m%d")  
        date_list.append(date_str)
        begin_date += datetime.timedelta(days=1)  
    return date_list

def getTimePreference(hours):
    """ calculate hour preference of user's aoi 
        {9:'0.2631',
         1:'0.1578',
         2:'0.1052',
         4:'0.1052',
         7:'0.1052'}
    """
    TOP_HOUR = 10
    """ hour regularization: if hour < 7, set 6  """
    hours = map(lambda x: ( x < 7 and 6 ) or x , hours)
    try:
        t_pref = ""
        cnter = Counter(hours)
        cnter = [(it[0], it[1]/float(len(hours))) for it in cnter.most_common(TOP_HOUR)]
        for hour,prob in cnter:
            t_pref += "\"%d\":%.4f," % (hour, prob)
        return "{" + t_pref[:-1]  + "}" 
    except Exception,e:
        return "{}"

def getWeekPreference(dateLists):
    """ METRIC EXPLAINATION
           weight: total unique date count
       week_ratio: #{}
          weekday: #{real location weekday}/#{total mining date window weekday}
          weekend: #{real location weekend}/#{total mining date window weekend}
        
    """
    dt_perc = "\"weight\":{weight},\"weekday_pref\":{weekday_pref},\"weekday\":{weekday_wei},\"weekend\":{weekend_wei}"
  
    try:
        loc_week_metric  = getWeekdistri(dateLists)
        weekday_wei  = loc_week_metric.get("weekday")/WEEKDAY_BASIC
        weekend_wei  = loc_week_metric.get("weekend")/WEEKEND_BASIC
        weight       = sum(loc_week_metric.values())
        weekday_pref = loc_week_metric.get("weekday")/float(weight)
        """ 
        dt_perc.update({"weight":weight, \
                        "weekday_pref":round(weekday_pref,4), \
                        "weekday":round(weekday_wei,4), \
                        "weekend":round(weekend_wei,4)}) 
        return dt_perc
        """
        dt_perc = dt_perc.format(weight = weight, \
                                 weekday_pref = round(weekday_pref,4), \
                                 weekday_wei = round(weekday_wei,4), \
                                 weekend_wei = round(weekend_wei,4))
        return "{" + dt_perc + "}" 
    except Exception,e:
        return "{}"

def getInfo(locInfo, method):
    if isinstance(locInfo, list):
        try:
            if method == "latlng":
                return [[loc[0],loc[1]] for loc in locInfo]
            elif method == "date":
                return [loc[2] for loc in locInfo]
            elif method == "hour":
                return [loc[3] for loc in locInfo]
            else:
                return []
        except Exception,e:
            return []

def isHoliday(dt):
    """ date format: '20161010' , type:str """
    #springFestival = ["20160207","20160208","20160209","20160210","20160211","20160212","20160213","20160214"]
    qingming = ["20170402","20170403","20170404"]
    laodong  = ["20170429","20170430","20170501"]
    duanwu   = ["20170528","20170529","20170530"]
    guoqing  = ["20171001","20171002","20171003","20171004","20171005","20171006","20171007","20171008"]
    return dt in qingming+laodong+duanwu+guoqing

def isWeekday(dt):
    """date format: '20161010' , type:str """
    import datetime
    if not isinstance(dt, str):
        dt = str(dt)
    if len(dt) != 8:
        return
    try:
        weekday = datetime.date(int(dt[0:4]), int(dt[4:6]), int(dt[6:8])).isoweekday()
        if 1 <= weekday <= 5:
            return True
        elif 6 <= weekday <= 7:
            return False
    except Exception, e:
        return

def locMapper(method = "all"):
    for line in sys.stdin:
        items = line.strip("\n").split() 
        """ ACTION: split format of hql is '\001', not "\001" """
        if len(items) != 8:
            continue
   
        #imei, ds, lat, lng, radius, type, wf, _ = items
        imei, ds, lat, lng, radius, type, wf, coor_type = items
        lat, lng = float(lat), float(lng)
        dt = ds[0:8]
        loc_hour = ds[8:10] 
        loc_time = ds[8:12]
       
        if method == "weekend" and isWeekday(dt):
            continue
        if method == "weekday" and not isWeekday(dt):
            continue
        if not (( 17 < lat < 55) and ( 70 < lng < 136 )):
            continue
        if coor_type.find("WGS84") > -1:
            lat, lng = GPS2GCJ(lat, lng)
        
        print "%s\t%s\t%s\t%.6f\t%.6f" % (imei, dt, loc_hour, lat, lng)


def clusterModel(end_date, date_window):
    def read_input():
        for line in sys.stdin:
            yield line.strip().split() 
    
    counter_idx = 0
    """calculate the basic metric of date preference  """
    BASIC_RATIO = getWeekdistri(getBetweenDay(end_date, date_window))
    
    global WEEKDAY_BASIC, WEEKEND_BASIC
    WEEKDAY_BASIC, WEEKEND_BASIC = float(BASIC_RATIO.get("weekday")), float(BASIC_RATIO.get("weekend"))
    
    for imei, imeigroup in groupby(read_input(), itemgetter(0)): 
        imeigroup = sorted(imeigroup, key=lambda d:d[1], reverse=True) 
        loc_info = []
  
        for val in list(imeigroup): 
            """              lat, lng, imei, dt """
            #loc_info.append([list(val)[3],list(val)[4],list(val)[0],list(val)[1]])
            lat  = float(list(val)[3])
            lng  = float(list(val)[4])
            dt   = int(list(val)[1])
            hour = int(list(val)[2])
            loc_info.append([float('%.6f' % lat), float('%.6f' % lng), dt, hour])
        
        minPts, radius, mgfactor = dbscanParameter(len(loc_info))
        labels = dbscan(getInfo(loc_info,method="latlng"), minPts, radius, mgfactor)      
        clusters = findCluster(loc_info, labels, level=5)
        print>>sys.stderr, "reporter:counter:Internal Counters,ALL,%d" % counter_idx
        print>>sys.stderr, "reporter:status:calculate features"
        if clusters is None:
            clusters = [loc_info]
  
        clf = MeanShift(cluster_all=False, min_bin_freq=5, bandwidth=.5)
        centers = []
        CLUSTER_CENTER_LIMITS = 300
        
        try:
            for i in xrange(len(clusters)):
                counter_idx += 1
                clst  = getInfo(clusters[i], method="latlng")
                dts   = getInfo(clusters[i], method="date")
                hours = getInfo(clusters[i], method="hour")
                dt_set = set(dts)
                uniq_dt_cnt = len(dt_set)
                last_time = max([int(x) for x in dt_set])
                score = getScore(uniq_dt_cnt)     
                time_preference = getTimePreference(hours)
                date_preference = getWeekPreference(dt_set)
                
                try:
                    if clst is not None and len(clst) >= 5 :
                        clf.fit(clst)
                        
                        """ calculate the radius of dbscan clusters """
                        radius = evaluateRadiusMin(clst)
                        """ choose then best cluser """
                        center = list(clf.cluster_centers_)[0]
                        
                        if i == 0:
                            centers.append([center,radius,score,last_time,time_preference,date_preference])
                        else:
                            tag_idx = 0
                            for pos_ in centers:
                                if Distance(pos_[0][0],pos_[0][1], center[0],center[1]) > CLUSTER_CENTER_LIMITS:
                                    tag_idx += 1 
                                if tag_idx == len(centers):
                                    centers.append([center,radius,score,last_time,time_preference,date_preference])
                except Exception,e:
                    continue
            centers = "|".join(['%.6f#%.6f#%d#%.4f#%s#%s#%s' % (p[0][0], p[0][1], p[1], p[2], p[3], p[4], p[5]) for p in centers])   
            print "%s\t%s" % (imei, centers)
        except Exception,e:
            continue

if __name__ == "__main__":
    if args.act == "mWeekend":
        locMapper(method = "weekend")
    elif args.act == "mWeekday":
        locMapper(method = "weekday")
    elif args.act == "m":
        locMapper()
    elif args.act == "runModel":
        clusterModel(end_date=args.date,date_window=args.window)
    else:
        print "[HELP] python aoi_loc_cluster_model_local.py --act "runModel" --date "20160516" --window 60
