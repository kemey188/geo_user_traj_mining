
import sys
from itertools import groupby, chain
from operator import itemgetter
import json
import time
from collections import OrderedDict, Counter
sys.path.append("rugis")

class trackModel(object):
    def __init__(self, ts_margin_threshold=600, dt_fmt="timestamp", output_fmt="normal", dbg=False):
        self.ts_margin_threshold = ts_margin_threshold
        self.dt_fmt     = dt_fmt
        self.output_fmt = output_fmt
        self.dbg = dbg

    def build_feature(self):

        def read_input():
            for line in sys.stdin:
                yield line.strip().split()
        
        for uid, group in groupby(read_input(), itemgetter(0)):
            ordered_traj = sorted(list(group), key=lambda d:d[1], reverse = False)
            """
            ordered_traj:
                [['360AE73010500089151599299354', '1515995021', '24.907056', '118.525792'], 
                 ['360AE73010500089151599299354', '1515995026', '24.907056', '118.526058'],
                 ...
                ]
            """
            ts_margin  = self.calculate_traj_margin(ordered_traj)
            if self.dbg: print ts_margin
            #ts_beg, ts_end  = ordered_traj[i][2:], ordered_traj[i][2:]
            margins_info   = [([i, i+1], ts_margin[i]) for i in range(len(ts_margin)) if ts_margin[i] > self.ts_margin_threshold]
            if margins_info is None:
                margins_info = [([0, len(ts_margin)-1], ts_margin[-1])]
            margins_pairs = self.calculate_margin_pairs(ordered_traj, margins_info)
    
            if self.output_fmt == "json":
                features = OrderedDict([("uid", uid),
                                        ("key_traj", margins_pairs)])
                print json.dumps(features, ensure_ascii=False, indent=1)
            else:
                for item in margins_pairs:
                    output_info = [uid] + list(item.values())
                    print "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s" % tuple(output_info)
            
            
    def calculate_traj_margin(self, traj_group):
        ts_set = [int(x[1]) for x in traj_group]
        ts_margin = [ts_set[i+1]-ts_set[i] for i in range(len(ts_set)-1)] 
        return ts_margin
    
    
    def calculate_margin_pairs(self, ordered_traj, margins_info):
        from math import ceil
        
        """ get margin index"""
        margins_idx  = [x[0] for x in margins_info]
        margins_stop = [x[1] for x in margins_info]
        margins_idx_set  = list(chain.from_iterable(margins_idx))
        
        """ fill start point or stop point """
        margins_idx_set.insert(0,0)
        margins_idx_set.append(len(ordered_traj)-1)
        
        margins_pairs   = [(margins_idx_set[i], margins_idx_set[i+1]) for i in range(len(margins_idx_set)-1) if i%2 == 0]
        if self.dbg: print margins_pairs

        margins_pairs_info = [OrderedDict([
                                           ("start_t", self.date_fmt(ordered_traj[i][1])),
                                           ("end_t", self.date_fmt(ordered_traj[j][1])),
                                           ("start_loc", "%s,%s" % (ordered_traj[i][2], ordered_traj[i][3])),
                                           ("end_loc", "%s,%s" % (ordered_traj[j][2], ordered_traj[j][3])),
                                           ("drive_minutes", int(ceil((int(ordered_traj[j][1])-int(ordered_traj[i][1]))/60.0))),
                                           ("stop_minutes", 0),
                                           ("loc_cnt", j-i+1)
                                          ]) for i,j in margins_pairs]
        
        for i in range(len(margins_stop)):
            margins_pairs_info[i]["stop_minutes"] = int(ceil(margins_stop[i]/60.0))
    
        return margins_pairs_info
    
    
    def date_fmt(self, timeStamp):
        if self.dt_fmt == "timestamp":
            return timeStamp
        else:
            try:
                timeStamp = int(timeStamp)
                timeArray = time.localtime(timeStamp)
                timet = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
                return timet
            except Exception,e:
                return ""
   

    def time_distri(self, timeStamp):
        timeArray = time.localtime(timeStamp)
        dt = time.strftime("%Y%m%d", timeArray)
        hour = time.strftime("%H", timeArray)
        return (dt, hour)
        

    def isWeekday(self, dt):
        import datetime
        
        """date format: '20161010' , type:str """
        dt = self.date_fmt(dt)
        if dt != "":
            dt = dt.split()[0].replace("-","")
        else:
            return

        try:
            weekday = datetime.date(int(dt[0:4]), int(dt[4:6]), int(dt[6:8])).isoweekday()
            if 1 <= weekday <= 5:
                return True
            elif 6 <= weekday <= 7:
                return False
        except Exception, e:
            return


    def getInfo(self, dataset, method):
        """
            dataset: terminated by "\t"
            360AE73520403655 1516408327 1516411171 30.603265,104.035837 30.248991,104.072292 48 137 252
        """
        if isinstance(dataset, list):
            try:
                if method == "latlng":
                    latlng = [[[float(loc[3].split(",")[0]), float(loc[3].split(",")[1])],
                               [float(loc[4].split(",")[0]), float(loc[4].split(",")[1])]] for loc in dataset]
                    return list(chain.from_iterable(latlng))
                elif method == "datetm":
                    date_list = [[int(loc[1]), int(loc[2])] for loc in dataset]
                    return list(chain.from_iterable(date_list))
                elif method == "loccnt":
                    return [int(loc[7]) for loc in dataset]
                elif method == "drive_minutes":
                    return [int(loc[5]) for loc in dataset]
                elif method == "stop_minutes":
                    return [int(loc[6]) for loc in dataset]
                else:
                    return []
            except Exception,e:
                return []
   

    def find_cluster(self, loc_set, top=5):
        from rugis import dbscan
        
        #labels = dbscan(loc_set, minPts, radius, mgfactor) 
        labels = dbscan(loc_set, 1, 300, 1.5) 
        clstr_info  = dict(Counter(labels))
        best_labels = [] 
        for lab, cnt in clstr_info.iteritems():
            if len(best_labels) < top:
                if cnt > 1: best_labels.append(lab)
            else:
                break
        return {"label":labels, "best_label":best_labels}


    def candidates_info(self, dataset, label_dict):
        best_label = label_dict.get("best_label")
        K = len(best_label)
        if K == 0:
            return
        labels  = label_dict.get("label")
        loc_set = self.getInfo(dataset, "latlng")
        dtm_set = self.getInfo(dataset, "datetm")
        
        """ init list """
        loc_candidates  = [None]*K
        dtm_candidates  = [None]*K
        start_cnt_info  = [0]*K
        desti_cnt_info  = [0]*K
        tag_set = set()

        """ remapping for best choosen labels
            key: label   value: idx
            {0: 0, 1: 1, 3: 2, 4: 3}
        """
        new_mapping = dict(zip(best_label, range(K)))

        for idx in range(len(labels)):
            if labels[idx] in best_label:
                _idx = new_mapping[labels[idx]]
                if not (labels[idx] in tag_set):
                    tag_set.add(labels[idx])
                    loc_candidates[_idx] = []
                    dtm_candidates[_idx] = []
                    
                """ calculate the status of each cluster: start count/end count """
                if idx % 2 == 0:
                    start_cnt_info[_idx] += 1
                else:
                    desti_cnt_info[_idx] += 1
    
                loc_candidates[_idx].append(loc_set[idx])
                dtm_candidates[_idx].append(dtm_set[idx])

        candidates = {
                      "loc_candidates": loc_candidates, 
                      "dtm_candidates": dtm_candidates,
                      "start_cnt_info": start_cnt_info,
                      "desti_cnt_info": desti_cnt_info
                     }
        return candidates


    def destinations_output(self, uid, candidates_info, MERGE_THRESHOLD=300):
        from numpy import average
        from rgeo_parser import rgeoParser
	from rugis import Distance	

        loc_candidates = candidates_info.get("loc_candidates")
        dtm_candidates = candidates_info.get("dtm_candidates")
        start_cnt_info = candidates_info.get("start_cnt_info")
        desti_cnt_info = candidates_info.get("desti_cnt_info")
        N = len(loc_candidates)
        desti_loc_record = []

        for idx in range(N):
            desti_loc = map(average, zip(*loc_candidates[idx]))
            

            """ filter the similar candidates which below MERGE_THRESHOLD """
            if idx == 0:
                desti_loc_record.append(desti_loc)
            else:
                _dis = [Distance(desti_loc[0],desti_loc[1],loc[0],loc[1]) for loc in desti_loc_record]
                if min(_dis) < MERGE_THRESHOLD:
                    continue
                else:
                    desti_loc_record.append(desti_loc)

	    dt_distri, hour_distri = zip(*map(self.time_distri, dtm_candidates[idx]))
            weekday_pref = sum([self.isWeekday(str(dt)) for dt in dt_distri])/float(len(dt_distri))
	    dt_cnt  = len(set(dt_distri))
            hour_pref = {k:round(v/float(len(hour_distri)),2) for k,v in dict(Counter(hour_distri)).iteritems()}
            desti_prob = float(desti_cnt_info[idx])/sum(desti_cnt_info) if sum(desti_cnt_info) > 0 else 0
            score = self.getScore(len(loc_candidates[idx]))
            rego_info = rgeoParser(desti_loc[0], desti_loc[1])
            rego_info = "%s\t%s\t%s\t%s\t%s" % tuple(rego_info[:-1]) if len(rego_info) > 0 else "N\tN\tN\tN\tN"
            
            print "%s\t%.6f,%.6f\t%.4f\t%.4f\t%.4f\t%s\t%s\t" % (uid, desti_loc[0], desti_loc[1], score, desti_prob, weekday_pref, dt_cnt, hour_pref) + rego_info


    def getScore(self, x):
        import math
        def sigMoid(x, base = 2):
            if x < 0:
                return 1
            else:
                return 1/(1 + math.exp(-x/float(base)))
            
        score = 2*(sigMoid(x) - .5)
        return float('%.4f' % score)


    def predict(self):
        #sys.path.append("./scipy")
        #from sklearn.cluster import MeanShift
        
        def read_input():
            for line in sys.stdin:
                yield line.strip().split()

        for uid, traj_group in groupby(read_input(), itemgetter(0)): 
            traj_group = sorted(traj_group, key=lambda d:d[1], reverse=False)
            loc_candidates = self.getInfo(traj_group, method="latlng")
            datetm = self.getInfo(traj_group, method="datetm")

            cluster_info = self.find_cluster(loc_candidates)
            candidates_info = self.candidates_info(traj_group, cluster_info)
            if candidates_info is None:
                continue

            self.destinations_output(uid, candidates_info)


if __name__ == "__main__":
    if sys.argv[1] == "--build_feature":
        trackModel(ts_margin_threshold=1800, dt_fmt="read", output_fmt="json").build_feature()
        #trackModel(ts_margin_threshold=600, dt_fmt="timestamp", output_fmt="normal").build_feature()
    elif sys.argv[1] == "--predict":
        trackModel().predict()
    else:
        pass

