#! /usr/bin/env python 

#Author: kemey.RU
#Date:20160522

import sys 
from itertools import groupby 
from operator import itemgetter 
from collections import Counter
from qgis import dbscan, Distance
from sklearn.cluster import MeanShift


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
        minPts, radius, mgfactor = 5, 100, 1.5
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


def mapper():
  for line in sys.stdin:
     items = line.strip().split()
     #ACTION: split format of hql is '\001', not "\001"
     #items = line.strip().split('\001') 
     if len(items) != 7:
       continue



     #uid, ds, lat, lng, radius, type, wf, _ = items
     uid, ds, lat, lng, radius, type, wf = items
     dt = ds[0:8]
     loc_hour = ds[8:10]
     loc_time = ds[8:12]

     if not (( 17 < float(lat) < 55) and ( 70 < float(lng) < 136 )):
        continue

     print "%s\t%s\t%s\t%s\t%s" %(uid, dt, loc_hour, lat, lng)


def reducer():
   def read_input():
      for line in sys.stdin:
         yield line.strip().split()

   for uid, uidgroup in groupby(read_input(), itemgetter(0)):
      uidgroup = sorted(uidgroup, key=lambda d:d[1], reverse=True)
      loc_info = []

      for val in list(uidgroup):
         ##              lat, lng, uid, dt
         #loc_info.append([list(val)[3],list(val)[4],list(val)[0],list(val)[1]])
         lat = float(list(val)[3])
         lng = float(list(val)[4])

         loc_info.append([float('%.6f' % lat), float('%.6f' % lng)])

      #print loc_info
      minPts, radius, mgfactor = dbscanParameter(len(loc_info))

      labels = dbscan(loc_info, minPts, radius, mgfactor)
      #print labels
      clusters = findCluster(loc_info, labels, level=5)
      #clusters = findCluster(loc_info, labels, level=5, debug=True)
      #print clusters

      if clusters is None:
         clusters = [loc_info]

      clf = MeanShift(cluster_all=False, min_bin_freq=5, bandwidth=0.1)
      centers = []

      CLUSTER_CENTER_LIMITS = 150


      try:
         for i in xrange(len(clusters)):
            try:
               if clusters[i] is not None and len(clusters[i]) >= 5 :
                 clf.fit(clusters[i])
                 # choose good points
                 candidate_centers = list(clf.cluster_centers_)
                 #print candidate_centers
                 if i == 0:
                    bstCenter = list(candidate_centers[0])
                    centers.append(bstCenter)

                 if len(candidate_centers) >= 1:
                    for pos_i in candidate_centers:
                       tag_idx = 0
                       for pos_j in centers:
                          if Distance(pos_i[0],pos_i[1], pos_j[0],pos_j[1]) >= CLUSTER_CENTER_LIMITS:
                             tag_idx += 1
                          if tag_idx == len(centers):
                             centers.append(list(pos_i))
            except Exception,e:
               continue
         centers = "|".join(['%.6f,%.6f' % (p[0], p[1]) for p in centers])
         print "%s\t%s" % (uid, centers)

      except Exception,e:
         print e
         print "%s\t%s" % (uid, '')


if __name__ == "__main__":
  if sys.argv[1] == "-m":
    mapper()
  elif sys.argv[1] == "-r":
    reducer()

