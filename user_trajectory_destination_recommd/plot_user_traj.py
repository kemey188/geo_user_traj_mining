import sys

def ts2datetime(timeStamp):
    import time 
    try:
        timeStamp = int(timeStamp)
        timeArray = time.localtime(timeStamp)
        timet = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
        return timet
    except Exception,e:
        return ""


print "["
for line in sys.stdin:
    if not line:
        break
    line = line.strip().split("\t")
    if len(line) != 4: break
    uid, ts, lat, lng, = line
    datet = ts2datetime(ts)

    print  "{\"id\":\"%s\", \"time\":\"%s\", \"lat\":%s, \"lng\":%s}," % (uid, datet, lat, lng)
print "{}]"

