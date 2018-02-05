The purpose of this project is to predict the destination useing user's history trajectory behavior.

* ***Build trajectory Features***

run as:

``` shell
cat data/train_traj.txt |python model.py --build_feature
```
Then, following json result is the maxmum sub trajectory features of user's track, 

``` json
{
 "uid": "ca6264e0fd903bff174c604d54b6990f", 
 "key_traj": [
  {
   "start_t": "2018-01-19 21:13:09", 
   "end_t": "2018-01-19 21:13:09", 
   "start_loc": "22.793403,113.736199", 
   "end_loc": "22.793403,113.736199", 
   "drive_minutes": 0, 
   "stop_minutes": 180, 
   "loc_cnt": 6
  }, 
  {
   "start_t": "2018-01-20 00:12:45", 
   "end_t": "2018-01-20 00:14:21", 
   "start_loc": "22.793381,113.736184", 
   "end_loc": "22.793381,113.736184", 
   "drive_minutes": 2, 
   "stop_minutes": 20, 
   "loc_cnt": 20
  }, 
  {
   "start_t": "2018-01-20 00:34:04", 
   "end_t": "2018-01-20 00:39:02", 
   "start_loc": "22.793381,113.736184", 
   "end_loc": "22.793351,113.736084", 
   "drive_minutes": 5, 
   "stop_minutes": 36, 
   "loc_cnt": 40
  }, 
  {
   "start_t": "2018-01-20 01:14:31", 
   "end_t": "2018-01-20 01:16:07", 
   "start_loc": "22.793386,113.735994", 
   "end_loc": "22.793386,113.735994", 
   "drive_minutes": 2, 
   "stop_minutes": 28, 
   "loc_cnt": 20
  }, 
  {
   "start_t": "2018-01-20 01:44:00", 
   "end_t": "2018-01-20 01:45:36", 
   "start_loc": "22.793386,113.735994", 
   "end_loc": "22.793386,113.735994", 
   "drive_minutes": 2, 
   "stop_minutes": 26, 
   "loc_cnt": 20
  }, 
  {
   "start_t": "2018-01-20 02:10:42", 
   "end_t": "2018-01-20 02:11:52", 
   "start_loc": "22.793386,113.735994", 
   "end_loc": "22.793386,113.735994", 
   "drive_minutes": 2, 
   "stop_minutes": 0, 
   "loc_cnt": 15
  }
 ]
}
```
 
* ***Predict***

Of course, you can build the feautres in normal output format. when we get the features, it's time to train our model, let's go: 

``` shell
cat data/features |python model.py --predict
```

