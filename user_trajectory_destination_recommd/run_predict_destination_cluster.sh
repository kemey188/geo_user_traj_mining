#/usr/bin/sh
## Author: kemey@163.com
##   Date: 20180118 10:43:21

HADOOP=/usr/bin/hadoop/software/hadoop/bin/hadoop

traj_input="/the/input/user/track/raw/log/"
logparser_output="XXX/traj_recommendation/logparser"
feature_output="XXX/traj_recommendation/features"
predict_output="XXX/traj_recommendation/destination"
rugis="XXX/traj_recommendation/rugis"
aoi_cate_dict="aoi_category_maping.dict"

## 0. Build features for car tracks
#-----------------------------
function TRACK_LOGPARSER() {
  dt=$1
  ${HADOOP} fs -rmr ${logparser_output}/${dt}
  ${HADOOP} streaming \
	-Dmapred.job.name="lukaimin_user_track_features_extraction" \
	-Dmapred.success.file.status=true \
	-Dmapred.compress.map.output=true \
	-Dmapred.output.compress=true \
	-Dstream.num.map.output.key.fields=2 \
	-Dnum.key.fields.for.partition=1 \
	-Dmapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec \
	-Dmapred.job.priority=NORMAL \
	-partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
	-input ${traj_input}/${dt}/*.gz \
	-output ${logparser_output}/${dt} \
	-mapper "python logparser.py" \
	-reducer "cat" \
	-file ${rugis} \
	-file logparser.py \
	-numReduceTasks 10
}


## 1. Build features for car tracks
#-----------------------------
function BUILD_TRACK_FEATURES() {
  dt=$1
  ${HADOOP} fs -rmr ${feature_output}/${dt}
  ${HADOOP} streaming \
	-Dmapred.job.name="lukaimin_user_track_features_extraction" \
	-Dmapred.success.file.status=true \
	-Dmapred.compress.map.output=true \
	-Dmapred.output.compress=true \
	-Dstream.num.map.output.key.fields=2 \
	-Dnum.key.fields.for.partition=1 \
	-Dmapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec \
	-Dmapred.job.priority=NORMAL \
	-partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
	-input ${logparser_output}/${dt}/*.gz \
	-output ${feature_output}/${dt} \
	-mapper "./python_anaconda model.py --build_feature" \
	-reducer NONE \
	-file ${rugis} \
	-file model.py \
	-cacheArchive hdfs://hdfs_namenode:port/XXX/anaconda.tar.gz#anaconda
}


## 2. Predict Destination
#-----------------------------
function PREDICT_DESTINATION() {
  dt=$1
  dt_list=$2
  ${HADOOP} fs -rmr ${predict_output}/${dt}
  echo "[--input scale]:"
  echo -e "\t${feature_output}/${dt_set}/part-*"
  echo "-------------------"
  
  ${HADOOP} streaming \
	-Dmapred.job.name="lukaimin_user_track_destination_candidates_predict" \
	-Dmapred.success.file.status=true \
	-Dmapred.job.priority=HIGH \
	-input  ${feature_output}/${dt_list}/part-* \
	-output ${predict_output}/${dt} \
	-mapper  "cat" \
	-reducer "./python_anaconda model.py --predict" \
	-file ${rugis} \
	-file model.py \
    	-file ${aoi_cate_dict} \
	-cacheArchive hdfs://hdfs_namenode:port/XXX/anaconda.tar.gz#anaconda
}


dt=20180201
SCALE_THRESHOLD=15
TRACK_LOGPARSER ${dt}
BUILD_TRACK_FEATURES ${dt}

## input data为从$dt 往前推算15天，包含$dt
dt_set=`${HADOOP} fs -ls ${feature_output} \
		|cut -d/ -f7 \
		|awk '$1<="'${dt}'"' \
		|tail -${SCALE_THRESHOLD} \
		|tr "\n" "," \
		|sed -e "s/^/{/g" \
		|sed -e "s/,$/}/g"`

PREDICT_DESTINATION ${dt} ${dt_set}

