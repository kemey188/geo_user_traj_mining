#!/bin/bash
# File Name: plot_user_traj.sh
# Created Time: 2018年01月18日 星期四 15时11分13秒

uid=$1  # user_id: 360AE74130506706
traj=./data/train_traj.txt

zcat ${traj}| grep ${uid}|python plot_user_traj.py > ./data/user_traj_solo.json

