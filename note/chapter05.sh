#!/bin/bash
#chapter05的工作流shell脚本


#上传需要的数据到hdfs
hadoop fs -mkdir -p /user/root/user_login
hadoop fs -put /Users/xuyongcai/IdeaProjects/hadooptrain/note/user_login.txt /user/root/user_login

#1.筛选出1月、2月的用户登陆数据
selectinput=/user/root/user_login/user_login.txt
selectoutput=/user/root/user_login/JanFeb

hadoop jar /Users/xuyongcai/IdeaProjects/hadooptrain/target/hadoop-train-1.0.jar com.xuyongcai.hadoop.chapter05.SelectData $selectinput $selectoutput


#2.日志文件统计
countinput=/user/root/user_login/JanFeb/part-m-00000
countoutput=/user/root/user_login/logcount

hadoop jar /Users/xuyongcai/IdeaProjects/hadooptrain/target/hadoop-train-1.0.jar com.xuyongcai.hadoop.chapter05.LogCount $countinput $countoutput
