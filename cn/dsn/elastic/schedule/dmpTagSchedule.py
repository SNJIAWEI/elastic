#!/usr/bin/env python
# -*- coding: utf8 -*-

import commands
import os
import math
import datetime

'''
    __date__:   2016年01月04日
    __author__: jiawei
    __desc__:   标签自动化运行流程：
        1> bz2上传到hdfs
        2> bzip2parquet到HDFS目录 (input data 1)
        3> 统一用户（distinct user OR userGraph data）
        4> 上下文标签（MakeContext: input data 2）
        5> 用户一天的标签聚合（input data 3&4）

    20160201-Update:
        说明: 删除上述1和2步骤, flume已将数据对接到hdfs /adlogs/parquetlog/20160201/ 目录下, 程序直接读取即可
'''

TIME_FMT_YMD = '%Y-%m-%d'
TIME_FMT_YMDHMS = '%Y-%m-%d %H:%M:%S'
YESTODAY = (datetime.date.today() - datetime.timedelta(days=1)).strftime(TIME_FMT_YMD)
# YESTODAY = '2015-12-31'
ADLOG_END_WITH = '.log.bz2'
TODAY_FILE_NAME = YESTODAY + ADLOG_END_WITH

BZ2_FILE_PATH_ONE = '/data1/logbackup/'  # 日志存放服务器路径1
BZ2_FILE_PATH_SEC = '/data4/logbackup/'  # 日志存放服务器路径2

HDFS_PROTOCOL = 'hdfs://dsdc04:8020'
UPLOAD_BZ2_HDFS_PATH = HDFS_PROTOCOL + '/adlogs/bz2/'  # bz2压缩文件存放路径
BZ2_PARQUET_SAVE_PATH = HDFS_PROTOCOL + '/adlogs/parquetlog/'  # bz2转换成parquet文件存放路径
DISTINCT_USER_SAVE_PATH = HDFS_PROTOCOL + '/adlogs/users/'  # 统一用户识别 数据存放路径
CONTEXT_TAGS_SAVE_PATH = HDFS_PROTOCOL + '/adlogs/tagsout/contexttags/'  # 用户上下文标签 存放路径
USERS_TAGS_MERGE_SAVE_PATH = HDFS_PROTOCOL + '/adlogs/tagsout/userstagsmerge/'  # 统一用户&上下文标签  合并存储路径
APPLICATION_JAR = '/home/hdfs/bj/data-analysis-1.0-SNAPSHOT.jar'  # 程序jar存放路径

'''
    获取当前时间 yyyy-MM-dd hh:mm:ss
'''
def get_current_time():
    return time.strftime(TIME_FMT_YMDHMS, time.localtime())


'''
    根据bz2的大小计算块数,向上取整
'''
def get_bz2_should_partitions(bz2_local_path):
    if os.path.exists(bz2_local_path):
        if os.path.isfile(bz2_local_path):
            bz2_file_size = os.path.getsize(bz2_local_path) / 1024.0 / 1024.0
            return math.ceil(bz2_file_size / 256.0)
        else:
            return None
    else:
        return None


'''
    生成基础的spark-submit前缀字符串, 请在该方法返回后自行追加参数
'''
def make_spark_job(job_name, drivermem='1g', execmem='1g', execcores=1, numexec=60, memfraction=0.2, mainclass='',
                   jarpath=''):
    if len(mainclass) > 1 and len(jarpath) > 1:
        spark_submit = 'spark-submit --master yarn-client ' \
                       '--name %s ' \
                       '--driver-memory %s ' \
                       '--executor-memory %s ' \
                       '--executor-cores %d ' \
                       '--num-executors %d ' \
                       '--conf spark.storage.memoryFraction=%2.1f ' \
                       '--class %s %s ' \
                       % (job_name, drivermem, execmem, execcores, numexec, memfraction, mainclass, jarpath)
        return spark_submit
    else:
        print '%s \t make_spark_job method loss args mainclass or jarpath' % get_current_time()
        return None


''' 查看今天bz2文件是否已写入就绪 '''
def is_ready_today_file(file_path1, file_path2=''):
    file_path1 += TODAY_FILE_NAME
    file_path2 += TODAY_FILE_NAME

    if os.path.isfile(file_path1):
        return [True, file_path1]
    else:
        if os.path.isfile(file_path2):
            return [True, file_path2]
        else:
            return [False, None]


''' 删除当前需要存放的文件路径 '''
def delete_current_hdfs_save_path(current_save_path):
    cmd_hdfs_delete = 'hadoop fs -rm -r ' + current_save_path
    commands.getoutput(cmd_hdfs_delete)


'''
    将今天写入就绪的bz2文件上传到hdfs
'''
def upload_bz2_hdfs(local_file, save_path):
    cmd_mkdir = 'hadoop fs -mkdir -p %s' % UPLOAD_BZ2_HDFS_PATH
    commands.getoutput(cmd_mkdir)
    cmd_put = 'hadoop fs -put %s %s' % (local_file, save_path)
    commands.getoutput(cmd_put)


''' 将上传好的bz2文件转换成parquet文件格式 '''
def bz2parquet(bz2_local_path, bz2_hdfs_path, parquet_save_path):
    cmd_spark_submit = make_spark_job('Bzip2Parquet-' + YESTODAY.replace('-', ''),
                                      '2g', '1g', 1, 125, 0.1, 'com.lomark.tools.Bzip2Parquet', APPLICATION_JAR)

    if cmd_spark_submit is not None:
        if len(bz2_hdfs_path) > 0 and len(parquet_save_path) > 0:

            partitions_number = get_bz2_should_partitions(bz2_local_path)
            if partitions_number is not None:
                delete_current_hdfs_save_path(parquet_save_path + YESTODAY.replace('-', ''))
                cmd_spark_submit += '%s %s %d gzip ' % (bz2_hdfs_path + TODAY_FILE_NAME,
                                                        parquet_save_path + YESTODAY.replace('-', ''),
                                                        int(partitions_number))
                print '%s \t %s ' % (get_current_time(), cmd_spark_submit)
                commands.getoutput(cmd_spark_submit)
            else:
                print '%s \t bz2parquet method can not get partitions_number value !' % partitions_number
                exit()
        else:
            print '%s \t bz2parquet method [bz2_hdfs_path | parquet_save_path] args loss !' % get_current_time()
            exit()
    else:
        print '%s \t bz2parquet method [cmd_spark_submit] args loss !' % get_current_time()
        exit()


''' 统一用户识别 '''
def distinct_users(parquet_file_path):
    cmd_dist_users = make_spark_job('DistinctUsers-' + YESTODAY.replace('-', ''),
                                    '2g', '1g', 1, 125, 0.1, 'com.lomark.users.DistinctUsers', APPLICATION_JAR)
    if cmd_dist_users is not None:
        if len(parquet_file_path) > 0:
            delete_current_hdfs_save_path(DISTINCT_USER_SAVE_PATH + YESTODAY.replace('-', ''))
            cmd_dist_users += '%s %s %d ' % (parquet_file_path + YESTODAY.replace('-', ''),
                                             DISTINCT_USER_SAVE_PATH + YESTODAY.replace('-', ''), 500)
            print '%s \t %s ' % (get_current_time(), cmd_dist_users)
            commands.getoutput(cmd_dist_users)
        else:
            print '%s \t distinct_users method [parquet_file_path] args loss !' % get_current_time()
            exit()
    else:
        print '%s \t distinct_users method [cmd_dist_users] args loss !' % get_current_time()
        exit()


''' 用户上下文标签 '''


def make_context_tag(parquet_file_path):
    cmd_make_context_tag = make_spark_job('MakeContextTags-' + YESTODAY.replace('-', ''),
                                          '2g', '2g', 1, 100, 0.1, 'com.lomark.tags.MakeContextTags', APPLICATION_JAR)
    if cmd_make_context_tag is not None:
        if len(parquet_file_path) > 0:
            delete_current_hdfs_save_path(CONTEXT_TAGS_SAVE_PATH + YESTODAY.replace('-', ''))
            cmd_make_context_tag += '%s %s %d ' % (parquet_file_path + YESTODAY.replace('-', ''),
                                                   CONTEXT_TAGS_SAVE_PATH + YESTODAY.replace('-', ''), 1800)
            print '%s \t %s ' % (get_current_time(), cmd_make_context_tag)
            commands.getoutput(cmd_make_context_tag)
        else:
            print '%s \t make_context_tag method [parquet_file_path] args loss !' % get_current_time()
            exit()
    else:
        print '%s \t make_context_tag method [cmd_make_context_tag] args loss !' % get_current_time()
        exit()


''' 合并当天的用户及用户标签 '''


def users_tags_merge(context_tags_path, distinct_user_path):
    cmd_user_tag_merge = make_spark_job('UserTagsMerge-' + YESTODAY.replace('-', ''),
                                        '2g', '3g', 1, 60, 0.3, 'com.lomark.tags.UserTagsMerge', APPLICATION_JAR)
    if cmd_user_tag_merge is not None:
        if len(context_tags_path) > 0 and len(distinct_user_path) > 0:
            delete_current_hdfs_save_path(USERS_TAGS_MERGE_SAVE_PATH + YESTODAY.replace('-', ''))
            cmd_user_tag_merge += '%s %d %s %d %s' % (context_tags_path + YESTODAY.replace('-', ''), 700,
                                                      distinct_user_path + YESTODAY.replace('-', ''), 700,
                                                      USERS_TAGS_MERGE_SAVE_PATH + YESTODAY.replace('-', ''))
            print '%s \t %s ' % (get_current_time(), cmd_user_tag_merge)
            commands.getoutput(cmd_user_tag_merge)
        else:
            print '%s \t user_tag_merge method [context_tags_path | distinct_user_path] args loss !' % get_current_time()
            exit()
    else:
        print '%s \t user_tag_merge method [cmd_user_tag_merge] args loss !' % get_current_time()
        exit()


if __name__ == '__main__':
    is_ready, local_path = is_ready_today_file(BZ2_FILE_PATH_ONE, BZ2_FILE_PATH_SEC)
    message = '%s \t %s is ready %s' % (get_current_time(), TODAY_FILE_NAME, is_ready)
    if is_ready:
        message += ', the path is %s ~!' % local_path
        print message

        upload_bz2_hdfs(local_path, UPLOAD_BZ2_HDFS_PATH)
        print '%s \t upload_bz2_hdfs %s to hdfs successed ~!' % (get_current_time(), TODAY_FILE_NAME)

        bz2parquet(local_path, UPLOAD_BZ2_HDFS_PATH, BZ2_PARQUET_SAVE_PATH)
        print '%s \t bz2parquet %s successed ~!' % (get_current_time(), BZ2_PARQUET_SAVE_PATH)

        distinct_users(BZ2_PARQUET_SAVE_PATH)
        print '%s \t distinct_users successed ~!' % get_current_time()

        make_context_tag(BZ2_PARQUET_SAVE_PATH)
        print '%s \t distinct_users successed ~!' % get_current_time()

        users_tags_merge(CONTEXT_TAGS_SAVE_PATH, DISTINCT_USER_SAVE_PATH)
        print '%s \t users_tags_merge successed ~! \n\n' % get_current_time()
    else:
        print '%s \t %s is not  ready ~!' % (get_current_time(), TODAY_FILE_NAME)
