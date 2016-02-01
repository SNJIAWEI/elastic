#!/usr/bin/env python
# -*- coding: utf8 -*-

import commands
import datetime

'''
    __date__:   2016年01月04日
    __author__: jiawei
    __desc__:   标签自动化运行流程：
        1> 读取flume写入到hdfs /adlogs/parquetlog/20160201/ 数据
        2> 统一用户（userGraph data）
        3> 上下文标签（MakeContext: input data 1）
        4> 用户一天的标签聚合（input data 2&3）
'''

TIME_FMT_YMD = '%Y-%m-%d'
TIME_FMT_YMDHMS = '%Y-%m-%d %H:%M:%S'
YESTODAY = (datetime.date.today() - datetime.timedelta(days=1)).strftime(TIME_FMT_YMD)
# YESTODAY = '2015-12-31'


HDFS_PROTOCOL = 'hdfs://dsdc04:8020'
PARQUET_FILE_PATH = HDFS_PROTOCOL + '/adlogs/parquetlog/'  # bz2转换成parquet文件存放路径
DISTINCT_USER_SAVE_PATH = HDFS_PROTOCOL + '/adlogs/users/'  # 统一用户识别 数据存放路径
CONTEXT_TAGS_SAVE_PATH = HDFS_PROTOCOL + '/adlogs/tagsout/contexttags/'  # 用户上下文标签 存放路径
USERS_TAGS_MERGE_SAVE_PATH = HDFS_PROTOCOL + '/adlogs/tagsout/userstagsmerge/'  # 统一用户&上下文标签  合并存储路径
APPLICATION_JAR = '/home/hdfs/bj/data-analysis-1.0-SNAPSHOT.jar'  # 程序jar存放路径

'''
    获取当前时间 yyyy-MM-dd hh:mm:ss
'''
def get_current_time():
    return datetime.datetime.now().strftime(TIME_FMT_YMDHMS)

'''
    目标文件夹是否准备好
'''
def is_ready_file(hdfs_file_dir):
    cmd_dir_exist = "hadoop fs -ls " + hdfs_file_dir
    out = commands.getoutput(cmd_dir_exist)
    return 'No such file or directory' in out

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


''' 删除当前需要存放的文件路径 '''
def delete_current_hdfs_save_path(current_save_path):
    cmd_hdfs_delete = 'hadoop fs -rm -r ' + current_save_path
    commands.getoutput(cmd_hdfs_delete)


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
    need_process_path = PARQUET_FILE_PATH + YESTODAY.replace('-', '')
    is_ready = is_ready_file(need_process_path)
    if is_ready:
        print '%s \t %s is ready %s !' % (get_current_time(), need_process_path, is_ready)

        distinct_users(PARQUET_FILE_PATH)
        print '%s \t distinct_users successed ~!' % get_current_time()

        make_context_tag(PARQUET_FILE_PATH)
        print '%s \t distinct_users successed ~!' % get_current_time()

        users_tags_merge(CONTEXT_TAGS_SAVE_PATH, DISTINCT_USER_SAVE_PATH)
        print '%s \t users_tags_merge successed ~! \n\n' % get_current_time()
    else:
        print '%s \t %s is not ready ~!' % (get_current_time(), need_process_path)
