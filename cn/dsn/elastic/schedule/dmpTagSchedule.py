#!/usr/bin/env python
# -*- coding: utf8 -*-

import commands
import datetime
import os
import sys
import threading
import time

'''
    __date__:   2016年01月04日
    __author__: jiawei
    __desc__:   标签自动化运行流程：
        1> 读取flume写入到hdfs /adlogs/parquetlog/20160201/xx 数据
        2> 统一用户（userGraph data）
        3> 上下文标签（MakeContext: input data 1）
        4> 用户一天的标签聚合（input data 2&3）

        脚本使用方法:
            程序在第一次执行的时候, distinct_users方法参数 '/adlogs/tmpdir/testnull' 开启, 注释掉HISTORY_USER_SAVE_PATH
'''

TIME_FMT_YMD = '%Y-%m-%d'
TIME_FMT_YMDHMS = '%Y-%m-%d %H:%M:%S'
# YESTODAY = (datetime.date.today() - datetime.timedelta(days=1)).strftime(TIME_FMT_YMD)
YESTODAY = ''

HDFS_PROTOCOL = 'hdfs://dsdc04:8020'
PARQUET_FILE_PATH = HDFS_PROTOCOL + '/adlogs/parquetlog/'           # bz2转换成parquet文件存放路径
DISTINCT_USER_SAVE_PATH = HDFS_PROTOCOL + '/adlogs/users/current/'          # 统一用户识别 数据存放路径
HISTORY_USER_SAVE_PATH = HDFS_PROTOCOL + '/adlogs/users/history/'
ARCHIVE_USER_SAVE_PATH = HDFS_PROTOCOL + '/adlogs/users/archive/'
CONTEXT_TAGS_SAVE_PATH = HDFS_PROTOCOL + '/adlogs/tagsout/contexttags/'     # 用户上下文标签 存放路径
UNDEFINED_GEOHASH_PATH = HDFS_PROTOCOL + '/adlogs/ungeohash/'               # 未识别的经纬度存放路径
USERS_TAGS_MERGE_SAVE_PATH = HDFS_PROTOCOL + '/adlogs/tagsout/userstagsmerge/'  # 统一用户&上下文标签  合并存储路径
USERS_BLACK_LIST_PATH = HDFS_PROTOCOL + '/adlogs/users/blacklist/'              # 用户黑名单
APPLICATION_JAR = '/home/hdfs/bj/appjars/yjw/data-analysis-1.0-SNAPSHOT.jar'    # 程序jar存放路径
APPLICATION_LIB = '/home/hdfs/bj/appjars/yjw/'

'''
    获取当前时间 yyyy-MM-dd hh:mm:ss
'''
def get_current_time():
    return datetime.datetime.now().strftime(TIME_FMT_YMDHMS)

'''
    目标文件夹是否准备好
'''
def not_exist_file(hdfs_file_dir):
    cmd_hdfs_ls_dir = "hadoop fs -ls " + hdfs_file_dir
    out = commands.getoutput(cmd_hdfs_ls_dir)
    return 'No such file or directory' in out

''' 监测目录下文件是否大于1G 或者 大于1M '''
def size_greater_than_1g(hdfs_file_dir):
    cmd_hdfs_du = "hadoop fs -du -s -h " + hdfs_file_dir
    out = commands.getoutput(cmd_hdfs_du)
    return 'M' in out or 'G' in out

''' 删除当前需要存放的文件路径 '''
def delete_current_hdfs_save_path(current_save_path):
    cmd_hdfs_delete = 'hadoop fs -rm -r %s' % current_save_path
    commands.getoutput(cmd_hdfs_delete)

''' 记录下history目录下数据所属日期 '''
def record_current_hist_date(file_content):
    if not os.path.exists('/home/hdfs/'):
        os.makedirs('/home/hdfs/')
    else:
        pass
    f = open('/home/hdfs/.hdfs_hist_date', 'w')
    f.write(file_content)
    f.flush()
    f.close()

''' 读取文件记录下history存储的数据所属日期 '''
def read_hist_current_date():
    if os.path.exists('/home/hdfs/.hdfs_hist_date'):
        f = open('/home/hdfs/.hdfs_hist_date', 'r')
        hist_file_date = f.readline()
        f.close()
        return hist_file_date
    else:
        return None

'''
    检查当前用户目录生成数据是否正常, 若正常则将当前的数据所属日期记录下来
    将history中的数据归档到archive目录下, 然后将当前产出的用户数据移动到history目录下作为下次的历史滚动数据
'''
def move_hist_archive():
    if size_greater_than_1g(DISTINCT_USER_SAVE_PATH):
        archive_date = read_hist_current_date()
        if archive_date is None:
            archive_date = YESTODAY.replace('-', '')

        if not_exist_file(ARCHIVE_USER_SAVE_PATH + archive_date):
            cmd_hdfs_makedir = 'hadoop fs -mkdir -p %s' % ARCHIVE_USER_SAVE_PATH + archive_date
            commands.getoutput(cmd_hdfs_makedir)

        cmd_hdfs_mv_hist_2_arch = 'hadoop fs -mv %s %s' % (HISTORY_USER_SAVE_PATH + '*', ARCHIVE_USER_SAVE_PATH + archive_date)
        commands.getoutput(cmd_hdfs_mv_hist_2_arch)

        cmd_hdfs_mv_user_2_hist = 'hadoop fs -mv %s %s' % (DISTINCT_USER_SAVE_PATH + '*', HISTORY_USER_SAVE_PATH)
        commands.getoutput(cmd_hdfs_mv_user_2_hist)
        record_current_hist_date(YESTODAY.replace('-', ''))
        return True
    else:
        print '%s \t WARN: move_hist_archive method checked [/adlogs/users/current/] size less 1m or 1g !' % get_current_time()
        return False


''' 统一用户识别 '''
def distinct_users(parquet_file_path):
    cmd_dist_users = 'spark-submit ' \
        '--master yarn-client ' \
        '--conf spark.executor.extraJavaOptions="-XX:-UseGCOverheadLimit -XX:InitiatingHeapOccupancyPercent=35 -XX:ConcGCThreads=20 -XX:+UseG1GC" ' \
        '--conf spark.driver.memory=6g ' \
        '--conf spark.storage.memoryFraction=0.2 ' \
        '--conf spark.rdd.compress=true ' \
        '--class com.lomark.users.UserGraph ' \
        '--name %s ' \
        '--executor-memory 1g ' \
        '--executor-cores 1 ' \
        '--num-executors 120 %s '  % ("UserGraph-" + YESTODAY.replace('-', ''), APPLICATION_JAR)

    if len(parquet_file_path) > 0:
        if not not_exist_file(DISTINCT_USER_SAVE_PATH):
            delete_current_hdfs_save_path(DISTINCT_USER_SAVE_PATH)
        cmd_dist_users += ' %s %d %s %d %s %s' % (parquet_file_path + '/*/*.parquet', 2400,
                                                # '/adlogs/tmpdir/testnull', 2400, # 第一次运行时传空文件
                                               HISTORY_USER_SAVE_PATH, 2400,
                                               USERS_BLACK_LIST_PATH + YESTODAY.replace('-', ''), DISTINCT_USER_SAVE_PATH)
        print '%s \t %s ' % (get_current_time(), cmd_dist_users)
        commands.getoutput(cmd_dist_users)
    else:
        print '%s \t distinct_users method [parquet_file_path] args loss !' % get_current_time()
        exit()


''' 用户上下文标签 '''
def make_context_tag(parquet_file_path):
    cmd_make_context_tag = 'spark-submit ' \
                           '--master yarn-client ' \
                           '--name %s ' \
                           '--driver-memory 2g ' \
                           '--executor-memory 2g ' \
                           '--executor-cores 1 ' \
                           '--num-executors 80 ' \
                           '--conf spark.storage.memoryFraction=0.1 ' \
                           '--jars /home/hdfs/bj/hbase-libs/htrace-core-3.1.0-incubating.jar,' \
                           '/home/hdfs/bj/hbase-libs/hbase-common-1.1.2.2.3.4.0-3485.jar,' \
                           '/home/hdfs/bj/hbase-libs/hbase-client-1.1.2.2.3.4.0-3485.jar,' \
                           '/home/hdfs/bj/hbase-libs/hbase-prefix-tree-1.1.2.2.3.4.0-3485.jar,' \
                           '/home/hdfs/bj/hbase-libs/hbase-protocol-1.1.2.2.3.4.0-3485.jar,' \
                           '/home/hdfs/bj/hbase-libs/hbase-server-1.1.2.2.3.4.0-3485.jar,' \
                           '/home/hdfs/bj/hbase-libs/ezmorph-1.0.6.jar,' \
                           '/home/hdfs/bj/hbase-libs/geohash-java-1.0.6.jar,' \
                           '/home/hdfs/bj/hbase-libs/guava-12.0.1.jar,' \
                           '/home/hdfs/bj/hbase-libs/json-lib-2.2.2-jdk15.jar,' \
                           '/home/hdfs/bj/hbase-libs/jts-1.12.jar ' \
                           '--class com.lomark.tags.MakeContextTags %s ' % ("MakeContextTags-" + YESTODAY.replace('-', ''), APPLICATION_JAR)

    if len(parquet_file_path) > 0:
        if not not_exist_file(CONTEXT_TAGS_SAVE_PATH + YESTODAY.replace('-', '')):
            delete_current_hdfs_save_path(CONTEXT_TAGS_SAVE_PATH + YESTODAY.replace('-', ''))
        cmd_make_context_tag += '%s %s %d ' % (parquet_file_path + '/*/*.parquet', CONTEXT_TAGS_SAVE_PATH + YESTODAY.replace('-', ''), 1600)
        print '%s \t %s ' % (get_current_time(), cmd_make_context_tag)
        commands.getoutput(cmd_make_context_tag)
    else:
        print '%s \t make_context_tag method [parquet_file_path] args loss !' % get_current_time()
        exit()


''' 合并当天的用户及用户标签 '''
def users_tags_merge(context_tags_path, distinct_user_path):
    cmd_user_tag_merge = 'spark-submit ' \
                         '--master yarn-client ' \
                         '--name %s ' \
                         '--driver-memory 2g ' \
                         '--executor-memory 3g ' \
                         '--executor-cores 1 ' \
                         '--num-executors 60 ' \
                         '--conf spark.storage.memoryFraction=0.3 ' \
                         '--class com.lomark.tags.UserTagsMerge %s ' % ("UserTagsMerge-" + YESTODAY.replace('-', ''), APPLICATION_JAR)

    if len(context_tags_path) > 0 and len(distinct_user_path) > 0:
        if not not_exist_file(USERS_TAGS_MERGE_SAVE_PATH + YESTODAY.replace('-', '')):
            delete_current_hdfs_save_path(USERS_TAGS_MERGE_SAVE_PATH + YESTODAY.replace('-', ''))
        cmd_user_tag_merge += '%s %d %s %d %s' % (context_tags_path + YESTODAY.replace('-', ''), 700,
                                                  distinct_user_path, 700,
                                                  USERS_TAGS_MERGE_SAVE_PATH + YESTODAY.replace('-', ''))
        print '%s \t %s ' % (get_current_time(), cmd_user_tag_merge)
        commands.getoutput(cmd_user_tag_merge)
    else:
        print '%s \t user_tag_merge method [context_tags_path | distinct_user_path] args loss !' % get_current_time()
        exit()

'''
    将用户合并后的数据导入到hbase
    在hbase里,做用户标签的历史合并
'''
def tag_2_hbase_his_merge(pf):
    if not not_exist_file(USERS_TAGS_MERGE_SAVE_PATH + YESTODAY.replace('-', '')):
        if size_greater_than_1g(USERS_TAGS_MERGE_SAVE_PATH + YESTODAY.replace('-', '')):
            cmd_tags_2_hbase = 'java -Djava.ext.dirs=%s com.lomark.tools.Tags2H %s %s %s' % (APPLICATION_LIB,
                                                                                  YESTODAY.replace('-', ''),
                                                                                  USERS_TAGS_MERGE_SAVE_PATH + YESTODAY.replace('-', ''), pf)
            print '%s \t %s ' % (get_current_time(), cmd_tags_2_hbase)
            commands.getoutput(cmd_tags_2_hbase)
        else:
            print '%s \t tag_2_hbase_his_merge data is too small !' % get_current_time()
            exit()
    else:
        print '%s \t tag_2_hbase_his_merge file is not exist !' % get_current_time()
        exit()

# 对hbase中的数据做衰减操作
def make_final_user_tags():
    cmd_final_userTag = 'spark-submit ' \
                        '--master yarn-client ' \
                        '--name %s ' \
                        '--driver-memory 2g ' \
                        '--executor-memory 2g ' \
                        '--executor-cores 1 ' \
                        '--num-executors 70 ' \
                        '--conf spark.storage.memoryFraction=0.3 ' \
                        '--jars /home/hdfs/bj/hbase-libs/hbase-common-1.1.2.2.3.4.0-3485.jar,' \
                        '/home/hdfs/bj/hbase-libs/hbase-client-1.1.2.2.3.4.0-3485.jar,' \
                        '/home/hdfs/bj/hbase-libs/hbase-protocol-1.1.2.2.3.4.0-3485.jar,' \
                        '/home/hdfs/bj/hbase-libs/guava-12.0.1.jar ' \
                        '--class com.lomark.tags.MakeUserTags %s %s' % ('FinalUserTags-' + YESTODAY.replace('-', ''),
                                                                        APPLICATION_JAR,
                                                                        YESTODAY.replace('-', ''))
    print '%s \t %s ' % (get_current_time(), cmd_final_userTag)
    commands.getoutput(cmd_final_userTag)

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print 'Please give me one args: yyyy-MM-dd'
        exit()
    YESTODAY = sys.argv[1]
    need_process_path = PARQUET_FILE_PATH + YESTODAY.replace('-', '')
    no_exist = not_exist_file(need_process_path)
    if not no_exist:
        print '%s \t %s is ready %s ! \n' % (get_current_time(), need_process_path, not no_exist)

        distinct_users(need_process_path)
        print '%s \t distinct_users successed ~! \n' % get_current_time()

        successed = move_hist_archive()
        print '%s \t move_hist_archive %s ~! \n' % (get_current_time(), successed)

        make_context_tag(need_process_path)
        print '%s \t make_context_tag successed ~! \n' % get_current_time()

        users_tags_merge(CONTEXT_TAGS_SAVE_PATH, HISTORY_USER_SAVE_PATH)
        print '%s \t users_tags_merge successed ~! \n' % get_current_time()

        # 多线程, 开启7个线程将数据导入到Hbase中
        threads = []
        part_files = ['part-000', 'part-001', 'part-002', 'part-003', 'part-004', 'part-005', 'part-006']
        for pf in part_files:
            threads.append(threading.Thread(target=tag_2_hbase_his_merge, name=pf, args=(pf,)))

        for thread in threads:
            thread.setDaemon(True)
            thread.start()
        print '%s \t 7 threads are starting ! \n' % get_current_time()

        # 主线程等待所有子线程结束
        for childThread in threads:
            threading.Thread.join(childThread)
        print '%s \t tag_2_hbase_his_merge successed ~! \n' % get_current_time()

        # 监测活动线程数量, 当所有子线程结束了, 在控制台数据内容(主线程占1)
        # while True:
        #     if (threading.activeCount() - 1) == 0:
        #         print '%s \t tag_2_hbase_his_merge successed ~! \n' % get_current_time()
        #         break
        #     else:
        #         time.sleep(30)

        make_final_user_tags()
        print '%s \t make_final_user_tags successed ~! \n' % get_current_time()
    else:
        print '%s \t %s is not ready ~!' % (get_current_time(), need_process_path)
        exit()