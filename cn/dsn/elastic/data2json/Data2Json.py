# -*- coding: utf8 -*-
import json
import time
import os


def user_tag_2_json(source_path, save_path):
    for file_name in os.listdir(source_path):
        full_path = os.path.join(source_path, file_name)
        f = file(full_path, 'r')
        while True:
            line = f.readline()
            if len(line) == 0:
                break
            line_array = line.split("\t")
            tags = []
            for value in line_array[1:]:
                kv = value.split(":")
                try:
                    tag = {"tag": kv[0], "count": int(kv[1])}
                    tags.append(tag)
                except ValueError, e:
                    print "The value is not a number\n", e
            index = {"_id": line_array[0][5:]}
            data = {"index": index}
            tags_obj = {"@timestamp": time.ctime(), "tags": tags}

            try:
                save_file = open("{0}/{1}.json".format(save_path, file_name), 'a')
                save_file.write(json.dumps(data) + "\n" + json.dumps(tags_obj) + "\n")
            finally:
                save_file.close()

        f.close()


# 将合并后的用户标签数据转成JSON文件格式
if __name__ == '__main__':
    start = time.time()
    file_path = '/Users/Jiawei/Downloads/mydate'
    to_path = '/Users/Jiawei/Downloads/mydate'
    user_tag_2_json(file_path, to_path)
    c = time.time() - start
    print('程序运行耗时:%0.2f' % c)
