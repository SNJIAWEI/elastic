import Elasticsearch
from datetime import datetime

es = Elasticsearch()
# es.index(index="my-index", doc_type="test-type", id=42, body={"any": "data", "timestamp": datetime.now()})

fileName = '/alidata1/tagsdata/part-00000'

f = file(fileName, 'r')
i = 0
while True:
    line = f.readline()
    i += 1
    if len(line) == 0:  # Zero length indicates EOF
        break
    print "Current Index file %s , rendline %d" % (fileName, i)
    arr = line.split("\t")
    # body={"tags": [{"tag": "A01", "count": 5},{"tag": "A02", "count": 10}]}
    # es.index(index="tags-index4", doc_type="test-type", id=arr[0], body={"tags": arr[1], "timestamp": datetime.now()})
    tags = []
    for a in arr[1:]:
        kv = a.split(":")
        try:
            tag = {"tag": kv[0], "count": int(kv[1])}
            tags.append(tag)
        except ValueError, e:
            print "The value is not a numbers\n", e
            continue

        # r = es.index(index="tags-index03", doc_type="test-type", id=i, body={"tags": tags, "lmid": arr[0][5:], "timestamp": datetime.now()})
        r = es.index(index="tags-index03", doc_type="test-type", id=arr[0][5:],
                     body={"tags": tags, "timestamp": datetime.now()})

        # print("===" + str(r))

print "Create Index Successed ~!"
f.close()
# es.index(index="tags-index", doc_type="test-type", id=42, body={"any": "data", "timestamp": datetime.now()})
# es.get(index="my-index", doc_type="test-type", id=42)['_sourc
