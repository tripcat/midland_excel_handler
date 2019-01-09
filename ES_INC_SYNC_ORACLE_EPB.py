
from datetime import datetime
import os
from Oracle_Process import Oracle
import time
import re

output_file = 'EBP_SEARCH_INFO.json'
conn = Oracle('APIS','APIS','10.0.0.182:1521','apispdb')

# action = {'_index': module, '_type': doc_type, '_retry_on_conflict': '3',
# '_op_type': 'update', 'doc': {'my doc': 'here'}, 'doc_as_upsert': True}

with open('EPB_SEARCH_INFO.json','w') as f:
    sql_execute = "select * from V_ESTATE_ELASTICSEARCH"

    cur_est = conn.queryAll(sql_execute)
    key_est = ['TYPE','EST_ID','EST_ENG_NAME','EST_CHI_NAME','REGION_NAME','INET_MAJOR_DIST', 'INET_DIST', 'LOT_ALL', 'ENG_ADDR', 'CHI_ADDR', 'DELETED']
    for rec in cur_est:
        json_rec = ""
        es_prefix_string = '{"index":{"_index":"estate-search"'
        es_prefix_string = es_prefix_string + ', "_id":"'+ rec[1] + '"}}'
        for i in range(len(key_est)):
            v_rec = rec[i]
            if v_rec is None:
                v_rec = ''
            if chr(13) in v_rec:
                v_rec = v_rec.replace(chr(13),'')
                v_rec = v_rec.replace(chr(10),'')
            json_rec = json_rec + ',"' + key_est[i] + '":"' + v_rec + '"'
        json_rec = '{' + json_rec[1:] + '}\n'

        f.write(es_prefix_string + '\n')
        f.write(json_rec)

    sql_execute = "select * from V_PHASE_ELASTICSEARCH"
    cur_est = conn.queryAll(sql_execute)
    key_est = ['TYPE', 'EST_ID', 'EST_ENG_NAME', 'EST_CHI_NAME', 'REGION_NAME', 'INET_MAJOR_DIST', 'INET_DIST',
               'PHASE_ID', 'PHASE_ENG_NAME', 'PHASE_CHI_NAME', 'LOT_ALL', 'ENG_ADDR', 'CHI_ADDR', 'AD_ENG_NAME_LIST', 'AD_CHI_NAME_LIST', 'DELETED']
    for rec in cur_est:
        json_rec = ""
        es_prefix_string = '{"index":{"_index":"estate-search"'
        es_prefix_string = es_prefix_string + ', "_id":"'+ rec[7] + '"}}'
        for i in range(len(key_est)):
            v_rec = rec[i]
            if v_rec is None:
                v_rec = ''
            if chr(13) in v_rec:
                v_rec = v_rec.replace(chr(13),'')
                v_rec = v_rec.replace(chr(10),'')
            json_rec = json_rec + ',"' + key_est[i] + '":"' + v_rec + '"'
        json_rec = '{' + json_rec[1:] + '}\n'

        f.write(es_prefix_string + '\n')
        f.write(json_rec)

        f.write(es_prefix_string + '\n')
        f.write(json_rec)

    sql_execute = "select * from V_BUILDING_ELASTICSEARCH"
    cur_est = conn.queryAll(sql_execute)
    key_est = ['TYPE', 'EST_ID', 'EST_ENG_NAME', 'EST_CHI_NAME', 'REGION_NAME', 'INET_MAJOR_DIST', 'INET_DIST',
               'PHASE_ID', 'PHASE_ENG_NAME', 'PHASE_CHI_NAME', 'BLDG_ID', 'BLDG_ENG_NAME', 'BLDG_CHI_NAME', 'LOT_LIST', 'ENG_ADDR', 'CHI_ADDR', 'AD_ENG_NAME_LIST', 'AD_CHI_NAME_LIST', 'DELETED']
    for rec in cur_est:
        json_rec = ""
        es_prefix_string = '{"index":{"_index":"estate-search"'
        es_prefix_string = es_prefix_string + ', "_id":"'+ rec[10] + '"}}'
        for i in range(len(key_est)):
            v_rec = rec[i]
            if v_rec is None:
                v_rec = ''
            if chr(13) in v_rec:
                v_rec = v_rec.replace(chr(13),'')
                v_rec = v_rec.replace(chr(10),'')
            json_rec = json_rec + ',"' + key_est[i] + '":"' + v_rec + '"'
        json_rec = '{' + json_rec[1:] + '}\n'

        f.write(es_prefix_string + '\n')
        f.write(json_rec)

# with open('EPB_SEARCH_INFO.json','r+') as f:
#     f.seek(0, os.SEEK_END)
#     pos = f.tell() - 1
#     while pos > 0 and f.read(1) != "\n":
#         pos -= 1
#         f.seek(pos, os.SEEK_SET)
#     if pos > 0:
#         f.seek(pos, os.SEEK_SET)
#         f.truncate()
#

del conn


