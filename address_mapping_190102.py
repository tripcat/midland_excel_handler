'''
s = '1102231990xxxxxxxx'
res = re.search('(?P<province>\d{3})(?P<city>\d{3})(?P<born_year>\d{3})',s)
print(res.groupdict())
#{'province': '110', 'city': '223', 'born_year': '199'}

line = "Cats are smarter than dogs"
matchObj = re.match( r'(.*) are (.*?) .*', line, re.M|re.I)
if matchObj:
    print("matchObj.group() : ", matchObj.group())
    print("matchObj.group(1) : ", matchObj.group(1))
    print("matchObj.group(2) : ", matchObj.group(2))
else:
    print("No match!!")
#matchObj.group() :  Cats are smarter than dogs

it = re.finditer(r"\d+","12a32bc43jf3")
for match in it:
    print (match.group() )

'''
 


import pandas as pd
import re
#from collections import namedtuple
from Oracle_Process import Oracle
#import pandas as pd

conn = Oracle('APIS','APIS','10.0.0.182:1521','apispdb')
'''
sql_execute = "select region_id, alias_name from region_alias_mstr where alias_name not in ('N','K','H')"
cur_r = conn.queryAll(sql_execute)
#df_r = pd.DataFrame(cur_r,columns=['rid','rname'])

sql_execute = "select inet_mjdist_code, eng_name, region_id from inet_major_district_mstr"
cur_m = conn.queryAll(sql_execute)

sql_execute = "select d.inet_dist_code, d.eng_name, m.inet_mjdist_code from apis.inet_district_mstr d, apis.inet_major_district_mstr m where d.inet_mjdist_id = m.inet_mjdist_id"
cur_d = conn.queryAll(sql_execute)
#df_d = pd.DataFrame(cur_d,columns=['dcode','dname'])

sql_execute = "select street_id, eng_name from street_mstr"
cur_s = conn.queryAll(sql_execute)
#df_s = pd.DataFrame(cur_s,columns=['sid','sname'])

sql_execute = "select est_id, eng_name from estate where deleted = 'N' and est_id not in ('E000015031') order by length(eng_name) desc"
cur_e = conn.queryAll(sql_execute)
#df_e = pd.DataFrame(cur_e,columns=['eid','ename'])

sql_execute = "select phase_id, est_id, eng_name from phase where deleted = 'N'"
cur_p = conn.queryAll(sql_execute)
#df_p = pd.DataFrame(cur_e,columns=['pid','pname'])

sql_execute = "select bldg_id, phase_id, est_id, eng_name, bldg_type from building where deleted = 'N'"
cur_b = conn.queryAll(sql_execute)
#df_b = pd.DataFrame(cur_b,columns=['bid','bname'])

del conn
'''


#obj_r = namedtuple('obj_r',['region_id','rname'])


# input_file = 'C:/Users/justinhu/addr.csv'
# data = pd.read_csv(r"C:\Users\justinhu\addr.csv", header = 0)
# tol_num = data.shape[0]
# for i in range(tol_num):
#     raw_addr = data.iloc[i]

#%%
import pandas as pd
import re
#from collections import namedtuple
from Oracle_Process import Oracle

def prop_data_load(conn):
    sql_execute = "select region_id, alias_name from region_alias_mstr where alias_name not in ('N','K','H')"
    cur_r = conn.queryAll(sql_execute)
    #df_r = pd.DataFrame(cur_r,columns=['rid','rname'])
    
    sql_execute = "select inet_mjdist_code, eng_name, region_id,inet_mjdist_id from inet_major_district_mstr"
    cur_m = conn.queryAll(sql_execute)
    
    sql_execute = "select d.inet_dist_code, d.eng_name, m.inet_mjdist_code, d.inet_dist_id, m.inet_mjdist_id from apis.inet_district_mstr d, apis.inet_major_district_mstr m where d.inet_mjdist_id = m.inet_mjdist_id"
    cur_d = conn.queryAll(sql_execute)
    #df_d = pd.DataFrame(cur_d,columns=['dcode','dname'])
    
    sql_execute = "select street_id, eng_name from street_mstr"
    cur_s = conn.queryAll(sql_execute)
    #df_s = pd.DataFrame(cur_s,columns=['sid','sname'])
    
    sql_execute = "select est_id, eng_name, inet_dist_id, inet_mjdist_id  from apis.estate where deleted = 'N' and eng_name not in ('TEMP','TEST','temp','LAND','...','XXX','.','A','--','---') order by length(eng_name) desc"
    cur_e = conn.queryAll(sql_execute)
    #df_e = pd.DataFrame(cur_e,columns=['eid','ename'])
    
    sql_execute = "select phase_id, est_id, eng_name from phase where deleted = 'N'"
    cur_p = conn.queryAll(sql_execute)
    #df_p = pd.DataFrame(cur_e,columns=['pid','pname'])
    
    sql_execute = "select bldg_id, phase_id, est_id, eng_name, bldg_type from building where deleted = 'N'"
    cur_b = conn.queryAll(sql_execute)
    
    sql_execute = "select est_id, phase_id, BUILDING_NAME_EB from apis.SB_EST_PHASE_INFO_EN where phase_id not in (' ','P999999')"
    cur_eb_p = conn.queryAll(sql_execute)
    return cur_r,cur_m,cur_d,cur_s,cur_e,cur_p,cur_b,cur_eb_p
    

def addr_parse(namestr,cur_r,cur_m,cur_d,cur_e,cur_p,cur_b,cur_eb_p):
    v_region_id = ''
    v_str_region = ''
    v_str_mjdist = ''
    v_mjdist_code = ''
    v_str_dist = ''
    v_dist_code = ''
    v_str_lot_full = ''
    v_str_dd_full = ''
    v_str_estate = ''
    v_est_id = ''
    v_str_phase = ''
    v_phase_id = ''
    v_str_street_full = ''
    v_str_bldg_full = ''
    v_bldg_id = ''
    v_str_floor_full = ''
    v_str_floor_net = ''
    v_str_unit_full = ''
    v_str_unit_net = ''
    v_has_carpark = ''
    v_has_roof = ''
    unparse_namestr = ''
    
    inet_dist_id = ''
    inet_mjdist_id = ''
    
    namestr = namestr.strip()
    namestr = namestr.replace('"','')
    #single_parts = namestr.split(' ')
    single_parts = re.split(r'[,\s]',namestr)
    single_parts = [i for i in single_parts if i != '']
    #re.split(r'[!@%^],c)
    
    #region
    if len(single_parts) > 1:
        region_part = single_parts[-2] + ' ' + single_parts[-1]
    else:
        region_part = single_parts[-1]
        
    trim_region_part = region_part.replace(' ','')
    
    v_str_region = ""
    v_region_id = ""
    
    for item in cur_r:
        rname = item[1]
        if region_part.find(rname)>=0:
            v_str_region = rname
            v_region_id = item[0]
            break
    
    newpart = trim_region_part.replace(v_str_region.replace(' ',''),'')
    if len(single_parts) > 1:
        single_parts[-2] = newpart
        single_parts[-1] = ''
    else:
        single_parts[-1] = newpart
        
    unparse_namestr = ' '.join(single_parts)  
    
    #major district + district
    single_parts = re.split(r'[,\s]',unparse_namestr)
    single_parts = [i for i in single_parts if i != '']
    mjdist_part = ''
    
    if len(single_parts) > 2:
        mjdist_part = single_parts[-3] + ' ' + single_parts[-2] + ' ' + single_parts[-1]
    elif len(single_parts)  == 0:
        mjdist_part = ''        
    else:
        for i in range(len(single_parts)):
            idx = -(len(single_parts) - i)
            mjdist_part += ' ' + single_parts[idx]
        
    trim_mjdist_part = mjdist_part.replace(' ','')
    v_mjdist_code = ''
    
    v_str_mjdist = ''
    for item in cur_m:
        mname = item[1]
        if trim_mjdist_part.find(mname.replace(' ',''))>=0:
            v_str_mjdist = mname
            v_mjdist_code = item[0]
            inet_mjdist_id = item[3]
            break
    
    ISLAND_DIST_LIST = ["LAMMA ISLAND","CHEUNG CHAU","PENG CHAU","HEI LING CHAU"]
    
    if v_str_mjdist == '':
        v_str_dist = ''
        v_dist_code = ''
        
        for island_dist in ISLAND_DIST_LIST:
            if trim_mjdist_part.find(island_dist.replace(' ','')) >=0:
                v_str_dist = island_dist
                v_dist_code = 'LT / ISL'
                break
        
        if v_dist_code == '':    
            for item in cur_d:
                dname_raw_full = item[1]
                dname_list = re.split(r'[/|\(]',dname_raw_full)
                for dname_raw in dname_list:
                    dname_raw = dname_raw.replace('STATION','')
                    dname_raw = dname_raw.replace(')','')
                    dname = dname_raw.replace(' ','')
                    if trim_mjdist_part.find(dname)>=0:
                        v_str_dist = dname_raw
                        v_dist_code = item[0]
                        inet_dist_id = item[3]
                        break
                    
        if v_str_dist != '':
            newpart = trim_mjdist_part.replace(v_str_dist.replace(' ',''),'')
            #MUIWOLANTAU MUIWOLANTAU
            if len(single_parts) > 2:
                single_parts[-3] = newpart
                single_parts[-2] = ''
                single_parts[-1] = ''
            elif len(single_parts)  == 0:
                pass        
            else:
                for i in range(len(single_parts)):
                    idx = -(len(single_parts) - i)
                    if i == 0:
                        single_parts[idx] = newpart  
                    else:
                        single_parts[idx] = ''
                        
            unparse_namestr = ' '.join(single_parts)   
            
    else:
        m_cur_d = [d for d in cur_d if d[2] == v_mjdist_code]
        
        newpart = trim_mjdist_part.replace(v_str_mjdist.replace(' ',''),'')
    
        if len(single_parts) > 2:
            single_parts[-3] = newpart
            single_parts[-2] = ''
            single_parts[-1] = ''
        elif len(single_parts)  == 0:
            pass        
        else:
            for i in range(len(single_parts)):
                idx = -(len(single_parts) - i)
                if i == 0:
                    single_parts[idx] = newpart  
                else:
                    single_parts[idx] = ''
                    
        unparse_namestr = ' '.join(single_parts) 
    
        single_parts = re.split(r'[,\s]',unparse_namestr)
        single_parts = [i for i in single_parts if i != '']
        
        dist_part = ''
        if len(single_parts) > 2:
            dist_part = single_parts[-3] + ' ' + single_parts[-2] + ' ' + single_parts[-1]
        elif len(single_parts)  == 0:
            dist_part = ''
        else:
            for i in range(len(single_parts)):
                idx = -(len(single_parts) - i)
                dist_part += ' ' + single_parts[idx]
        
        trim_dist_part = dist_part.replace(' ','')
    
        v_str_dist = ''
        v_dist_code = ''
        for island_dist in ISLAND_DIST_LIST:
            if trim_dist_part.find(island_dist.replace(' ','')) >=0:
                v_str_dist = island_dist
                v_dist_code = 'LT / ISL'
                break    
        
        if v_dist_code == '':  
            for item in m_cur_d:
                dname_raw_full = item[1]
                dname_list = re.split(r'[/|\(]',dname_raw_full)
                for dname_raw in dname_list:
                    dname_raw = dname_raw.replace('STATION','')
                    dname_raw = dname_raw.replace(')','')
                    dname = dname_raw.replace(' ','')
                    if trim_dist_part.find(dname) >=0:
                        v_str_dist = dname_raw
                        v_dist_code = item[0]
                        inet_dist_id = item[3]
                        break
        '''            
        if v_str_dist != '':
            newpart = trim_dist_part.replace(v_str_dist.replace(' ',''),'')
            if len(single_parts) > 2:
                single_parts[-3] = newpart
                single_parts[-2] = ''
                single_parts[-1] = ''
            elif len(single_parts)  == 0:
                pass        
            else:
                for i in range(len(single_parts)):
                    idx = -(len(single_parts) - i)
                    if i == 0:
                        single_parts[idx] = newpart  
                    else:
                        single_parts[idx] = ''
                        
            unparse_namestr = ' '.join(single_parts)       
        '''
        
    if v_str_dist == '' and v_str_mjdist == '' and v_str_region == '':
        unparse_namestr = namestr            
         
    #unit
    res = re.search('(?P<raw_unit>(ROOM|UNIT|FLAT)\s\w*)',unparse_namestr)
    if res is not None:
        v_str_unit_full = res.groupdict()['raw_unit']
        unparse_namestr = unparse_namestr.replace(v_str_unit_full,'') 
        v_str_unit_net = re.sub('(ROOM|UNIT|FLAT)\s', '', v_str_unit_full)
    else:
        v_str_unit_full = ''    
        v_str_unit_net = ''
    
    #floor
    unparse_namestr = unparse_namestr.replace("FIRST FLOOR","1ST FLOOR")
    unparse_namestr = unparse_namestr.replace("SECOND FLOOR","2ND FLOOR")
    unparse_namestr = unparse_namestr.replace("THIRD FLOOR","3RD FLOOR")
    
    res = re.search('(?P<raw_floor>(\w*/F(LOOR)?\s?)|((1ST|2ND|3RD|\d+TH|\d+|GROUND)\sFLOOR))',unparse_namestr)
    if res is not None:
        v_str_floor_full = res.groupdict()['raw_floor']
        unparse_namestr = unparse_namestr.replace(v_str_floor_full,'') 
        v_str_floor_net = re.sub('(/F(LOOR)?\s)|((ST|ND|RD|TH|ROUND)?\sFLOOR)', '', v_str_floor_full)   
    else:
        v_str_floor_full = ''
        v_str_floor_net = ''    

    #LOT
    v_str_dd_full = ''
    v_str_lot_full = ''
    v_str_lot_ext_full = ''

    while re.search('(?P<raw_lot>LOTS?\sNOS?\.?\s?\d+(\s?(&|AND)\s?\d+)?)',unparse_namestr) is not None:
        res = re.search('(?P<raw_lot>LOTS?\sNOS?\.?\s?\d+(\s?(&|AND)\s?\d+)?)',unparse_namestr)
        v_str_lot_single = res.groupdict()['raw_lot']
        unparse_namestr = unparse_namestr.replace(v_str_lot_single,'')
        v_str_lot_full += v_str_lot_single + ','
    v_str_lot_full = v_str_lot_full[:-1]
        
    if v_str_lot_full != '':
        while re.search('(?P<raw_lotext>((S\.|SECTION\s)[A-N]|(S\.?S\.\s?|SUB-SECTION\s|SUB-SEC\.)\d+|(R\.?P\.|REMAINING\sPORTION\s)|(EX\.|EXTENSION))\s?)',unparse_namestr) is not None:
             res = re.search('(?P<raw_lotext>((S\.|SECTION\s)[A-N]|(S\.?S\.\s?|SUB-SECTION\s|SUB-SEC\.)\d+|(R\.?P\.|REMAINING\sPORTION\s)|(EX\.|EXTENSION))\s?)',unparse_namestr)
             v_str_lot_ext_single = res.groupdict()['raw_lotext']
             unparse_namestr = unparse_namestr.replace(v_str_lot_ext_single,'')
             v_str_lot_ext_single = v_str_lot_ext_single.replace('SUB-SECTION ','SS.')
             v_str_lot_ext_single = v_str_lot_ext_single.replace('SUB-SEC.','SS.')
             v_str_lot_ext_single = v_str_lot_ext_single.replace('S.S.','SS.')
             v_str_lot_ext_single = v_str_lot_ext_single.replace('SECTION ','S.')
             v_str_lot_ext_single = v_str_lot_ext_single.replace('REMAINING PORTION ','R.P.')
             v_str_lot_ext_single = v_str_lot_ext_single.replace('EXTENSION','EX.')
            
             v_str_lot_ext_full += v_str_lot_ext_single 
    if v_str_lot_ext_full != '' and v_str_lot_ext_full[-1] in (' ',','):
        v_str_lot_ext_full = v_str_lot_ext_full[:-1]
    
    res = re.search('(?P<raw_DD>D\.?D\.?\s?(NO\.?\s)?\d+[L]?\s?)',unparse_namestr)
    if res is not None:
         v_str_dd_full = res.groupdict()['raw_DD']
         unparse_namestr = unparse_namestr.replace(v_str_dd_full,'')
    else:
         v_str_dd_full = ''
    
    #estate + phase
    v_str_estate = ''
    v_str_phase = ''
    v_est_id = ''
    v_phase_id = ''
    
    
    for item in cur_e:
        ename = ' ' + item[1] 
        #print(rname)
        if unparse_namestr.find(ename)>=0:
            #print(item[0])
            v_str_estate = ename.lstrip()
            v_est_id = item[0]
            break
        
    if v_str_estate != '':
        unparse_namestr = unparse_namestr.replace(v_str_estate,'')  
        e_cur_p = [p for p in cur_p if p[1] == v_est_id]
        if len(e_cur_p) > 0:
            v_str_phase = ''
            for item in e_cur_p:
                pname = ' ' + item[2]
                if unparse_namestr.find(pname)>=0:
                    v_str_phase = pname.lstrip()
                    v_est_id = item[1]
                    v_phase_id = item[0]
                    break
        
            if v_str_phase != '':
                unparse_namestr = unparse_namestr.replace(v_str_phase,'')  
                    
        else:
            v_str_phase = ''
            res = re.search('(?P<raw_phase>(PHASE\s.*\s))',unparse_namestr)
            if res is not None:
                v_str_phase = res.groupdict()['raw_phase']
                unparse_namestr = unparse_namestr.replace(v_str_phase,'')     
    
    else:
        for item in cur_eb_p:
            pname = ' ' + item[2]
            extname = re.search(r"(?P<extra_str>\(.*\))",item[2])
            if extname is not None:
                extname = extname.groupdict()['extra_str']
                net_pname = pname.replace(extname,'')
                net_pname = net_pname.rstrip()
                
            if unparse_namestr.find(pname)>=0:
                v_str_phase = pname.lstrip()
                v_est_id = item[1]
                v_phase_id = item[0]
                break
        
            if v_str_phase == '' and extname is not None:
                if unparse_namestr.find(net_pname)>=0:
                    v_str_phase = pname.lstrip()
                    v_est_id = item[1]
                    v_phase_id = item[0]            
            
        if v_str_phase != '':
            unparse_namestr = unparse_namestr.replace(v_str_phase,'')  
        
    #bldg
    v_str_bldg_full = ''
    v_bldg_id = ''
    
    res = re.search('(?P<raw_bldg>(TOWER|BLOCK|HOUSE)\s(NO\.)?\s?\w*\s)',unparse_namestr)  #HOUSE NO. 3 
    if res is not None:
        v_str_bldg_raw = res.groupdict()['raw_bldg']
        v_str_bldg_raw_map = v_str_bldg_raw.replace("NO. ","").strip()
        v_str_bldg_raw_map = ' ' + v_str_bldg_raw_map.replace("NO.","")
    else:
        v_str_bldg_raw = ''
        v_str_bldg_raw_map = ''
        
    if v_phase_id != '':
        if v_str_unit_full != '':
            b_cur_pe = [b for b in cur_b if b[1] == v_phase_id and b[4] not in ('CARPARK','SHOP')]
        else:
            b_cur_pe = [b for b in cur_b if b[1] == v_phase_id]
            
        if len(b_cur_pe) == 1:
            v_str_bldg_full = b_cur_pe[0][3]
            v_bldg_id = b_cur_pe[0][0]
            
        elif len(b_cur_pe) > 0:
            for item in b_cur_pe:
                bname = ' ' + item[3]
                if unparse_namestr.find(bname)>=0  or (bname.find(v_str_bldg_raw_map) >=0 and v_str_bldg_raw != ''):
                    v_str_bldg_full = bname.lstrip()
                    v_bldg_id = item[0]
                    break
        
    elif v_est_id != '':    
        if v_str_unit_full != '':
            b_cur_pe = [b for b in cur_b if b[2] == v_est_id and b[4] not in ('CARPARK','SHOP')]
        else:
            b_cur_pe = [b for b in cur_b if b[2] == v_est_id]
    
        if len(b_cur_pe) == 1:
            v_str_bldg_full = b_cur_pe[0][3]
            v_bldg_id = b_cur_pe[0][0]
        elif len(b_cur_pe) > 0:
            for item in b_cur_pe:
                bname = ' ' + item[3]
                if unparse_namestr.find(bname)>=0 or (bname.find(v_str_bldg_raw_map) >=0 and v_str_bldg_raw != ''):
                    v_str_bldg_full = bname.lstrip()
                    v_bldg_id = item[0]
                    break
        
    if v_str_bldg_full == '':   
        if v_str_bldg_raw != '':
            v_str_bldg_full = v_str_bldg_raw
        else:
            res = re.search('(?P<raw_bldg>\s\w*\s(COURT|MANSION|BUILDING|HOUSE))',unparse_namestr)
            if res is not None:
                v_str_bldg_full = res.groupdict()['raw_bldg']
            else:    
                v_str_bldg_full = ''
    
    if v_str_bldg_full != 'CARPARK':
        unparse_namestr = unparse_namestr.replace(v_str_bldg_full,'') 
    
    unparse_namestr = unparse_namestr.replace(v_str_bldg_raw,'')     

    #STREET    
    res = re.search('(?P<raw_street>(NOS?\.)?\s?\d+(\s?-\s?\d+)?\s(\(.*\)\s)?[^\d]*(STREET|ROAD|LANE))',unparse_namestr)
    if res is not None:
        v_str_street_full = res.groupdict()['raw_street']
        unparse_namestr = unparse_namestr.replace(v_str_street_full,'') 
    else:
        res = re.search('(?P<raw_street_pre>(NO(S)?\.)?\s?\d+[a-gA-G]?(-\s?\d+)?\s?(\((\w|\s)*\)\s?)?[^(\d|/)]*)',unparse_namestr)
        if res is not None:
            v_str_street_pre = res.groupdict()['raw_street_pre']
            unparse_namestr = unparse_namestr.replace(v_str_street_pre,'') 
            v_str_street_full = v_str_street_pre.strip() + ' ' + v_str_estate
        else:
            v_str_street_full = ''
        
    
    #carpark
    res = re.search('(?P<raw_carpark>CARPARK)',unparse_namestr)
    if res is not None:
        v_has_carpark = 1
    else:
        v_has_carpark = 0    
    
    #roof
    res = re.search('(?P<raw_roof>ROOF)',unparse_namestr)
    if res is not None:
        v_has_roof = 1 
    else:
        v_has_roof = 0
    
    v_tx_type = ''
    if v_str_lot_full != '':
        v_tx_type = 'LOT'
    else:
        if v_has_carpark == 1 and v_str_unit_full == '':
            v_tx_type = 'CP'

        
    if v_est_id != '' and v_dist_code == '':
        inet_dist_id  = [e[2] for e in cur_e if e[0] == v_est_id]
        if len(inet_dist_id) > 0:
            if inet_dist_id[0] is not None:
                inet_dist_id = inet_dist_id[0]
                v_dist_code_item = [d for d in cur_d if d[3] == inet_dist_id]  
                if len(v_dist_code_item) > 0:
                    v_dist_code_item = v_dist_code_item[0]
                    v_dist_code = v_dist_code_item[0]
                    v_mjdist_code = v_dist_code_item[2]
        
        
    if v_mjdist_code == '' and v_dist_code != '':
        v_mjdist_code = [d[2] for d in cur_d if d[0] == v_dist_code]
        v_mjdist_code = v_mjdist_code[0]
        
    if v_region_id == '' and v_mjdist_code != '':
        v_region_id = [m[2] for m in cur_m if m[0] == v_mjdist_code]
        v_region_id = v_region_id[0]

    
    return v_region_id, v_str_region, v_str_mjdist, v_mjdist_code, v_str_dist, v_dist_code, \
            v_str_lot_full, v_str_lot_ext_full, v_str_dd_full, v_str_estate, v_est_id, v_str_phase, v_phase_id, v_str_street_full, \
            v_str_bldg_full, v_bldg_id, v_str_floor_full, v_str_floor_net, v_str_unit_full, v_str_unit_net, v_has_carpark, v_has_roof, unparse_namestr



def unit_exact_match(conn, v_est_id, v_phase_id, v_bldg_id, v_str_floor_net, v_str_unit_net):
    query_str = 'select u.unit_id, u.floor, u.flat, u.prn_no, u.unit_type_id, u.bldg_id, u.phase_id, b.eng_name as bldg_eng_name, p.eng_name as phase_eng_name ' \
                'from unit u, building b, phase p ' \
                'where u.bldg_id = b.bldg_id and u.phase_id = p.phase_id(+)'
    
    query_bldg = ''
    query_floor = ''
    query_flat = ''
    map_status = ''
    
    cur_u = []
    unit_num = 0
    
    if v_est_id not in('','DUP_ESTNAME'):
        query_str += " and u.est_id ='" + v_est_id + "'"
        if v_phase_id != '':
            query_str += " and u.phase_id ='" + v_phase_id + "'"
            
        if v_bldg_id != '':
            query_bldg = query_str + " and u.bldg_id ='" + v_bldg_id + "'"  
            cur_bldg = conn.queryAll(query_bldg)
            unit_num_bldg = len(cur_bldg)
        else:
            query_bldg = query_str
            cur_bldg = []
            unit_num_bldg = 0
        
        if v_str_floor_net != '':
            query_floor =  query_bldg + " and u.floor ='" + v_str_floor_net + "'"   
            cur_floor = conn.queryAll(query_floor)
            unit_num_floor = len(cur_floor)
        else:
            query_floor =  query_bldg
            cur_floor = []
            unit_num_floor = 0
            
        if v_str_unit_net != '':
            query_flat = query_floor + " and u.flat ='" + v_str_unit_net + "'"  
            cur_flat = conn.queryAll(query_flat)
            unit_num_flat = len(cur_flat)
        else:
            query_flat = query_floor
            cur_flat = []
            unit_num_flat = 0

        if unit_num_flat == 1:
            cur_u = cur_flat
            unit_num = 1
            map_status = 'Full Mapped - Flat'
        if unit_num_floor == 1:
            cur_u = cur_floor
            unit_num = 1
            map_status = 'Full Mapped - Floor'            
        if unit_num_bldg == 1:
            cur_u = cur_bldg
            unit_num = 1        
            map_status = 'Full Mapped - Bldg'
            
        if unit_num == 0:
            if unit_num_flat != 0:
                cur_u = cur_flat
                unit_num = unit_num_flat
                map_status = "Multi - Flat"
            elif unit_num_floor != 0:
                cur_u = cur_floor
                unit_num = unit_num_floor
                map_status = "Multi - Floor"
            elif unit_num_bldg !=0:
                cur_u = cur_bldg
                unit_num = unit_num_bldg
                map_status = "Multi - Bldg"                
        
        if unit_num == 0:
            cur_u = conn.queryAll(query_str)
            unit_num = len(cur_u)
            if unit_num == 1:     
                map_status = 'Full Mapped - Est'                
            elif unit_num > 1:
                map_status = 'Multi - Est'
            else:  
                map_status = 'No Units'
    elif v_est_id == '':
        map_status = 'No Estate Info'
    else:
        map_status = 'Duplicate Estate Name'
        
    if unit_num == 1:
        unit_id = cur_u[0][0]
    else:
        unit_id = '' 
    return cur_u, unit_num, map_status, unit_id
#    
    
#%%
conn = Oracle('APIS','APIS','10.0.0.182:1521','apispdb')
cur_r,cur_m,cur_d,cur_s,cur_e,cur_p,cur_b,cur_eb_p =  prop_data_load(conn) 

l1 = "FLAT B4 ON 1/F WITH PORTION OF FLAT ROOF OF MUI WO CENTRE, NO.3 NGAN WAN ROAD, MUI WO, LANTAU"
l12 = "1/F NO.7C TAI SHEK HAU CHEUNG CHAU NEW TERRITORIES"
l2 = "FLAT 4 ON 22/F OF BLOCK B OF HILTON PLAZA, NO.3-9 SHA TIN CENTRE STREET, SHATIN"
l3 = "2/F & ROOF DD3, LAMMA ISLAND LOT NO.1933 LAMMA ISLAND NEW TERRITORIES"
l4 = "LOT NO.2804 IN DD316"
l5 = "FLAT B4 ON 2ND FLOOR (TOGETHER WITH THE FLAT ROOF SPACE AS SHOWN ON THE PLAN ANNEXED TO ASSIGNMENT M/N UB344660 & THEREON COLOURED GREEN & ALSO WITH THE FLAT ROOF SPACE AS SHOWN ON THE PLAN & THEREON COLOURED BLUE HATCHED RED) TSIMSHATSUI MANSION NOS. 83-"
l6 = "UNIT D ON 2ND FLOOR INCLUDING THE BALCONY(IES), THE UTILITY PLATFORM(S) AND A/C PLATFORM(S) OF COURT B (???? TOWER 1 DRAGONS RANGE NO. 33 LAI PING ROAD SHA TIN NEW TERRITORIES"
l7 = "RESIDENTIAL CARPARKING SPACE NO. 130 ON LOWER GROUND 1 FLOOR THE CORONATION NO. 1 YAU CHEUNG ROAD KOWLOON"
l8 = "FLAT A ON 17TH FLOOR OF CENTRAL HEIGHTS NO.9 TONG TAK STREET TSEUNG KWAN O SAI KUNG NT"
l9 = "FLAT 5 ON 21/F OF BLOCK B  KING MING COURT  NOS.2-6 (NO.6) TSUI LAM ROAD  TSEUNG KWAN O SAI KUNG N.T."
l0 = "SECOND FLOOR INCLUDING THE BALCONY APPURTENANT THERETO  TOGETHER WITH THE ROOF THEREABOVE  AND THE STAIRCASES LEADING FROM THE FIRST FLOOR UP TO THE ROOF OF THE BUILDING ERECTED ON THE SECTION A OF LOT NO.1173 IN D.D.123  NO.208B WANG CHAU FUK HING TSUEN"
la = "HOUSE NO. 3 WITH GARDEN OPEN COURT AND GARAGE OF SILVER VIEW LODGE, SAI KUNG, NEW TERRITORIES."
lb = "LOT NO. 951 IN D.D. NO. 215FLAT A3 ON5TH FLOOR AND ROOF OF KO SHING HOUSE, SAI KUNG,N.T."
lc = "2/F & MAIN ROOF LOT NO.424 IN DD236 SAI KUNG NEW TERRITORIES"   
ld = "1ST FLOOR OF NO. 88C SAI KUNG ROAD, SAI KUNG, N.T."
le = "HOUSE NO. 3 OF CAPITAL VILLA"
lf = "GROUND FLOOR OF NO. 10D HOI PONG STREET, SAI KUNG, N.T."
lg = "SECOND FLOOR INCLUDING THE BALCONY AND THE ROOF, 269 SHEK WU TONG, YUEN LONG, N.T."
lh = "FLAT A1 ON 3RD FLOOR OF KO FU HOUSE, 42-56 FUK MAN ROAD, SAI KUNG, N.T."
l11 = "G/F LOT NOS.263 & 264 IN DD206 SHATIN NEW TERRITORIES"
l12 = "FIRST FLOOR S.A OF YUN KONG TSUN LOT NO.107 AND YUN KONG TSUN LOT NO.108 BOTH IN DD106, YUEN LONG NEW TERRITORIES"
l13 = "2/F & ROOF SS.2 OF S.A OF LOT NO.2263 IN DD120 YUEN LONG NEW TERRITORIES"
l14 = "FLAT C ON THE 7TH FLOOR OF BLOCK E OF SAI KUNG TOWN CENTRE, NOS. 22- 40 FUK MAN ROAD, SAI KUNG, N. T."
l15 = "2/F AND ROOF SUB-SECTION 2 OF SECTION B OF LOT NO.1284 IN DD222 SAI KUNG NEW TERRITORIES"
l16 = "LOT NO. 88S.B S.S.5 R.P.IN D.D. NO. 2151ST FLOOR ON THE SAID BUILDING ERECTED, SAI KUNG, N.T."
l17 = "1/3 SHARE OF LOTS NOS. 15S.A, 252 AND 254 ALL IN D.D. NO. 213"
namestr = l16
v_region_id, v_str_region, v_str_mjdist, v_mjdist_code, v_str_dist, v_dist_code, v_str_lot_full, v_str_lot_ext_full, \
v_str_dd_full, v_str_estate, v_est_id, v_str_phase, v_phase_id, v_str_street_full, v_str_bldg_full, v_bldg_id,\
v_str_floor_full, v_str_floor_net, v_str_unit_full, v_str_unit_net, v_has_carpark, v_has_roof, unparse_namestr = addr_parse(namestr,cur_r,cur_m,cur_d,cur_e,cur_p,cur_b,cur_eb_p)
 
cur_u, unit_num, map_status, unit_id = unit_exact_match(conn, v_est_id, v_phase_id, v_bldg_id, v_str_floor_net, v_str_unit_net)

#%%
#input_file = 'C:/Users/justinhu/addr.csv'
#from itertools import izip
import csv
import time

conn = Oracle('APIS','APIS','10.0.0.182:1521','apispdb')
cur_r,cur_m,cur_d,cur_s,cur_e,cur_p,cur_b,cur_eb_p =  prop_data_load(conn)   

input_file = 'C:/Users/justinhu/addr_48K.csv'
output_file = 'C:\Justin\APIS\midland_excel_handler\parsed_addr.csv'
data = pd.read_csv(input_file, header = 0)
#tol_num = data.shape[0]
start_time = time.time()

with open(output_file,'w') as csvfile:
    counter = 0
    tol_num = 0
    thr = 2000
    writer = csv.writer(csvfile, dialect='unix')

    header_list = ['ADDR_ENG','MEMO_NO','REGISTRY_CODE','DELIVERY_DATE','v_region_id','v_str_region','v_str_mjdist','v_mjdist_code','v_str_dist','v_dist_code','v_str_lot_full','v_str_lot_ext_full',\
              'v_str_dd_full','v_str_estate','v_est_id','v_str_phase','v_phase_id','v_str_street_full','v_str_bldg_full','v_bldg_id',\
              'v_str_floor_full','v_str_floor_net','v_str_unit_full','v_str_unit_net','unit_id','v_has_carpark','v_has_roof','map_status','unparse_namestr',\
              'unit_num']
    
    writer.writerow(header_list)
    
    for row in zip(data.index, data['ADDR_ENG'], data['MEMO_NO'], data['REGISTRY_CODE'], data['DELIVERY_DATE']):
    
        raw_addr = row[1]
        
        v_region_id, v_str_region, v_str_mjdist, v_mjdist_code, v_str_dist, v_dist_code, v_str_lot_full, v_str_lot_ext_full,\
        v_str_dd_full, v_str_estate, v_est_id, v_str_phase, v_phase_id, v_str_street_full, v_str_bldg_full, v_bldg_id,\
        v_str_floor_full, v_str_floor_net, v_str_unit_full, v_str_unit_net, v_has_carpark, v_has_roof, unparse_namestr = addr_parse(raw_addr,cur_r,cur_m,cur_d,cur_e,cur_p,cur_b,cur_eb_p)
        
        cur_u, unit_num, map_status, unit_id = unit_exact_match(conn, v_est_id, v_phase_id, v_bldg_id, v_str_floor_net, v_str_unit_net)
        
        row_list = [row[1],row[2],row[3],row[4],v_region_id, v_str_region, v_str_mjdist, v_mjdist_code, v_str_dist, v_dist_code, v_str_lot_full, v_str_lot_ext_full,\
        v_str_dd_full, v_str_estate, v_est_id, v_str_phase, v_phase_id, v_str_street_full, v_str_bldg_full, v_bldg_id,\
        v_str_floor_full, v_str_floor_net, v_str_unit_full, v_str_unit_net, unit_id, v_has_carpark, v_has_roof, map_status, unparse_namestr, unit_num]

       
        writer.writerow(row_list)
        #print(unit_id)

        counter += 1
        tol_num += 1
        if counter == thr:
            print("%d records have been parsed." % tol_num)
            counter = 0
    
    end_time = time.time()
    elapsed_time = round(end_time - start_time, 2)    
    print("Parse Completed. Total %d records have been parsed. Elasped time: %f seconds" % (tol_num,elapsed_time))
    
del conn


#%%
l2 = "FLAT 4 ON 22/F OF BLOCK B OF HILTON PLAZA, NO.3-9 SHA TIN CENTRE STREET, SHATIN"
l3 = "BLOCK B, 1ST FLOOR, LAI YUEN, SAI KUNG, N.T."
namestr = l3

v_region_id = ''
v_str_region = ''
v_str_mjdist = ''
v_mjdist_code = ''
v_str_dist = ''
v_dist_code = ''
v_str_lot_full = ''
v_str_dd_full = ''
v_str_estate = ''
v_est_id = ''
v_str_phase = ''
v_phase_id = ''
v_str_street_full = ''
v_str_bldg_full = ''
v_bldg_id = ''
v_str_floor_full = ''
v_str_floor_net = ''
v_str_unit_full = ''
v_str_unit_net = ''
v_has_carpark = ''
v_has_roof = ''
unparse_namestr = ''

inet_dist_id = ''
inet_mjdist_id = ''

namestr = namestr.strip()
namestr = namestr.replace('"','')
#single_parts = namestr.split(' ')
single_parts = re.split(r'[,\s]',namestr)
single_parts = [i for i in single_parts if i != '']
#re.split(r'[!@%^],c)

#region
if len(single_parts) > 1:
    region_part = single_parts[-2] + ' ' + single_parts[-1]
else:
    region_part = single_parts[-1]
    
trim_region_part = region_part.replace(' ','')

v_str_region = ""
v_region_id = ""

for item in cur_r:
    rname = item[1]
    if region_part.find(rname)>=0:
        v_str_region = rname
        v_region_id = item[0]
        break

newpart = trim_region_part.replace(v_str_region.replace(' ',''),'')
if len(single_parts) > 1:
    single_parts[-2] = newpart
    single_parts[-1] = ''
else:
    single_parts[-1] = newpart
    
unparse_namestr = ' '.join(single_parts)  

#major district + district
single_parts = re.split(r'[,\s]',unparse_namestr)
single_parts = [i for i in single_parts if i != '']
mjdist_part = ''

if len(single_parts) > 2:
    mjdist_part = single_parts[-3] + ' ' + single_parts[-2] + ' ' + single_parts[-1]
elif len(single_parts)  == 0:
    mjdist_part = ''        
else:
    for i in range(len(single_parts)):
        idx = -(len(single_parts) - i)
        mjdist_part += ' ' + single_parts[idx]
    
trim_mjdist_part = mjdist_part.replace(' ','')
v_mjdist_code = ''

v_str_mjdist = ''
for item in cur_m:
    mname = item[1]
    if trim_mjdist_part.find(mname.replace(' ',''))>=0:
        v_str_mjdist = mname
        v_mjdist_code = item[0]
        inet_mjdist_id = item[3]
        break

ISLAND_DIST_LIST = ["LAMMA ISLAND","CHEUNG CHAU","PENG CHAU","HEI LING CHAU"]

if v_str_mjdist == '':
    v_str_dist = ''
    v_dist_code = ''
    
    for island_dist in ISLAND_DIST_LIST:
        if trim_mjdist_part.find(island_dist.replace(' ','')) >=0:
            v_str_dist = island_dist
            v_dist_code = 'LT / ISL'
            break
    
    if v_dist_code == '':    
        for item in cur_d:
            dname_raw_full = item[1]
            dname_list = re.split(r'[/|\(]',dname_raw_full)
            for dname_raw in dname_list:
                dname_raw = dname_raw.replace('STATION','')
                dname_raw = dname_raw.replace(')','')
                dname = dname_raw.replace(' ','')
                if trim_mjdist_part.find(dname)>=0:
                    v_str_dist = dname_raw
                    v_dist_code = item[0]
                    inet_dist_id = item[3]
                    break
                
    if v_str_dist != '':
        newpart = trim_mjdist_part.replace(v_str_dist.replace(' ',''),'')
        #MUIWOLANTAU MUIWOLANTAU
        if len(single_parts) > 2:
            single_parts[-3] = newpart
            single_parts[-2] = ''
            single_parts[-1] = ''
        elif len(single_parts)  == 0:
            pass        
        else:
            for i in range(len(single_parts)):
                idx = -(len(single_parts) - i)
                if i == 0:
                    single_parts[idx] = newpart  
                else:
                    single_parts[idx] = ''
                    
        unparse_namestr = ' '.join(single_parts)   
        
else:
    m_cur_d = [d for d in cur_d if d[2] == v_mjdist_code]
    
    newpart = trim_mjdist_part.replace(v_str_mjdist.replace(' ',''),'')

    if len(single_parts) > 2:
        single_parts[-3] = newpart
        single_parts[-2] = ''
        single_parts[-1] = ''
    elif len(single_parts)  == 0:
        pass        
    else:
        for i in range(len(single_parts)):
            idx = -(len(single_parts) - i)
            if i == 0:
                single_parts[idx] = newpart  
            else:
                single_parts[idx] = ''
                
    unparse_namestr = ' '.join(single_parts) 

    single_parts = re.split(r'[,\s]',unparse_namestr)
    single_parts = [i for i in single_parts if i != '']
    
    dist_part = ''
    if len(single_parts) > 2:
        dist_part = single_parts[-3] + ' ' + single_parts[-2] + ' ' + single_parts[-1]
    elif len(single_parts)  == 0:
        dist_part = ''
    else:
        for i in range(len(single_parts)):
            idx = -(len(single_parts) - i)
            dist_part += ' ' + single_parts[idx]
    
    trim_dist_part = dist_part.replace(' ','')

    v_str_dist = ''
    v_dist_code = ''
    for island_dist in ISLAND_DIST_LIST:
        if trim_dist_part.find(island_dist.replace(' ','')) >=0:
            v_str_dist = island_dist
            v_dist_code = 'LT / ISL'
            break    
    
    if v_dist_code == '':  
        for item in m_cur_d:
            dname_raw_full = item[1]
            dname_list = re.split(r'[/|\(]',dname_raw_full)
            for dname_raw in dname_list:
                dname_raw = dname_raw.replace('STATION','')
                dname_raw = dname_raw.replace(')','')
                dname = dname_raw.replace(' ','')
                if trim_dist_part.find(dname) >=0:
                    v_str_dist = dname_raw
                    v_dist_code = item[0]
                    inet_dist_id = item[3]
                    break
    '''            
    if v_str_dist != '':
        newpart = trim_dist_part.replace(v_str_dist.replace(' ',''),'')
        if len(single_parts) > 2:
            single_parts[-3] = newpart
            single_parts[-2] = ''
            single_parts[-1] = ''
        elif len(single_parts)  == 0:
            pass        
        else:
            for i in range(len(single_parts)):
                idx = -(len(single_parts) - i)
                if i == 0:
                    single_parts[idx] = newpart  
                else:
                    single_parts[idx] = ''
                    
        unparse_namestr = ' '.join(single_parts)       
    '''
    
if v_str_dist == '' and v_str_mjdist == '' and v_str_region == '':
    unparse_namestr = namestr            
     
#unit
res = re.search('(?P<raw_unit>(ROOM|UNIT|FLAT)\s\w*)',unparse_namestr)
if res is not None:
    v_str_unit_full = res.groupdict()['raw_unit']
    unparse_namestr = unparse_namestr.replace(v_str_unit_full,'') 
    v_str_unit_net = re.sub('(ROOM|UNIT|FLAT)\s', '', v_str_unit_full)
else:
    v_str_unit_full = ''    
    v_str_unit_net = ''

#floor
unparse_namestr = unparse_namestr.replace("FIRST FLOOR","1ST FLOOR")
unparse_namestr = unparse_namestr.replace("SECOND FLOOR","2ND FLOOR")
unparse_namestr = unparse_namestr.replace("THIRD FLOOR","3RD FLOOR")

res = re.search('(?P<raw_floor>(\w*/F(LOOR)?\s?)|((1ST|2ND|3RD|\d+TH|\d+|GROUND)\sFLOOR))',unparse_namestr)
if res is not None:
    v_str_floor_full = res.groupdict()['raw_floor']
    unparse_namestr = unparse_namestr.replace(v_str_floor_full,'') 
    v_str_floor_net = re.sub('(/F(LOOR)?\s)|((ST|ND|RD|TH|ROUND)?\sFLOOR)', '', v_str_floor_full)   
else:
    v_str_floor_full = ''
    v_str_floor_net = ''    

#LOT
v_str_dd_full = ''
v_str_lot_full = ''
v_str_lot_ext_full = ''

while re.search('(?P<raw_lot>LOTS?\sNOS?\.?\s?\d+(\s?(&|AND)\s?\d+)?)',unparse_namestr) is not None:
    res = re.search('(?P<raw_lot>LOTS?\sNOS?\.?\s?\d+(\s?(&|AND)\s?\d+)?)',unparse_namestr)
    v_str_lot_single = res.groupdict()['raw_lot']
    unparse_namestr = unparse_namestr.replace(v_str_lot_single,'')
    v_str_lot_full += v_str_lot_single + ','
v_str_lot_full = v_str_lot_full[:-1]
    
if v_str_lot_full != '':
    while re.search('(?P<raw_lotext>((S\.|SECTION\s)[A-N]|(S\.?S\.\s?|SUB-SECTION\s|SUB-SEC\.)\d+|(R\.?P\.|REMAINING\sPORTION\s)|(EX\.|EXTENSION))\s?)',unparse_namestr) is not None:
         res = re.search('(?P<raw_lotext>((S\.|SECTION\s)[A-N]|(S\.?S\.\s?|SUB-SECTION\s|SUB-SEC\.)\d+|(R\.?P\.|REMAINING\sPORTION\s)|(EX\.|EXTENSION))\s?)',unparse_namestr)
         v_str_lot_ext_single = res.groupdict()['raw_lotext']
         unparse_namestr = unparse_namestr.replace(v_str_lot_ext_single,'')
         v_str_lot_ext_single = v_str_lot_ext_single.replace('SUB-SECTION ','SS.')
         v_str_lot_ext_single = v_str_lot_ext_single.replace('SUB-SEC.','SS.')
         v_str_lot_ext_single = v_str_lot_ext_single.replace('S.S.','SS.')
         v_str_lot_ext_single = v_str_lot_ext_single.replace('SECTION ','S.')
         v_str_lot_ext_single = v_str_lot_ext_single.replace('REMAINING PORTION ','R.P.')
         v_str_lot_ext_single = v_str_lot_ext_single.replace('EXTENSION','EX.')
        
         v_str_lot_ext_full += v_str_lot_ext_single 
if v_str_lot_ext_full != '' and v_str_lot_ext_full[-1] in (' ',','):
    v_str_lot_ext_full = v_str_lot_ext_full[:-1]

res = re.search('(?P<raw_DD>D\.?D\.?\s?(NO\.?\s)?\d+[L]?\s?)',unparse_namestr)
if res is not None:
     v_str_dd_full = res.groupdict()['raw_DD']
     unparse_namestr = unparse_namestr.replace(v_str_dd_full,'')
else:
     v_str_dd_full = ''

#estate + phase
v_str_estate = ''
v_str_phase = ''
v_est_id = ''
v_phase_id = ''

v_str_estate_raw = ''

for item in cur_e:
    ename = ' ' + item[1] 
    if unparse_namestr.find(ename)>=0:
        v_str_estate_raw = ename.lstrip()
        break

if v_str_estate_raw != '':
    n_cur_e = [e for e in cur_e if e[1] == v_str_estate_raw]

    if len(n_cur_e) == 1:
        v_str_estate = n_cur_e[0][1]
        v_est_id = n_cur_e[0][0]
    else:
        if inet_dist_id != '':
            d_cur_e = [e for e in n_cur_e if e[2] == inet_dist_id]
            if len(d_cur_e) == 1:
                v_str_estate = d_cur_e[0][1]
                v_est_id = d_cur_e[0][0]
            else:
                v_str_estate = v_str_estate_raw
                v_est_id = 'DUP_ESTNAME'
        elif inet_mjdist_id != '':
            if inet_mjdist_id not in ('2010','3010'):
                m_cur_e = [e for e in n_cur_e if e[3] == inet_mjdist_id]
            else:
                m_cur_e = [e for e in n_cur_e if e[3] in ('2010','3010')]
                
            if len(m_cur_e) == 1:
                v_str_estate = m_cur_e[0][1]
                v_est_id = m_cur_e[0][0]
            else:
                v_str_estate = v_str_estate_raw
                v_est_id = 'DUP_ESTNAME'
        else:
            v_str_estate = v_str_estate_raw
            v_est_id = 'DUP_ESTNAME'

if v_est_id != 'DUP_ESTNAME':
    if v_str_estate != '':
        unparse_namestr = unparse_namestr.replace(v_str_estate,'')  
        e_cur_p = [p for p in cur_p if p[1] == v_est_id]
        if len(e_cur_p) > 0:
            v_str_phase = ''
            for item in e_cur_p:
                pname = ' ' + item[2]
                if unparse_namestr.find(pname)>=0:
                    v_str_phase = pname.lstrip()
                    v_est_id = item[1]
                    v_phase_id = item[0]
                    break
        
            if v_str_phase != '':
                unparse_namestr = unparse_namestr.replace(v_str_phase,'')  
                    
        else:
            v_str_phase = ''
            res = re.search('(?P<raw_phase>(PHASE\s.*\s))',unparse_namestr)
            if res is not None:
                v_str_phase = res.groupdict()['raw_phase']
                unparse_namestr = unparse_namestr.replace(v_str_phase,'')     
    
    else:
        for item in cur_eb_p:
            pname = ' ' + item[2]
            extname = re.search(r"(?P<extra_str>\(.*\))",item[2])
            if extname is not None:
                extname = extname.groupdict()['extra_str']
                net_pname = pname.replace(extname,'')
                net_pname = net_pname.rstrip()
                
            if unparse_namestr.find(pname)>=0:
                v_str_phase = pname.lstrip()
                v_est_id = item[1]
                v_phase_id = item[0]
                break
        
            if v_str_phase == '' and extname is not None:
                if unparse_namestr.find(net_pname)>=0:
                    v_str_phase = pname.lstrip()
                    v_est_id = item[1]
                    v_phase_id = item[0]            
            
        if v_str_phase != '':
            unparse_namestr = unparse_namestr.replace(v_str_phase,'')  
    
#bldg
v_str_bldg_full = ''
v_bldg_id = ''

res = re.search('(?P<raw_bldg>(TOWER|BLOCK|HOUSE)\s(NO\.)?\s?\w*\s)',unparse_namestr)  #HOUSE NO. 3 
if res is not None:
    v_str_bldg_raw = res.groupdict()['raw_bldg']
    v_str_bldg_raw_map = v_str_bldg_raw.replace("NO. ","").strip()
    v_str_bldg_raw_map = ' ' + v_str_bldg_raw_map.replace("NO.","")
else:
    v_str_bldg_raw = ''
    v_str_bldg_raw_map = ''
    
if v_phase_id != '':
    if v_str_unit_full != '':
        b_cur_pe = [b for b in cur_b if b[1] == v_phase_id and b[4] not in ('CARPARK','SHOP')]
    else:
        b_cur_pe = [b for b in cur_b if b[1] == v_phase_id]
        
    if len(b_cur_pe) == 1:
        v_str_bldg_full = b_cur_pe[0][3]
        v_bldg_id = b_cur_pe[0][0]
        
    elif len(b_cur_pe) > 0:
        for item in b_cur_pe:
            bname = ' ' + item[3]
            if unparse_namestr.find(bname)>=0  or (bname.find(v_str_bldg_raw_map) >=0 and v_str_bldg_raw != ''):
                v_str_bldg_full = bname.lstrip()
                v_bldg_id = item[0]
                break
    
elif v_est_id != '':    
    if v_str_unit_full != '':
        b_cur_pe = [b for b in cur_b if b[2] == v_est_id and b[4] not in ('CARPARK','SHOP')]
    else:
        b_cur_pe = [b for b in cur_b if b[2] == v_est_id]

    if len(b_cur_pe) == 1:
        v_str_bldg_full = b_cur_pe[0][3]
        v_bldg_id = b_cur_pe[0][0]
    elif len(b_cur_pe) > 0:
        for item in b_cur_pe:
            bname = ' ' + item[3]
            if unparse_namestr.find(bname)>=0 or (bname.find(v_str_bldg_raw_map) >=0 and v_str_bldg_raw != ''):
                v_str_bldg_full = bname.lstrip()
                v_bldg_id = item[0]
                break
    
if v_str_bldg_full == '':   
    if v_str_bldg_raw != '':
        v_str_bldg_full = v_str_bldg_raw
    else:
        res = re.search('(?P<raw_bldg>\s\w*\s(COURT|MANSION|BUILDING|HOUSE))',unparse_namestr)
        if res is not None:
            v_str_bldg_full = res.groupdict()['raw_bldg']
        else:    
            v_str_bldg_full = ''

if v_str_bldg_full != 'CARPARK':
    unparse_namestr = unparse_namestr.replace(v_str_bldg_full,'') 

unparse_namestr = unparse_namestr.replace(v_str_bldg_raw,'')     

#STREET    
res = re.search('(?P<raw_street>(NOS?\.)?\s?\d+(\s?-\s?\d+)?\s(\(.*\)\s)?[^\d]*(STREET|ROAD|LANE))',unparse_namestr)
if res is not None:
    v_str_street_full = res.groupdict()['raw_street']
    unparse_namestr = unparse_namestr.replace(v_str_street_full,'') 
else:
    res = re.search('(?P<raw_street_pre>(NO(S)?\.)?\s?\d+[a-gA-G]?(-\s?\d+)?\s?(\((\w|\s)*\)\s?)?[^(\d|/)]*)',unparse_namestr)
    if res is not None:
        v_str_street_pre = res.groupdict()['raw_street_pre']
        unparse_namestr = unparse_namestr.replace(v_str_street_pre,'') 
        v_str_street_full = v_str_street_pre.strip() + ' ' + v_str_estate
    else:
        v_str_street_full = ''
    

#carpark
res = re.search('(?P<raw_carpark>CARPARK)',unparse_namestr)
if res is not None:
    v_has_carpark = 1
else:
    v_has_carpark = 0    

#roof
res = re.search('(?P<raw_roof>ROOF)',unparse_namestr)
if res is not None:
    v_has_roof = 1 
else:
    v_has_roof = 0

v_tx_type = ''
if v_str_lot_full != '':
    v_tx_type = 'LOT'
else:
    if v_has_carpark == 1 and v_str_unit_full == '':
        v_tx_type = 'CP'

    
if v_est_id != '' and v_dist_code == '':
    inet_dist_id  = [e[2] for e in cur_e if e[0] == v_est_id]
    if len(inet_dist_id) > 0:
        if inet_dist_id[0] is not None:
            inet_dist_id = inet_dist_id[0]
            v_dist_code_item = [d for d in cur_d if d[3] == inet_dist_id]  
            if len(v_dist_code_item) > 0:
                v_dist_code_item = v_dist_code_item[0]
                v_dist_code = v_dist_code_item[0]
                v_mjdist_code = v_dist_code_item[2]
    
    
if v_mjdist_code == '' and v_dist_code != '':
    v_mjdist_code = [d[2] for d in cur_d if d[0] == v_dist_code]
    v_mjdist_code = v_mjdist_code[0]
    
if v_region_id == '' and v_mjdist_code != '':
    v_region_id = [m[2] for m in cur_m if m[0] == v_mjdist_code]
    v_region_id = v_region_id[0]

#%%
stra  = 'VILLA BEL-AIR (DELUXE HOUSES)'
res = re.search(r"(?P<extra_str>\(.*\))",stra)
if res is not None:
    v_p = res.groupdict()['extra_str']
    
print(v_p)    

#%%

l11 = "G/F LOT NOS.263 & 264 IN DD206 SHATIN NEW TERRITORIES"
l12 = "FIRST FLOOR S.A OF YUN KONG TSUN LOT NO.107 AND YUN KONG TSUN LOT NO.108 BOTH IN DD106, YUEN LONG NEW TERRITORIES"
l13 = "2/F & ROOF SS.2 OF S.A OF LOT NO.2263 IN DD120 YUEN LONG NEW TERRITORIES"
l14 = "G/F INCLUDING GARDEN 163 TAI HONG WAI R.P. OF LOT NO.560 IN DD109 YUEN LONG NEW TERRITORIES"

unparse_namestr = l14

v_str_lot_full = ''
v_str_lot_ext_full = ''

while re.search('(?P<raw_lot>LOTS?\sNOS?\.?\s?\d+(\s?(&|AND)\s?\d+)?)',unparse_namestr) is not None:
    res = re.search('(?P<raw_lot>LOTS?\sNOS?\.?\s?\d+(\s?(&|AND)\s?\d+)?)',unparse_namestr)
    v_str_lot_single = res.groupdict()['raw_lot']
    unparse_namestr = unparse_namestr.replace(v_str_lot_single,'')
    v_str_lot_full += v_str_lot_single + ','
v_str_lot_full = v_str_lot_full[:-1]
print(v_str_lot_full)
    
if v_str_lot_full != '':
    while re.search('(?P<raw_lotext>(S\.[A-N]|SS\.\d+|R\.?P\.|E\.?X\.)\s)',unparse_namestr) is not None:
         res = re.search('(?P<raw_lotext>(S\.[A-N]|SS\.\d+|R\.?P\.|E\.?X\.)\s)',unparse_namestr)
         v_str_lot_ext_single = res.groupdict()['raw_lotext']
         unparse_namestr = unparse_namestr.replace(v_str_lot_ext_single,'')
         v_str_lot_ext_full += v_str_lot_ext_single 
v_str_lot_ext_full = v_str_lot_ext_full[:-1]
print(v_str_lot_ext_full)    
    
    
#%%
ll = "BLOCK B, 1ST FLOOR, LAI YUEN, SAI KUNG, N.T."
unparse_namestr = ll

                
#%%            
            
v_str_estate = ''
v_str_phase = ''
v_est_id = ''
v_phase_id = ''
    

'''    
    for item in cur_e:
        ename = ' ' + item[1] 
        #print(rname)
        if unparse_namestr.find(ename)>=0:
            #print(item[0])
            v_str_estate = ename.lstrip()
            v_est_id = item[0]
            break
        
    if v_str_estate != '':
        unparse_namestr = unparse_namestr.replace(v_str_estate,'')  
        e_cur_p = [p for p in cur_p if p[1] == v_est_id]
        if len(e_cur_p) > 0:
            v_str_phase = ''
            for item in e_cur_p:
                pname = ' ' + item[2]
                if unparse_namestr.find(pname)>=0:
                    v_str_phase = pname.lstrip()
                    v_est_id = item[1]
                    v_phase_id = item[0]
                    break
        
            if v_str_phase != '':
                unparse_namestr = unparse_namestr.replace(v_str_phase,'')  
                    
        else:
            v_str_phase = ''
            res = re.search('(?P<raw_phase>(PHASE\s.*\s))',unparse_namestr)
            if res is not None:
                v_str_phase = res.groupdict()['raw_phase']
                unparse_namestr = unparse_namestr.replace(v_str_phase,'')     
    
    else:
        for item in cur_eb_p:
            pname = ' ' + item[2]
            extname = re.search(r"(?P<extra_str>\(.*\))",item[2])
            if extname is not None:
                extname = extname.groupdict()['extra_str']
                net_pname = pname.replace(extname,'')
                net_pname = net_pname.rstrip()
                
            if unparse_namestr.find(pname)>=0:
                v_str_phase = pname.lstrip()
                v_est_id = item[1]
                v_phase_id = item[0]
                break
        
            if v_str_phase == '' and extname is not None:
                if unparse_namestr.find(net_pname)>=0:
                    v_str_phase = pname.lstrip()
                    v_est_id = item[1]
                    v_phase_id = item[0]            
            
        if v_str_phase != '':
            unparse_namestr = unparse_namestr.replace(v_str_phase,'')      
'''    