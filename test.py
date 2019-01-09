# import cx_Oracle as cx
# import xlrd
# from datetime import datetime
# from xlrd import xldate_as_tuple
# import os
#
# conn = cx.connect('APIS/APIS@10.0.0.182:1521/apispdb')
# cursor = conn.cursor()
# cursor.execute("select * from region_mstr")
# row = cursor.fetchone ()
# param = [{'TEMP_UNIT_CONFIG_DTL_ID': 'B000087570_9', 'VERSION_DATE': '2018/05/18 00:00:00', 'EST_ID': 'E000016447', 'PHASE_ID': ' ', 'BLDG_ID': 'B000087570', 'FLOOR_FRM': 'G', 'FLOOR_TO': 'G', 'FLOOR_SKIP': '-', 'FLAT': 'A', 'NUM_ELEVATORS': '2', 'DIRECTION_CO': '1', 'COMMENTS': '', 'EXCEL_NAME': 'sample_1.xls'}, {'TEMP_UNIT_CONFIG_DTL_ID': 'B000087570_10', 'VERSION_DATE': '2018/05/18 00:00:00', 'EST_ID': 'E000016447', 'PHASE_ID': ' ', 'BLDG_ID': 'B000087570', 'FLOOR_FRM': 'G', 'FLOOR_TO': 'G', 'FLOOR_SKIP': '-', 'FLAT': 'B', 'NUM_ELEVATORS': '2', 'DIRECTION_CO': '1', 'COMMENTS': '', 'EXCEL_NAME': 'sample_1.xls'}, {'TEMP_UNIT_CONFIG_DTL_ID': 'B000087570_11', 'VERSION_DATE': '2018/05/18 00:00:00', 'EST_ID': 'E000016447', 'PHASE_ID': ' ', 'BLDG_ID': 'B000087570', 'FLOOR_FRM': '1', 'FLOOR_TO': '1', 'FLOOR_SKIP': '-', 'FLAT': 'A', 'NUM_ELEVATORS': '2', 'DIRECTION_CO': '1', 'COMMENTS': '', 'EXCEL_NAME': 'sample_1.xls'}, {'TEMP_UNIT_CONFIG_DTL_ID': 'B000087570_12', 'VERSION_DATE': '2018/05/18 00:00:00', 'EST_ID': 'E000016447', 'PHASE_ID': ' ', 'BLDG_ID': 'B000087570', 'FLOOR_FRM': '1', 'FLOOR_TO': '1', 'FLOOR_SKIP': '-', 'FLAT': 'B', 'NUM_ELEVATORS': '2', 'DIRECTION_CO': '1', 'COMMENTS': '', 'EXCEL_NAME': 'sample_1.xls'}, {'TEMP_UNIT_CONFIG_DTL_ID': 'B000087570_13', 'VERSION_DATE': '2018/05/18 00:00:00', 'EST_ID': 'E000016447', 'PHASE_ID': ' ', 'BLDG_ID': 'B000087570', 'FLOOR_FRM': '1', 'FLOOR_TO': '1', 'FLOOR_SKIP': '-', 'FLAT': 'C', 'NUM_ELEVATORS': '2', 'DIRECTION_CO': '1', 'COMMENTS': '', 'EXCEL_NAME': 'sample_1.xls'}, {'TEMP_UNIT_CONFIG_DTL_ID': 'B000087570_14', 'VERSION_DATE': '2018/05/18 00:00:00', 'EST_ID': 'E000016447', 'PHASE_ID': ' ', 'BLDG_ID': 'B000087570', 'FLOOR_FRM': '1', 'FLOOR_TO': '1', 'FLOOR_SKIP': '-', 'FLAT': 'E', 'NUM_ELEVATORS': '2', 'DIRECTION_CO': '1', 'COMMENTS': '', 'EXCEL_NAME': 'sample_1.xls'}, {'TEMP_UNIT_CONFIG_DTL_ID': 'B000087570_15', 'VERSION_DATE': '2018/05/18 00:00:00', 'EST_ID': 'E000016447', 'PHASE_ID': ' ', 'BLDG_ID': 'B000087570', 'FLOOR_FRM': '2', 'FLOOR_TO': 20.0, 'FLOOR_SKIP': '4,13,14', 'FLAT': 'A', 'NUM_ELEVATORS': '2', 'DIRECTION_CO': '1', 'COMMENTS': '', 'EXCEL_NAME': 'sample_1.xls'}, {'TEMP_UNIT_CONFIG_DTL_ID': 'B000087570_16', 'VERSION_DATE': '2018/05/18 00:00:00', 'EST_ID': 'E000016447', 'PHASE_ID': ' ', 'BLDG_ID': 'B000087570', 'FLOOR_FRM': '2', 'FLOOR_TO': 20.0, 'FLOOR_SKIP': '4,13,14', 'FLAT': 'B', 'NUM_ELEVATORS': '2', 'DIRECTION_CO': '1', 'COMMENTS': '', 'EXCEL_NAME': 'sample_1.xls'}, {'TEMP_UNIT_CONFIG_DTL_ID': 'B000087570_17', 'VERSION_DATE': '2018/05/18 00:00:00', 'EST_ID': 'E000016447', 'PHASE_ID': ' ', 'BLDG_ID': 'B000087570', 'FLOOR_FRM': '2', 'FLOOR_TO': '2', 'FLOOR_SKIP': '-', 'FLAT': 'C', 'NUM_ELEVATORS': '2', 'DIRECTION_CO': '1', 'COMMENTS': '', 'EXCEL_NAME': 'sample_1.xls'}]
#
# #param = [{'TEMP_UNIT_CONFIG_DTL_ID': 'B000087570_9', 'VERSION_DATE': '2018/05/18 00:00:00', 'EST_ID': 'E000016447', 'PHASE_ID': ' ', 'BLDG_ID': 'B000087570', 'FLOOR_FRM': 'G', 'FLOOR_TO': 'G', 'FLOOR_SKIP': '-', 'FLAT': 'A', 'NUM_ELEVATORS': '2', 'DIRECTION_CO': '1', 'COMMENTS': '', 'EXCEL_NAME': 'sample_1.xls'}]
# sql_execute = "insert into APIS.TEMP_UNIT_CONFIG_DTL(TEMP_UNIT_CONFIG_DTL_ID, VERSION_DATE, EST_ID, PHASE_ID, BLDG_ID, FLOOR_FRM, FLOOR_TO, FLOOR_SKIP, FLAT, NUM_ELEVATORS, DIRECTION_CO, COMMENTS, EXCEL_NAME) values (:TEMP_UNIT_CONFIG_DTL_ID, to_date(:VERSION_DATE, 'yyyy/mm/dd hh24:mi:ss'), :EST_ID, :PHASE_ID, :BLDG_ID, :FLOOR_FRM, :FLOOR_TO, :FLOOR_SKIP, :FLAT, :NUM_ELEVATORS, :DIRECTION_CO, :COMMENTS, :EXCEL_NAME)"
# cursor.executemany(sql_execute,param)
# print (row)
# conn.commit()
# cursor.close ()
# conn.close ()

# print(1)

# ctype : 0 empty,1 string, 2 number, 3 date, 4 boolean, 5 error

# str = '葵興村\ue00c'
import re
# new_str = re.sub(r"\ue00c",'',str)
# #new_str = re.search(r'\\u',str)
# for i in str:
#     print(i)
# print(new_str)
# #print(str.decode("utf8"))

temp = "想做/ 兼_职/学生_/ 的 、加,我Q：  1 5.  8 0. ！！？？  8 6 。0.  2。 3     有,惊,喜,哦"
#temp = temp.decode("utf8")
string = re.sub("[\s+\.\!\/_,$%^*(+\"\']+|[+——！，。？、~@#￥%……&*（）]+", "",temp)
print(string)

from datetime import datetime
#version_date = date('2012/12/12').strftime('%Y/%m/%d')

#print(version_date)

a = '35&36'
if a.__contains__('&'):
    print('ok')
print(a.replace('&','-'))




str1 = 'CHAN TAI MAN 1/3, CHAN SIU MAN 2/3'
str2 = 'CHAN TAI MAN 50%, CHAN SIU MAN 50%'
str3 = 'CHAN TAI MAN 40, CHAN SIU MAN 60'
str4 = 'CHAN TAI MAN (50/100), CHAN SIU MAN (50/100)'
str5 = 'CHAN TAI MAN [40/100], CHAN SIU MAN [60/100]'
str6 = 'CHAN TAI MAN IO/IOO, CHAN SIU MAN 90/100, ' \
        'CHAN TAI MAN1/3, CHAN SIU MAN2/3, ' \
        'WONG PUI YUNG 9O /IOO, TAM KA KEI IO/IO, ' \
        'WONG WAI SUM ZOON IO/IOO, FUNG BO LING 9O/IO, '\
        'WONG KIT LIMO KITTY 4O /IOO, MAN YAP POMG JULIO 2O /IOO, WONG WING CHUNG 4O /IC, ' \
        'TANG HO MING 3 I5, KWAN PO SHAM 2/, ' \
        'CHONG YUEK HI ATHENA 3 /IO, LEE CHOR YEE 4 /IO, LEE MAN KIN 3/I, ' \
        'LAM WAI TING 2/4, CHING CHUI MEI4, CHING CHUI KUEN I, ' \
        'LIU KIT YING I ?2, WONG YUEN FONG, ' \
        'SEE CHUN WAI I 2, PAK YAN YAN, ' \
        'WONG SHUN WAI 3O /IOO, LEE WAI KONG WENDY 7O/IO, ' \
        'LAM YAU KUEN I , LAM KAM KUEN I s, LAM SIU KUEN I , LAM MAN KUEN I , LAM SHUN KUEN I , LAM TAK KUEN6, ' \
        'CHU WING MAN GRACE I2/IOO, CHAN WANG 88/IO, ' \
        'YAU KA KEUNGIO, FIING WAI LING KITTY 9/I, ' \
        'CHUNG WAI MAN 3O /IOO, LAM SUI NGOR 3O /IOO, LAM CHI MING 4O Iff, ' \
        'SIU KAI WAI BONI I I4, LAU KAM SHING4, CHAN MAN TAI4, IF CHI HUNG I, ' \
        'CHEUK YUET CHAUIOO, HON KING YUK 25 /IOO, WONG OI MING 74 Iff, ' \
        'YEUNG CHI MAN 4O tiC, LAO WAN 2 ?ADMINISTRATOR?, LAO WAN I 2 ?ADMINISTRATOR?,LAO WAN 2 ?ADMINISTRATOR, CHEUNG SOK O ALEXANDRA, 3298 LTD, LUI YIN WA2/3, LI HO SHUN ORSON I I2, LO FEL A, YANG CHEN KUO, WONG IOK I, HUI MAN 3HAM I, JEA DAVID P W, LEE CHON IAT 1/2, LEE CHON IAT 9O/IO, LEE CHON IAT IOIIO, CHAN YUEN WAH KITTY 2/I, TANG LAI KEI I f, HUI CHEUK HING DESMOND I 8, WONG SIU LAN I ., TAM KIT SHAN I t, LI HO SHUN ORSON I I2, UN LONG YIN I i2, CHAN WAN KI WINKY I5/I, KWOK TAI KUM I9 I2(, JAN TAN GHAZALI BIN 7/I, ' \
        'TANG SIU WA 99.99 TANG CHI LIMO O.OI%, LO SIU YUK 77.5, LO SIU YUK O.5%, LAM TSZ TSUN 1 / 2, LAI CHUN LUN ERIC 99.9OO, MOK DANIEL LAJ KEI 3OO(, TSUI DOMINIC M 3, TSANG ZHANG LI OI3(, FONG WING 7II f2, MO LAI SHUK HING KIMMY 5O Iit, CHU KOCH LOY GARY I 2 ?ADMINISTRATOR, TAM HO WA I 2 ?EXECUTOR, KWONG WAI ‘II2/, CHU KWOK CHU I t2, CHEUNG SOK I ALEXANDRA, CHAN 3IA MAN, CHAN CHOR YIP 25C, LEUNG KUN NING COLLEEN 2, LEUNG KUN NING COLLEEN 2O, LEUNG KUN NING COLLEEN 20, LEUNG KUN NING COLLEEN IO, KWONG WILLIAM 2 I3, CHU KWOK CHU I t2, YEUNG CHI MAN 4O tiC, WUWEN WEII/2, NG CHO SHAN 10ANNA, NG KIT HAR 1/2.YAULAIYUETI/, NG KWAN PO3O/IOt, CHAN SIU PO2I/IOO, AU WING TSZ9O/IO, WONG KWOK PO3O/IO, WONG SUET TEI2/, TAM SHUI OI4, CHOW WAIYEE2/3.CHOWNGLAUI/, N3HZNIHZ 3H, HEN OI9/I, MAK KIN KONGOF26/IOO, ZHU 7I7AM, CHUNG PO PO2 /3, SY SAU UN SALLY, NOAI LAN 7IMG.3, LEUNG FRANK 5IK-IU, WONG SUET TEI2/'
str25 = ''
str26 = 'LI .II, ZHANG .IIMG, MANAKTALA .4NEESH'


def name_analyzer(namestr,bs_type):
    name_part = ''
    percentage_part = ''

    namestr = namestr.strip()
    single_parts = namestr.split(' ')
    num_parts = len(single_parts)
    last_part = single_parts[-1]

    flag_type_1 = 0
    flag_type_2 = 0
    flag_abnormal_char = 0

    if num_parts > 1:
        if re.search(r'^/', last_part) is not None:
            last_2_part = single_parts[-2]
            if re.search(r'^(\d|I).*(\d|O)$', last_2_part) or re.search(r'^\d',last_2_part) or last_2_part == 'I':
                percentage_part = last_2_part + last_part
                name_part = ' '.join(single_parts[0:num_parts - 2])
            else:
                percentage_part = last_part
                name_part = ' '.join(single_parts[0:num_parts - 1])

            flag_type_1 = 1

        last_2_part = single_parts[-2]
        if re.match(r'^(\d|I|O)+/*(\d|I|O)+$',last_part) and last_part != 'OI':
            if re.match(r'^I?\d*$',last_2_part):
                percentage_part = last_2_part + ' ' + last_part
                name_part = ' '.join(single_parts[0:num_parts - 2])
            elif re.match(r'^OI',last_part) and re.search(r'\d', last_part):
                percentage_part = ''
                name_part = ' '.join(single_parts[0:num_parts])
            else:
                percentage_part = last_part
                name_part = ' '.join(single_parts[0:(num_parts - 1)])
            flag_type_2 = 1
        elif re.search(r'^(\d|I|i|\?)(\d|C|O|/|I|%|\(|ff)*$', last_part) is not None or re.match(r'^(O|0)?\.?O*(\d+|I)%?$',last_part) or re.match(r'^.$',last_part):
            if re.search(r'^(\d|I|i|\?)(\d|C|O|/|I|%|\(|ff)*$', last_2_part) is not None:
                percentage_part = last_2_part + ' ' + last_part
                name_part = ' '.join(single_parts[0:num_parts - 2])
            elif re.match(r'^.$',last_2_part):
                if last_2_part == last_part == 'I':
                    percentage_part = last_2_part + ' ' + last_part
                    name_part = ' '.join(single_parts[0:num_parts - 2])
                elif re.match(r'^\d+|\dO+|IO*$',last_part):
                    last_3_part = single_parts[-3]
                    if re.match(r'\d+',last_3_part):
                        percentage_part = last_3_part + ' '  + last_2_part + ' ' + last_part
                        name_part = ' '.join(single_parts[0:num_parts - 3])
                    else:
                        percentage_part = last_part
                        name_part = ' '.join(single_parts[0:(num_parts - 1)])
                else:
                    percentage_part = ''
                    name_part = ' '.join(single_parts[0:num_parts])
            elif re.match(r'^.$',last_part) and last_part != 'I' :
                if re.match(r'^\d+|\dO+|IO*$',last_part):
                    percentage_part = last_part
                    name_part = ' '.join(single_parts[0:(num_parts - 1)])
                else:
                    percentage_part = ''
                    name_part = ' '.join(single_parts[0:num_parts])
            elif re.match(r'^[0-9]+[a-zA-Z\.\-\(]+[0-9]?[a-zA-Z]*$',last_part):
                if re.match(r'^[0-9]{0,2}((O{0,2}(O|C|\()?)|(\.\d+O*))$',last_part):
                    percentage_part = last_part
                    name_part = ' '.join(single_parts[0:(num_parts - 1)])
                else:
                    percentage_part = ''
                    name_part = ' '.join(single_parts[0:num_parts])
            else:
                percentage_part = last_part
                name_part = ' '.join(single_parts[0:(num_parts - 1)])
            flag_type_2 = 1
        elif re.match(r'^\w+\d*/(\d*|IOO?t?)*',last_part):
            percentage_part = ''
            name_part = ' '.join(single_parts[0:num_parts])
            flag_type_2 = 1
        # elif re.match(r'^((\d+)|(\dO+)|(IO*))$',last_2_part) and num_parts > 2:   #
        elif re.match(r'^I?\d*O{0,2}$', last_2_part) and num_parts > 2 and (re.match(r'^\??[a-zA-Z]+\??$', last_part) is None or last_part in ('tiC','Iit','hOC','IO/ic','O.I%','IO/IOt','.II','.IIMG','.I’?')):
            percentage_part = last_2_part + ' ' + last_part
            name_part = ' '.join(single_parts[0:num_parts - 2])
            flag_type_2 = 1
        #elif last_part in ():


        if flag_type_1 + flag_type_2 == 0:
            if re.search(r'\d', last_part) is not None and re.search(r'/', last_part) is not None and re.match(r'^.$',last_part) is None and re.match(r'^‘I',last_part) is None:
                percentage_part = last_part
                name_part = ' '.join(single_parts[0:(num_parts - 1)])
            elif last_part in ('t2','f2'):
                percentage_part = last_part
                name_part = ' '.join(single_parts[0:(num_parts - 1)])
            else:
                percentage_part = ''
                name_part = ' '.join(single_parts[0:num_parts])

    else:
        percentage_part = ''
        name_part = namestr

    if re.search('[^a-zA-Z\s]', name_part) is not None and bs_type != 'COMPANY':
        flag_abnormal_char = 1

    name_space_num = name_part.count(' ')

    return name_part, percentage_part, flag_abnormal_char, name_space_num






res = re.split(',',str26)
for namestr in res:
    name,percentage,flag_abnormal_char,name_space_num = name_analyzer(namestr,'a')
    print('name: ' + name + ' percentage: ' + percentage )

test = '港圖灣 (2014終止銷售)'
ma = re.search(r'\(.*\)', test)
print(ma.group())

EST_NAME = re.sub('(?P<value>\(.*\))','',test)
EST_NAME = EST_NAME.strip()
print(EST_NAME)

last_part = '4O'
print(re.match(r'^I?\d*O{0,2}$',last_part))
last_part = 'tiC'
print(re.match(r'^\??[a-zA-Z]+\??$',last_part))





    # print(single_part[-2])

    # rr = re.search('[^a-zA-Z\s]',namestr)
    # print(namestr)
    # print(namestr.count(' '))
    # if rr is None:
    #     print('correct')
    # else:
    #     print('wrong')

# a = {'memo_no': '17011100700089', 'nature_of_inst': 'ASP', 'instrument_type': 'ASP', 'instrument_date': '20161213', 'delivery_date': '20170111', 'consideration': 5500000.0, 'markttype': '2', 'buyer_info': '', 'buyer_type': '', 'seller_info': '', 'seller_type': '', 'bldg_id': 'B13626', 'bldgname_eng': 'BAYVIEW TERRACE', 'bldgname_chn': '碧翠花園', 'bldg_type': 'R', 'estate_id': 'E00630',
#      'estate_type': 'E', 'estate_eng': 'BAYVIEW TERRACE', 'estate_chn': '碧翠花園', 'district_id': 'SHAM1', 'district_eng': 'SHAM TSENG (TUEN MUN)', 'district_chn': '深井(屯門)', 'dist_area': 'N', 'phase': '', 'floor': '8', 'block': '22', 'flat': 'C', 'unit_price': 6145.25, 'unit_area': 895.0, 'noof_unit': 1, 'car_park': 1}
# sql_string = "insert into APIS.TEMP_UNIT_CONFIG_DTL(memo_no, nature_of_inst, instrument_type, instrument_date, delivery_date, consideration, markttype, buyer_info, buyer_type, seller_info, seller_type, bldg_id, bldgname_eng, bldgname_chn, bldg_type, " \
#                                                                                 "estate_id,estate_type,estate_eng,estate_chn,district_id,district_eng,district_chn,dist_area,phase,floor,block,flat,unit_price,unit_area,noof_unit,car_park) " \
#                          "values ('{0}','{1}','{2}','{0}','{0}','{0}','{0}','{0}','{0}','{0}','{0}','{0}','{0}','{0}','{0}','{0}',to_date('{1}', 'yyyymmdd'),'{2}', '{3}', '{4}', '{5}', '{6}', '{7}', '{8}', '{9}', '{10}', '{11}', '{12}');".format(uid, version_date, est_id, phase_id, bldg_id, frm, to, skip, flat, num_lift, direction, comment, excel_source_name)
#
# {'memo_no': '17011100700089', 'nature_of_inst': 'ASP', 'instrument_type': 'ASP', 'instrument_date': '20161213', 'delivery_date': '20170111', 'consideration': 5500000.0, 'markttype': '2', 'buyer_info': '', 'buyer_type': '', 'seller_info': '', 'seller_type': '', 'bldg_id': 'B13626', 'bldgname_eng': 'BAYVIEW TERRACE', 'bldgname_chn': '碧翠花園', 'bldg_type': 'R', 'estate_id': 'E00630', 'estate_type': 'E', 'estate_eng': 'BAYVIEW TERRACE', 'estate_chn': '碧翠花園', 'district_id': 'SHAM1', 'district_eng': 'SHAM TSENG (TUEN MUN)', 'district_chn': '深井(屯門)', 'dist_area': 'N', 'phase': '', 'floor': '8', 'block': '22', 'flat': 'C', 'unit_price': 6145.25, 'unit_area': 895.0, 'noof_unit': 1, 'car_park': 1}
