import xlrd
from datetime import datetime
from xlrd import xldate_as_tuple
import os
import re
from Oracle_Process import Oracle
# -*- coding: utf-8 -*-

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


def sql_single_param_gen(bs_cat, bs_info, bs_type, memo_no, delivery_date, param_list_single):
    sql_single_param = {}
    bs_num = bs_info.count(',') + 1
    if bs_num > 1:
        bs_category = 'M' + bs_cat
    else:
        bs_category = 'S' + bs_cat

    if bs_type == 0.0:
        bs_type = ''
    else:
        bs_type = bs_type.upper()

    bs_list = bs_info.split(',')
    for namestr in bs_list:
        sql_single_param = {}
        name, percentage, if_abnormal_char, space_divided_num = name_analyzer(namestr,bs_type)
        sql_single_param['MEMO_NO'] = memo_no
        sql_single_param['DELIVERY_DATE'] = delivery_date
        sql_single_param['BS_CATEGORY'] = bs_category
        sql_single_param['BS_TYPE'] = bs_type
        sql_single_param['NAME'] = name
        sql_single_param['PERCENTAGE'] = percentage
        sql_single_param['IF_ABNORMAL_CHAR'] = if_abnormal_char
        sql_single_param['SPACE_DIVIDED_NUM'] = space_divided_num + 1
        param_list_single.append(sql_single_param)


def sql_single_param_insert(bs_cat, bs_info, bs_type, memo_no, delivery_date, param_list_single):
    sql_single_param = {}
    bs_num = bs_info.count(',') + 1
    if bs_num > 1:
        bs_category = 'M' + bs_cat
    else:
        bs_category = 'S' + bs_cat

    if bs_type == 0.0:
        bs_type = ''

    bs_list = bs_info.split(',')
    for namestr in bs_list:
        sql_single_param = {}
        name, percentage, if_abnormal_char, space_divided_num = name_analyzer(namestr,bs_type)
        sql_single_param['MEMO_NO'] = memo_no
        sql_single_param['DELIVERY_DATE'] = delivery_date
        sql_single_param['BS_CATEGORY'] = bs_category
        sql_single_param['BS_TYPE'] = bs_type
        sql_single_param['NAME'] = name
        sql_single_param['PERCENTAGE'] = percentage
        sql_single_param['IF_ABNORMAL_CHAR'] = if_abnormal_char
        sql_single_param['SPACE_DIVIDED_NUM'] = space_divided_num + 1
        param_list_single.append(sql_single_param)

        date_type = 'yyyymmdd'
        sql_execute_single = "INSERT INTO APIS.BUYERSELLER_INFO_SINGLE(MEMO_NO, DELIVERY_DATE, BS_CATEGORY, BS_TYPE, NAME, PERCENTAGE, IF_ABNORMAL_CHAR, NAME_PART_NUM) " \
                             "VALUES (:MEMO_NO, to_date(:DELIVERY_DATE, '" + date_type + "'), :BS_CATEGORY, :BS_TYPE, :NAME, :PERCENTAGE, :IF_ABNORMAL_CHAR, :SPACE_DIVIDED_NUM)"
        #print(sql_single_param)
        conn.insertSingle(sql_execute_single, sql_single_param)


def buyerseller_excel_process(excel_path, conn):

        single_insert_source = ['201112 CN.xls','201202 CN.xls','201304 CN.xlsx','201308CN.xlsx','201309CN.xlsx','201312 CN.xlsx','201403 CN.xlsx',
                                '201408CN.xlsx','201410 CN.xlsx','201411 CN.xlsx','201503 CN.xlsx','201612CN.xlsx']
        workbook = xlrd.open_workbook(excel_path)
        excel_source_name = excel_path.split('\\')[-1]
        if excel_source_name in single_insert_source:
            insert_type = 'Single'
        else:
            insert_type = 'Batch'

        sheet_names = workbook.sheet_names()
        param_list_tx = []
        param_list_single = []

        memo_no = ''
        nature_of_inst = ''
        instrument_type = ''
        instrument_date = ''
        delivery_date = ''
        consideration = 0
        markttype = 0
        buyer_info = ''
        buyer_type = ''
        seller_info = ''
        seller_type = ''
        bldg_id = ''
        bldgname_eng = ''
        bldgname_chn = ''
        bldg_type = ''
        estate_id = ''
        estate_type = ''
        estate_eng = ''
        estate_chn = ''
        district_id = ''
        district_eng = ''
        district_chn = ''
        dist_area = ''
        phase = ''
        floor = ''
        block = ''
        flat = ''
        unit_price = 0.0
        unit_area = 0
        noof_unit = 0
        car_park = 0

        c_memo_no = -1
        c_nature_of_inst = -1
        c_instrument_type = -1
        c_instrument_date = -1
        c_delivery_date = -1
        c_consideration = -1
        c_markttype = -1
        c_buyer_info = -1
        c_buyer_type = -1
        c_seller_info = -1
        c_seller_type = -1
        c_bldg_id = -1
        c_bldgname_eng = -1
        c_bldgname_chn = -1
        c_bldg_type = -1
        c_estate_id = -1
        c_estate_type = -1
        c_estate_eng = -1
        c_estate_chn = -1
        c_district_id = -1
        c_district_eng = -1
        c_district_chn = -1
        c_dist_area = -1
        c_phase = -1
        c_floor = -1
        c_block = -1
        c_flat = -1
        c_unit_price = -1
        c_unit_area = -1
        c_noof_unit = -1
        c_car_park = -1

        sql_execute = 'insert into apis.TX_BUYERSELLER_INFO('
        sql_value = ' VALUES ('

        for sheet_name in sheet_names:
                worksheet = workbook.sheet_by_name(sheet_name)
                print('excel:' + excel_source_name + ' sheet:' + worksheet.name)
                rows = worksheet.nrows
                cols = worksheet.ncols

                for c in range(cols):
                    field_value = worksheet.cell(0, c).value
                    if field_value == 'memo_no':
                        c_memo_no = c
                        sql_execute += field_value + ','
                        sql_value += ':' + field_value + ','
                    if field_value == 'nature_of_inst':
                        c_nature_of_inst = c
                        sql_execute += field_value + ','
                        sql_value += ':' + field_value + ','
                    if field_value == 'instrument_type':
                        c_instrument_type = c
                        sql_execute += field_value + ','
                        sql_value += ':' + field_value + ','
                    if field_value == 'instrument_date':
                        c_instrument_date = c
                        sql_execute += field_value + ','
                        sql_value += 'to_date(:' + field_value + ', \'yyyymmdd\'),'
                    if field_value == 'delivery_date':
                        c_delivery_date = c
                        sql_execute += field_value + ','
                        sql_value += 'to_date(:' + field_value + ', \'yyyymmdd\'),'
                    if field_value == 'consideration':
                        c_consideration = c
                        sql_execute += field_value + ','
                        sql_value += ':' + field_value + ','
                    if field_value == 'markttype':
                        c_markttype = c
                        sql_execute += field_value + ','
                        sql_value += ':' + field_value + ','
                    if field_value == 'buyer_info':
                        c_buyer_info = c
                        sql_execute += field_value + ','
                        sql_value += ':' + field_value + ','
                    if field_value == 'buyer_type':
                        c_buyer_type = c
                        sql_execute += field_value + ','
                        sql_value += ':' + field_value + ','
                    if field_value == 'seller_info':
                        c_seller_info = c
                        sql_execute += field_value + ','
                        sql_value += ':' + field_value + ','
                    if field_value == 'seller_type':
                        c_seller_type = c
                        sql_execute += field_value + ','
                        sql_value += ':' + field_value + ','
                    if field_value == 'bldg_id':
                        c_bldg_id = c
                        sql_execute += field_value + ','
                        sql_value += ':' + field_value + ','
                    if field_value == 'bldgname_eng':
                        c_bldgname_eng = c
                        sql_execute += field_value + ','
                        sql_value += ':' + field_value + ','
                    if field_value == 'bldgname_chn':
                        c_bldgname_chn = c
                        sql_execute += field_value + ','
                        sql_value += ':' + field_value + ','
                    if field_value == 'bldg_type':
                        c_bldg_type = c
                        sql_execute += field_value + ','
                        sql_value += ':' + field_value + ','
                    if field_value == 'estate_id':
                        c_estate_id = c
                        sql_execute += field_value + ','
                        sql_value += ':' + field_value + ','
                    if field_value == 'estate_type':
                        c_estate_type = c
                        sql_execute += field_value + ','
                        sql_value += ':' + field_value + ','
                    if field_value == 'estate_eng':
                        c_estate_eng = c
                        sql_execute += field_value + ','
                        sql_value += ':' + field_value + ','
                    if field_value == 'estate_chn':
                        c_estate_chn = c
                        sql_execute += field_value + ','
                        sql_value += ':' + field_value + ','
                    if field_value == 'district_id':
                        c_district_id = c
                        sql_execute += field_value + ','
                        sql_value += ':' + field_value + ','
                    if field_value == 'district_eng':
                        c_district_eng = c
                        sql_execute += field_value + ','
                        sql_value += ':' + field_value + ','
                    if field_value == 'district_chn':
                        c_district_chn = c
                        sql_execute += field_value + ','
                        sql_value += ':' + field_value + ','
                    if field_value == 'dist_area':
                        c_dist_area = c
                        sql_execute += field_value + ','
                        sql_value += ':' + field_value + ','
                    if field_value == 'phase':
                        c_phase = c
                        sql_execute += field_value + ','
                        sql_value += ':' + field_value + ','
                    if field_value == 'floor':
                        c_floor = c
                        sql_execute += field_value + ','
                        sql_value += ':' + field_value + ','
                    if field_value == 'block':
                        c_block = c
                        sql_execute += field_value + ','
                        sql_value += ':' + field_value + ','
                    if field_value == 'flat':
                        c_flat = c
                        sql_execute += field_value + ','
                        sql_value += ':' + field_value + ','
                    if field_value == 'unit_price':
                        c_unit_price = c
                        sql_execute += field_value + ','
                        sql_value += ':' + field_value + ','
                    if field_value == 'unit_area':
                        c_unit_area = c
                        sql_execute += field_value + ','
                        sql_value += ':' + field_value + ','
                    if field_value == 'noof_unit':
                        c_noof_unit = c
                        sql_execute += field_value + ','
                        sql_value += ':' + field_value + ','
                    if field_value == 'car_park':
                        c_car_park = c
                        sql_execute += field_value + ','
                        sql_value += ':' + field_value + ','

                sql_execute = sql_execute[0:len(sql_execute) - 1] + ')'
                sql_value = sql_value[0:len(sql_value) - 1] + ')'
                sql_execute += sql_value
                # print(sql_execute)

                for r in range(1,rows):
                    sql_param = {}

                    if c_memo_no != -1:
                        memo_no = worksheet.cell(r, c_memo_no).value
                    if memo_no != '':

                        sql_param['memo_no'] = memo_no

                        if c_nature_of_inst != -1:
                            nature_of_inst = worksheet.cell(r, c_nature_of_inst).value
                            sql_param['nature_of_inst'] = nature_of_inst
                        if c_instrument_type != -1:
                            instrument_type = worksheet.cell(r, c_instrument_type).value
                            sql_param['instrument_type'] = instrument_type
                        if c_instrument_date != -1:
                            instrument_date = worksheet.cell(r, c_instrument_date).value
                            sql_param['instrument_date'] = instrument_date
                        if c_delivery_date != -1:
                            delivery_date = worksheet.cell(r, c_delivery_date).value
                            sql_param['delivery_date'] = delivery_date
                        if c_consideration != -1:
                            consideration = worksheet.cell(r, c_consideration).value
                            sql_param['consideration'] = consideration
                        if c_markttype != -1:
                            markttype = worksheet.cell(r, c_markttype).value
                            sql_param['markttype'] = markttype

                        if c_buyer_info != -1:
                            buyer_info = worksheet.cell(r, c_buyer_info).value
                            sql_param['buyer_info'] = str(buyer_info)
                        if c_buyer_type != -1:
                            buyer_type = worksheet.cell(r, c_buyer_type).value
                            sql_param['buyer_type'] = buyer_type

                        if str(buyer_info) != '' or buyer_type != '':
                            if insert_type == 'Single':
                                sql_single_param_insert('B', str(buyer_info), buyer_type, memo_no, delivery_date, param_list_single)
                            else:
                                sql_single_param_gen('B', str(buyer_info), buyer_type, memo_no, delivery_date, param_list_single)

                        if c_seller_info != -1:
                            seller_info = worksheet.cell(r, c_seller_info).value
                            sql_param['seller_info'] = str(seller_info)
                        if c_seller_type != -1:
                            seller_type = worksheet.cell(r, c_seller_type).value
                            sql_param['seller_type'] = seller_type

                        if str(seller_info) != '' or seller_type != '':
                            if insert_type == 'Single':
                                sql_single_param_insert('S', str(seller_info), seller_type, memo_no, delivery_date,param_list_single)
                            else:
                                sql_single_param_gen('S', str(seller_info), seller_type, memo_no, delivery_date, param_list_single)

                        if c_bldg_id != -1:
                            bldg_id = worksheet.cell(r, c_bldg_id).value
                            sql_param['bldg_id'] = bldg_id
                        if c_bldgname_eng != -1:
                            bldgname_eng = worksheet.cell(r, c_bldgname_eng).value
                            sql_param['bldgname_eng'] = str(bldgname_eng)
                        if c_bldgname_chn != -1:
                            bldgname_chn = worksheet.cell(r, c_bldgname_chn).value
                            bldgname_chn = re.sub(r"\ue00c", '', bldgname_chn)
                            sql_param['bldgname_chn'] = str(bldgname_chn)
                        if c_bldg_type != -1:
                            bldg_type = worksheet.cell(r, c_bldg_type).value
                            sql_param['bldg_type'] = bldg_type
                        if c_estate_id != -1:
                            estate_id = worksheet.cell(r, c_estate_id).value
                            sql_param['estate_id'] = estate_id
                        if c_estate_type != -1:
                            estate_type = worksheet.cell(r, c_estate_type).value
                            sql_param['estate_type'] = estate_type
                        if c_estate_eng != -1:
                            estate_eng = worksheet.cell(r, c_estate_eng).value
                            sql_param['estate_eng'] = str(estate_eng)
                        if c_estate_chn != -1:
                            estate_chn = worksheet.cell(r, c_estate_chn).value
                            estate_chn = re.sub(r"\ue00c", '', estate_chn)
                            sql_param['estate_chn'] = str(estate_chn)
                        if c_district_id != -1:
                            district_id = worksheet.cell(r, c_district_id).value
                            sql_param['district_id'] = district_id
                        if c_district_eng != -1:
                            district_eng = worksheet.cell(r, c_district_eng).value
                            sql_param['district_eng'] = district_eng
                        if c_district_chn != -1:
                            district_chn = worksheet.cell(r, c_district_chn).value
                            sql_param['district_chn'] = district_chn
                        if c_dist_area != -1:
                            dist_area = worksheet.cell(r, c_dist_area).value
                            sql_param['dist_area'] = dist_area
                        if c_phase != -1:
                            phase = worksheet.cell(r, c_phase).value
                            sql_param['phase'] = phase
                        if c_floor != -1:
                            floor = worksheet.cell(r, c_floor).value
                            sql_param['floor'] = floor
                        if c_block != -1:
                            block = worksheet.cell(r, c_block).value
                            sql_param['block'] = block
                        if c_flat != -1:
                            flat = worksheet.cell(r, c_flat).value
                            sql_param['flat'] = flat
                        if c_unit_price != -1:
                            unit_price = worksheet.cell(r, c_unit_price).value
                            if unit_price == 0.0:
                                sql_param['unit_price'] = 0
                            else:
                                sql_param['unit_price'] = unit_price
                        if c_unit_area != -1:
                            unit_area = worksheet.cell(r, c_unit_area).value
                            if unit_area == 0.0:
                                sql_param['unit_area'] = 0
                            else:
                                sql_param['unit_area'] = unit_area
                        if c_noof_unit != -1:
                            noof_unit = worksheet.cell(r, c_noof_unit).value
                            if noof_unit != '':
                                sql_param['noof_unit'] = int(noof_unit)
                            else:
                                sql_param['noof_unit'] = 0
                        if c_car_park != -1:
                            car_park = worksheet.cell(r, c_car_park).value
                            if car_park != '':
                                sql_param['car_park'] = int(car_park)
                            else:
                                sql_param['car_park'] = 0

                        if insert_type == 'Single':
                            #print(sql_param)
                            conn.insertSingle(sql_execute, sql_param)

                        param_list_tx.append(sql_param)

                    else:
                        if worksheet.cell(r+1, c_memo_no).value == '':
                            break

                date_type = 'yyyymmdd'
                sql_execute_single = "INSERT INTO APIS.BUYERSELLER_INFO_SINGLE(MEMO_NO, DELIVERY_DATE, BS_CATEGORY, BS_TYPE, NAME, PERCENTAGE, IF_ABNORMAL_CHAR, NAME_PART_NUM) " \
                                    "VALUES (:MEMO_NO, to_date(:DELIVERY_DATE, '" + date_type + "'), :BS_CATEGORY, :BS_TYPE, :NAME, :PERCENTAGE, :IF_ABNORMAL_CHAR, :SPACE_DIVIDED_NUM)"

                if insert_type == 'Batch':
                    conn.insertBatch(sql_execute,param_list_tx)
                    conn.insertBatch(sql_execute_single,param_list_single)

                print('imported tx number:' + str(len(param_list_tx)))
                print('imported single number:' + str(len(param_list_single)))

                break

    # except Exception as err:
    #         print('Exception: ', err)
    #         print('params: ', sql_param)


conn = Oracle('APIS','APIS','10.0.0.182:1521','apispdb')
excel_file_path = r'C:\Justin\APIS\buyerseller_excel_files\completed'
list = os.listdir(excel_file_path)
for i in range(0, len(list)):
    excel_path = os.path.join(excel_file_path, list[i])
    if os.path.isfile(excel_path) and os.path.splitext(excel_path)[1] in ('.xls', '.xlsx'):
        buyerseller_excel_process(excel_path, conn)
del conn
