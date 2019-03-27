import cx_Oracle as cx
import xlrd
from datetime import datetime
from xlrd import xldate_as_tuple
import os
from Oracle_Process import Oracle
import time



def excel_process(excel_path, conn):

    start_time = time.time()

    config_err_txt = 'C:\Justin\APIS\config_excel_files\config_err.txt'
    workbook = xlrd.open_workbook(excel_path)
    filesize = int(os.path.getsize(excel_path)/1024)
    excel_source_name = excel_path.split('\\')[-1]
    sheet_names = workbook.sheet_names()
    sheets = workbook.sheets()

    param_list_dtl = []
    param_list_item = []

    print(excel_source_name + ' has started.')

    for sheet_name in sheet_names:
        if sheet_name not in ('各表使用間隔', '抽出有在使用', 'Result', 'Est. List'):
            worksheet = workbook.sheet_by_name(sheet_name)
            print(worksheet.name)
            break

    rows = worksheet.nrows
    cols = worksheet.ncols

    end_r = rows
    end_c = cols
    start_r = 0

    version_date_c = -1
    est_phase_id_c = -1
    bldg_id_c = -1
    floor_from_c = -1
    floor_to_c = -1
    floor_skip_c = -1
    comment_c = -1
    flat_c = -1
    lift_num_c = -1
    direction_c = -1

    date_type = 'yyyy/mm/dd'

    for r in range(rows):
        for c in range(cols):
            check_cell_value = worksheet.cell(r, c).value
            if check_cell_value == '版本日期':
                version_date_c = c
            if check_cell_value in ('專頁編號','est_id'):
                est_phase_id_c = c
            if check_cell_value in ('bldg_id','bldg ID'):
                bldg_id_c = c
            if check_cell_value in ('由', 'From'):
                floor_from_c = c
            if check_cell_value in ('至', 'To'):
                floor_to_c = c
            if check_cell_value in ('Skip', 'skip'):
                floor_skip_c = c
            if check_cell_value in ('註', 'Comment'):
                comment_c = c
            if check_cell_value == '單位':
                flat_c = c
            if check_cell_value == '電梯數量':
                lift_num_c = c
            if check_cell_value == '方向坐標':
                direction_c = c
                start_c = c + 1
                if worksheet.cell(r+1, c).value == '':
                    start_r = r + 2
                else:
                    start_r = r + 1
                break

    for r in range(start_r, end_r):
        bldg_id = worksheet.cell(r, bldg_id_c).value

        if bldg_id != '':
            if worksheet.cell_value(r, version_date_c) != '':
                version_date = datetime(*xldate_as_tuple(worksheet.cell_value(r, version_date_c), 0)).strftime('%Y/%m/%d')
            else:
                version_date = '2000/01/01'
            # version_date = datetime(*xldate_as_tuple(worksheet.cell_value(r, version_date_c), 0)).strftime('%Y/%m/%d')

            if worksheet.cell(r, floor_from_c).ctype == 2:
                frm = str(int(worksheet.cell(r, floor_from_c).value))
            elif worksheet.cell(r, floor_from_c).value == '' or worksheet.cell(r, 8).value is None:
                frm = ' '
            else:
                frm = str(worksheet.cell(r, floor_from_c).value).replace('&','-')

            if worksheet.cell(r, floor_to_c).ctype == 2:
                to = str(int(worksheet.cell(r, floor_to_c).value))
            elif worksheet.cell(r, floor_to_c).value == '' or worksheet.cell(r, floor_to_c).value is None:
                to = ' '
            else:
                to = str(worksheet.cell(r, floor_to_c).value).replace('&','-')

            if worksheet.cell(r, floor_skip_c).ctype == 2:
                skip = str(int(worksheet.cell(r, floor_skip_c).value))
            else:
                skip = str(worksheet.cell(r, floor_skip_c).value)

            if comment_c == -1:
                comment = ''
            else:
                comment = str(worksheet.cell(r, comment_c).value).strip()

            if worksheet.cell(r, flat_c).ctype == 2:
                flat = str(int(worksheet.cell(r, flat_c).value))
            else:
                flat = str(worksheet.cell(r, flat_c).value)

            num_lift = worksheet.cell(r, lift_num_c).value
            if num_lift == '' or num_lift is None:
                num_lift = '0'
            #elif num_lift not in ('?','-'):
            elif worksheet.cell(r, lift_num_c).ctype == 2:
                num_lift = str(int(worksheet.cell(r, lift_num_c).value))
            else:
                num_lift = str(worksheet.cell(r, lift_num_c).value)

            direction = worksheet.cell(r, direction_c).value
            if direction == '' or direction is None:
                direction = 0
            elif worksheet.cell(r, direction_c).ctype == 2:
                direction = str(int(worksheet.cell(r, direction_c).value))
            else:
                direction = str(worksheet.cell(r, direction_c).value)

            est_phase_id = worksheet.cell(r, est_phase_id_c).value
            if est_phase_id[0] == 'E':
                est_id = est_phase_id
                phase_id = ' '
            elif est_phase_id[0] == 'P':
                est_id = ' '
                phase_id = est_phase_id

            uid = bldg_id + '_' + str(r + 1) + '_' + str(filesize)

            sql_string = "insert into APIS.TEMP_UNIT_CONFIG_DTL(TEMP_UNIT_CONFIG_DTL_ID, VERSION_DATE, EST_ID, PHASE_ID, BLDG_ID, FLOOR_FRM, FLOOR_TO, FLOOR_SKIP, FLAT, NUM_ELEVATORS, DIRECTION_CO, COMMENTS, EXCEL_NAME) " \
                         "values ('{0}',to_date('{1}', 'yyyy/mm/dd'),'{2}', '{3}', '{4}', '{5}', '{6}', '{7}', '{8}', '{9}', '{10}', '{11}', '{12}');".format(uid, version_date, est_id, phase_id, bldg_id, frm, to, skip, flat, num_lift, direction, comment, excel_source_name)

            #print(uid)
            sql_param = {'TEMP_UNIT_CONFIG_DTL_ID': uid,
                         'VERSION_DATE': version_date,
                         'EST_ID': est_id,
                         'PHASE_ID': phase_id,
                         'BLDG_ID': bldg_id,
                         'FLOOR_FRM': frm,
                         'FLOOR_TO': to,
                         'FLOOR_SKIP': skip,
                         'FLAT': flat,
                         'NUM_ELEVATORS': num_lift,
                         'DIRECTION_CO': direction,
                         'COMMENTS': comment,
                         'EXCEL_NAME': excel_source_name}

            sql_execute = "insert into APIS.TEMP_UNIT_CONFIG_DTL(TEMP_UNIT_CONFIG_DTL_ID, VERSION_DATE, EST_ID, PHASE_ID, BLDG_ID, FLOOR_FRM, FLOOR_TO, FLOOR_SKIP, FLAT, NUM_ELEVATORS, DIRECTION_CO, COMMENTS, EXCEL_NAME, update_user_id) values " \
                          "(:TEMP_UNIT_CONFIG_DTL_ID, to_date(:VERSION_DATE, '" + date_type + "'), :EST_ID, :PHASE_ID, :BLDG_ID, :FLOOR_FRM, :FLOOR_TO, :FLOOR_SKIP, :FLAT, :NUM_ELEVATORS, :DIRECTION_CO, :COMMENTS, :EXCEL_NAME, 'EXCEL')"

            conn.insertSingle(sql_execute, sql_param)
            #param_list_dtl.append(sql_param)
        else:
            with open(config_err_txt, 'a') as f:
                f.write('BLDG_ID MISSING. EXCEL NAME: ' + excel_source_name + ' Row: ' + str(r) + ' Max Rows:' + str(end_r) + '\n')
    # sql_execute = "insert into APIS.TEMP_UNIT_CONFIG_DTL(TEMP_UNIT_CONFIG_DTL_ID, VERSION_DATE, EST_ID, PHASE_ID, BLDG_ID, FLOOR_FRM, FLOOR_TO, FLOOR_SKIP, FLAT, NUM_ELEVATORS, DIRECTION_CO, COMMENTS, EXCEL_NAME) values " \
    #             "(:TEMP_UNIT_CONFIG_DTL_ID, to_date(:VERSION_DATE, '" + date_type + "'), :EST_ID, :PHASE_ID, :BLDG_ID, :FLOOR_FRM, :FLOOR_TO, :FLOOR_SKIP, :FLAT, :NUM_ELEVATORS, :DIRECTION_CO, :COMMENTS, :EXCEL_NAME)"
    # print(len(param_list_dtl))
    # conn.insertBatch(sql_execute, param_list_dtl)

    for r in range(start_r, end_r):
        bldg_id = worksheet.cell(r, bldg_id_c).value
        uid = bldg_id + '_' + str(r + 1) + '_' + str(filesize)
        for c in range(start_c, end_c):
            main_id = worksheet.cell(0, c).value
            sub_id = worksheet.cell(1, c).value

            if main_id == '':
                if worksheet.cell(start_r - 2, c).value == '房間總數':
                    main_id = 'MP020'
                    sub_id = 'MP02000458'
                if worksheet.cell(start_r - 2, c).value == '套房總數':
                    main_id = 'MP020'
                    sub_id = 'MP02000459'
                if worksheet.cell(start_r - 2, c).value == '工作間連儲物室及廁所':
                    main_id = 'TOBEDEL'
                    sub_id = 'TOBEDEL'

            config_value = worksheet.cell(r, c).value
            #if config_value != 0 and config_value != '' and main_id != '':
            if config_value not in (0,'',' ') and main_id != '':
                config_value = int(config_value)
                sql_string = "insert into APIS.TEMP_UNIT_CONFIG_DTL_ITEMS(TEMP_UNIT_CONFIG_DTL_ID, UNIT_CONFIG_MAIN_ID, UNIT_CONFIG_SUB_ID, VALUE) " \
                             "values ('{0}', '{1}', '{2}', {3} );".format(uid, main_id, sub_id, config_value )
                # print(sql_string)
                sql_param = {'TEMP_UNIT_CONFIG_DTL_ID': uid,
                             'UNIT_CONFIG_MAIN_ID': main_id,
                             'UNIT_CONFIG_SUB_ID': sub_id,
                             'VALUE': config_value }

                param_list_item.append(sql_param)

    sql_execute = "insert into APIS.TEMP_UNIT_CONFIG_DTL_ITEMS(TEMP_UNIT_CONFIG_DTL_ID, UNIT_CONFIG_DTL_ID, UNIT_CONFIG_MAIN_ID, UNIT_CONFIG_SUB_ID, VALUE, update_user_id) values (:TEMP_UNIT_CONFIG_DTL_ID, 0, :UNIT_CONFIG_MAIN_ID, :UNIT_CONFIG_SUB_ID, :VALUE, 'EXCEL')"
    conn.insertBatch(sql_execute, param_list_item)

    end_time = time.time()
    elapsed_time = round(end_time - start_time, 2)
    print(excel_source_name + ' has been imported. Elasped time: ' + str(elapsed_time) + ' sec.')


if __name__ == '__main__':
    # try:
        # conn = cx.connect('APIS/APIS@10.0.0.182:1521/apispdb')
        conn = Oracle('APIS','APIS','10.0.0.182:1521','apispdb')
        excel_file_path = 'C:\Justin\APIS\config_excel_files'
        list = sorted(os.listdir(excel_file_path))
        for i in range(0, len(list)):
            excel_path = os.path.join(excel_file_path, list[i])
            if os.path.isfile(excel_path) and os.path.splitext(excel_path)[1] in ('.xls', '.xlsx'):
                # print(excel_path)
                excel_process(excel_path, conn)
        del conn

    # except Exception as err:
    #     print('Exception: ', err)
