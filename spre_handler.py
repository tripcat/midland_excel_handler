import cx_Oracle as cx
import xlrd
from datetime import datetime
from xlrd import xldate_as_tuple
import os
from Oracle_Process import Oracle
import time
import re


def excel_process(excel_path, conn):
    workbook = xlrd.open_workbook(excel_path)
    excel_source_name = excel_path.split('\\')[-1]
    sheet_names = workbook.sheet_names()
    worksheet = workbook.sheet_by_index(0)
    param_list_dtl = []

    rows = worksheet.nrows
    cols = worksheet.ncols

    tx_multiunit_count = 0

    for r in range(1, rows):
        EST_NAME = worksheet.cell(r, 0).value
        EOS_MATCH = re.search(r'\(.*\)', EST_NAME)
        if EOS_MATCH:
            END_OF_SALES = EOS_MATCH.group()
            EST_NAME = re.sub('(?P<value>\(.*\))', '', EST_NAME)
            EST_NAME = EST_NAME.strip()
        else:
            END_OF_SALES = ''
        DEAL_DATE = datetime(*xldate_as_tuple(worksheet.cell_value(r, 1), 0)).strftime('%d/%m/%Y')

        BLDG_NAME = worksheet.cell(r, 2).value
        if worksheet.cell(r, 2).ctype == 2:
            BLDG_NAME = str(int(worksheet.cell(r, 2).value))
        else:
            BLDG_NAME = worksheet.cell(r, 2).value

        FLOOR = worksheet.cell(r, 3).value
        if worksheet.cell(r, 3).ctype == 2:
            FLOOR = str(int(worksheet.cell(r, 3).value))
        else:
            FLOOR = worksheet.cell(r, 3).value

        FLAT = worksheet.cell(r, 4).value
        if worksheet.cell(r, 4).ctype == 2:
            FLAT = str(int(worksheet.cell(r, 4).value))
        else:
            FLAT = worksheet.cell(r, 4).value

        CARPARK = worksheet.cell(r, 5).value
        CONSIDERATION = worksheet.cell(r, 6).value
        if CONSIDERATION == '':
            CONSIDERATION = 0

        CANCEL_AMOUNT = worksheet.cell(r, 7).value
        if CANCEL_AMOUNT != '':
            IF_CANCEL = 'Y'
        else:
            IF_CANCEL = 'N'

        FLAT_FLOOR = worksheet.cell(r, 8).value
        UNIT_AREA_NET = worksheet.cell(r, 9).value
        UNIT_PRICE_NET = worksheet.cell(r, 10).value
        CONFIG = worksheet.cell(r, 11).value

        RAW_ID = worksheet.cell(r, 12).value
        MASTER_ID = worksheet.cell(r, 13).value

        sql_param_tx = {'EST_NAME': EST_NAME,
                     'END_OF_SALES': END_OF_SALES,
                     'DEAL_DATE': DEAL_DATE,
                     'BLDG_NAME': BLDG_NAME,
                     'FLOOR': FLOOR,
                     'FLAT': FLAT,
                     'CARPARK': CARPARK,
                     'CONSIDERATION': CONSIDERATION,
                     'CANCEL_AMOUNT': CANCEL_AMOUNT,
                     'FLAT_FLOOR': FLAT_FLOOR,
                     'UNIT_AREA_NET': UNIT_AREA_NET,
                     'UNIT_PRICE_NET': UNIT_PRICE_NET,
                     'CONFIG': CONFIG,
                     'IF_CANCEL':IF_CANCEL,
                     'RAW_ID': RAW_ID,
                     'MASTER_ID': MASTER_ID}

        date_type = 'dd/mm/yyyy'
        sql_execute = "insert into APIS.TEMP_TRANSACTION_SRPE(EST_NAME, BLDG_NAME, DEAL_DATE, END_OF_SALES, FLOOR, FLAT, CARPARK, CONSIDERATION, CANCEL_AMOUNT, FLAT_FLOOR, UNIT_AREA_NET, UNIT_PRICE_NET, CONFIG, IF_CANCEL, RAW_ID, TX_GROUP_ID) " \
                      "values (:EST_NAME, :BLDG_NAME, to_date(:DEAL_DATE, '" + date_type + "'), :END_OF_SALES, :FLOOR, :FLAT, :CARPARK, :CONSIDERATION, :CANCEL_AMOUNT, :FLAT_FLOOR, :UNIT_AREA_NET, :UNIT_PRICE_NET, :CONFIG, :IF_CANCEL, :RAW_ID, :MASTER_ID)"

        # print(sql_param_tx)
        conn.insertSingle(sql_execute, sql_param_tx)
        param_list_dtl.append(sql_param_tx)

    date_type = 'dd/mm/yyyy'
    # sql_execute = "insert into APIS.TRANSACTION_SRPE(EST_NAME, BLDG_NAME, DEAL_DATE, END_OF_SALES, FLOOR, FLAT, CARPARK, CONSIDERATION, CANCEL_AMOUNT, FLAT_FLOOR, UNIT_AREA_NET, UNIT_PRICE_NET, CONFIG, IF_CANCEL, RAW_ID, TX_GROUP_ID) " \
    #               "values (:EST_NAME, :BLDG_NAME, to_date(:DEAL_DATE, '" + date_type + "'), :END_OF_SALES, :FLOOR, :FLAT, :CARPARK, :CONSIDERATION, :CANCEL_AMOUNT, :FLAT_FLOOR, :UNIT_AREA_NET, :UNIT_PRICE_NET, :CONFIG, :IF_CANCEL, :RAW_ID, :MASTER_ID )"
    # conn.insertBatch(sql_execute, param_list_dtl)
    print('imported tx number:' + str(len(param_list_dtl)))

    # sql_string = "insert into APIS.TRANSACTION_SRPE(EST_NAME, BLDG_NAME, DEAL_DATE, FLOOR, FLAT, CARPARK, CONSIDERATION, CANCEL_AMOUNT, FLAT_FLOOR, UNIT_AREA_NET, UNIT_PRICE_NET, CONFIG) " \
    #              "values ('{0}', '{1}', to_date('{2}', 'dd/mm/yyyy'), '{3}', '{4}', '{5}', {6}, {7}, '{8}', {9}, {10}, '{11}');".format(
    #              EST_NAME, BLDG_NAME, DEAL_DATE, FLOOR, FLAT, CARPARK, CONSIDERATION, CANCEL_AMOUNT, FLAT_FLOOR, UNIT_AREA_NET, UNIT_PRICE_NET, CONFIG)



if __name__ == '__main__':
    # try:
        conn = Oracle('APIS','APIS','10.0.0.182:1521','apispdb')
        excel_file_path = 'C:\Justin\APIS\spre_excel_files'
        list = sorted(os.listdir(excel_file_path))
        for i in range(0, len(list)):
            excel_path = os.path.join(excel_file_path, list[i])
            if os.path.isfile(excel_path) and os.path.splitext(excel_path)[1] in ('.xls', '.xlsx'):
                # print(excel_path)
                excel_process(excel_path, conn)
        del conn
