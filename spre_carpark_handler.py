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
        ROWNUM = int(worksheet.cell(r, 0).value)
        for f in range(1,cols-1):

            if worksheet.cell(r, f).ctype == 2:
                cp_desc = str(int(worksheet.cell(r, f).value))
            else:
                cp_desc = str(worksheet.cell(r, f).value)

            if cp_desc not in ('', ' '):
                cp_desc = str.strip(cp_desc)
                sql_param_cp = {'ROW_NUM': ROWNUM,
                                'CP_NAME': cp_desc}
                print(sql_param_cp)
                param_list_dtl.append(sql_param_cp)
            else:
                pass

    sql_execute = "insert into APIS.TEMP_TRANSACTION_SRPE_CARPARK(ROW_NUM, CARPARK_NAME) " \
                  "values (:ROW_NUM, :CP_NAME)"
    conn.insertBatch(sql_execute, param_list_dtl)
    print('imported cp number:' + str(len(param_list_dtl)))

if __name__ == '__main__':
    # try:
        conn = Oracle('APIS','APIS','10.0.0.182:1521','apispdb')
        excel_file_path = 'C:\Justin\APIS\spre_excel_files\carpark'
        list = sorted(os.listdir(excel_file_path))
        for i in range(0, len(list)):
            excel_path = os.path.join(excel_file_path, list[i])
            if os.path.isfile(excel_path) and os.path.splitext(excel_path)[1] in ('.xls', '.xlsx'):
                # print(excel_path)
                excel_process(excel_path, conn)
        del conn
