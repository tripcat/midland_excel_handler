#import xlrd
from datetime import datetime
from xlrd import xldate_as_tuple
#import os
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
            elif last_part == 'OI':
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

n1 = "CHAN SUI OI"
bs_type = 's'
name_part, percentage_part, flag_abnormal_char, name_space_num = name_analyzer(n1,bs_type)