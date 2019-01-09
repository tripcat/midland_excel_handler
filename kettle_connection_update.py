import os, re


kettle_script_path = r"C:\kettle\APIS"
kettle_new_script_path = r"C:\kettle\APIS\APIS_UAT"
old_ip = '10.0.0.182'
old_database = '/apispdb'

new_ip = '10.0.0.182'
new_database = '/apisuat'


def connection_update(kettle_script, new_kettle_script, old_ip, old_database, new_ip, new_database):
    pair_flag = 0

    f_new = open(new_kettle_script, 'w')

    with open(kettle_script, 'r') as f:
        for line in f.readlines():
            if re.search(r'<server>.*</server>', line) is not None:
                line = line.replace(old_ip, new_ip)
                pair_flag = 1
                print(line)
            if re.search(r'<database>.*</database>', line) is not None and pair_flag == 1:
                line = line.replace(old_database, new_database)
                pair_flag = 0
                print(line)
            else:
                pass
            f_new.write(line)

    f_new.close()

#kettle_script = r'C:\kettle\APIS\MDB_IDCONVERT.ktr'
#new_kettle_script = r'C:\kettle\APIS\APIS_UAT\MDB_IDCONVERT.ktr'
#connection_update(kettle_script, new_kettle_script, old_ip, old_database, new_ip, new_database)


list = sorted(os.listdir(kettle_script_path))
for i in range(0, len(list)):
    kettle_script = os.path.join(kettle_script_path, list[i])
    new_kettle_script = os.path.join(kettle_new_script_path, list[i])
    if os.path.isfile(kettle_script) and os.path.splitext(kettle_script)[1] in ('.ktr', '.kjb'):
        connection_update(kettle_script, new_kettle_script, old_ip, old_database, new_ip, new_database)
