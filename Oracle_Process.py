
import cx_Oracle

class Oracle(object):
    def __init__(self, userName, password, host, instance):
        self._conn = cx_Oracle.connect("%s/%s@%s/%s" % (userName, password, host, instance))
        self.cursor = self._conn.cursor()

    def queryTitle(self, sql, nameParams={}):
        if len(nameParams) > 0:
            self.cursor.execute(sql, nameParams)
        else:
            self.cursor.execute(sql)

        colNames = []
        for i in range(0, len(self.cursor.description)):
            colNames.append(self.cursor.description[i][0])

        return colNames

    # query methods
    def queryAll(self, sql):
        self.cursor.execute(sql)
        return self.cursor.fetchall()

    def queryOne(self, sql):
        self.cursor.execute(sql)
        return self.cursor.fetchone()

    def queryBy(self, sql, nameParams={}):
        if len(nameParams) > 0:
            self.cursor.execute(sql, nameParams)
        else:
            self.cursor.execute(sql)

        return self.cursor.fetchall()

    def insertBatch(self, sql, nameParams=[]):
        """batch insert much rows one time,use location parameter"""
        self.cursor.prepare(sql)
        self.cursor.executemany(None, nameParams)
        self.commit()

    def insertSingle(self, sql, nameParams={}):
        self.cursor.prepare(sql)
        self.cursor.execute(None, nameParams)
        self.commit()

    def commit(self):
        self._conn.commit()

    def __del__(self):
        if hasattr(self, 'cursor'):
            self.cursor.close()

        if hasattr(self, '_conn'):
            self._conn.close()
