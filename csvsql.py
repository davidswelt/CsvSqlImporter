#!/usr/bin/python
# -*- coding: utf-8 -*-

# usage: ./import-csv.py *.csv

# import one or more CSV into a MySQL database
# detects (some) column types and allows for caching these types for consistency

# Will connect to MySQL server via SSH tunnel (or local socket)

# This CsvSqlImporter is subclassed at end to import AMT worker tables and cross-reference
# Hostname etc coded at bottom
# bonus information

# written by reitter@psu.edu on 12/19/2014.
# (note to myself: do not spend more than this day on this!)


# Requirement:  Mysqldb   (pip install MySQL-python)

    

import subprocess
import time
import threading

# Credit for this class goes to:
# http://stackoverflow.com/questions/4364355/how-to-open-an-ssh-tunnel-using-python
# Todo: this one might be more complete: https://github.com/pahaz/sshtunnel/blob/master/sshtunnel.py
class SshTunnel(threading.Thread):
    def __init__(self, localport, remoteport, remoteuser, remotehost):
        threading.Thread.__init__(self)
        self.localport = localport      # Local port to listen to
        self.remoteport = remoteport    # Remote port on remotehost
        self.remoteuser = remoteuser    # Remote user on remotehost
        self.remotehost = remotehost    # What host do we send traffic to
        self.daemon = True              # So that thread will exit when
                                        # main non-daemon thread finishes

    def run(self):
        if subprocess.call([
            'ssh', '-N',
                   '-L', str(self.localport) + ':' + self.remotehost + ':' + str(self.remoteport),
                   self.remoteuser + '@' + self.remotehost ]):
            raise Exception ('SSH tunnel setup failed')



import re
from dateutil import parser
import datetime

import MySQLdb
from _mysql_exceptions import OperationalError,ProgrammingError


import csv

class CsvSqlImporter:

    # these are intended to be overloaded
    def additional_init(self):
        """Called after initialization in constructor.
        May override defaults."""
        pass
    def additional_table_prep (self, table):
        """Called before starting file import, after table has been
        created or verified to exist. Columns may or may not have been
        added to the table, as no file has been read yet."""
        pass
    def file_post_processing(self, new_entry_ids, imported_file):
        """Called after a file has been imported to the DB.
        new_entry_ids contains a list of the ID fields for
        the rows that were just inserted into the table.
        Transaction is committed and the method may query
        the DB to retrieve the row.
        """
        pass
    def after_insert_row(self, row_dict):
        """Called after each individual inserted row.
        Transaction may not be committed to DB at that point."""
        pass
    ##########

    
    @staticmethod
    def sanitize_name(t):
        t = re.sub(r"\.","",t)
        t = re.sub(r" ","_",t)
        return t

    def guess_type(self, example, colname):

        if colname in self.types:
            return CsvSqlImporter.get_converter(self.types[colname])

        try:
            float(example)
            return CsvSqlImporter.get_converter("FLOAT")
        except:
            pass

        try:
            if example[0] in "$£€" and len(example)>1:
                float(example[1:])
                return CsvSqlImporter.get_converter("FLOAT-X")
        except:
            pass
        try:
            if len(example)>4 and " " in example and (":" in example or "/" in example or "-" in example):
                dt = parser.parse(example)
                if dt:
                    return CsvSqlImporter.get_converter("DATETIME")
        except:
            pass
        return CsvSqlImporter.get_converter("STRING")

    @staticmethod
    def get_converter(type):
        if "FLOAT" == type:
            return (type, "FLOAT(4)", lambda x: float(x))
        if "FLOAT-X" == type:
            return (type, "FLOAT(4)", lambda x: float(x[1:]))
        if "DATETIME" == type:
            return (type, "DATETIME", lambda x: parser.parse(x).strftime('%Y-%m-%d %H:%M:%S'))
        return ("STRING", "VARCHAR(200) CHARACTER SET utf8", lambda x: x)

    def add_column(self, name, sqltype):
        query = "ALTER TABLE payments ADD COLUMN %s %s"
        query = query%(name,sqltype)
        try:
            self.cursor.execute(query)
            self.addl_columns_added = True
            return True
        except OperationalError as e:
            pass  # double entry
        return False


    def __init__(self, files, sql_user, sql_password, database, table, host="localhost", ssh_username=None):
        self.files = files
        self.host = host
        self.ssh_username = ssh_username
        self.sql_user = sql_user
        self.sql_password = sql_password
        self.database = database
        self.table = table
        self.cursor = None
        self.types = {}
        self.converters = {}
        self.port = 3306
        self.drop_table = False

        # internal
        self.addl_columns_added = False

        self.additional_init()
    
    def set_column_types(self, types):
        self.types = types



    def start_tunnel(self):
        tunnel = SshTunnel(9989, self.port, self.ssh_username, self.host)
        tunnel.start()
        time.sleep(1)
        self.port = 9989
        self.host = "127.0.0.1"
        
    def run(self):
        if self.ssh_username:
            self.start_tunnel()
            
        connection = MySQLdb.connect(host=self.host, port=self.port, user=self.sql_user, passwd=self.sql_password, db=self.database)

        cursor = connection.cursor()
        self.cursor = cursor
        
        
        if self.drop_table:
            try:
                cursor.execute("DROP TABLE %s"%self.table)
            except:
                pass

        try:
            cursor.execute("CREATE TABLE %s (id INT NOT NULL AUTO_INCREMENT,PRIMARY KEY (id)) ENGINE=MyISAM"%self.table)
        except:
            pass

        connection.commit()
        self.additional_table_prep(self.table)

        for file in self.files:
            print "Importing ", file, "...",

            new_entry_ids = [] # (hit_id, mturk_id)

            with open (file, 'r') as f:
                reader = csv.reader(f)
                columns = next(reader)
                firstrow = next(reader)
                while len(firstrow) < len(columns):
                        firstrow = firstrow + ['']

                columns = [CsvSqlImporter.sanitize_name(x) for x in columns]

                # create columns
                for c,ex in zip(columns,firstrow):
                    if c in self.converters:  # do not override
                        break

                    typ,sqltype,fun = self.guess_type(ex, c)
                    self.converters[c] = fun
                    self.types[c] = typ

                    self.add_column(c, sqltype)

                query = 'insert into {0}({1}) values ({2})'
                query = query.format(self.table, ','.join(columns), ','.join(['%s'] * len(columns)))
                i=0
                for data in [firstrow]+[line for line in reader]:
                    i = i + 1
                    while len(data) < len(columns):
                        data = data + ['']

                    data_conv = []
                    dd = {}

                    for c,d in zip(columns,data):
                        fun = self.converters[c]
                        # print c, d, fun(d)
                        dc = None
                        try:
                            dc = fun(d)

                        except:
                            print "failed to convert d=",d," as type", typ
                            dc = d
                        data_conv += [dc]

                        dd[c] = dc


                    try:
                        self.cursor.execute(query, data_conv)
                        # keep track
                        new_entry_ids += [self.cursor.lastrowid]
                        # single entry postprocessing
                        self.after_insert_row(zip(columns,data_conv))
    
                    except OperationalError as e:
                        print "error: ",e 

                connection.commit()

            # file should be closed now
            self.file_post_processing(new_entry_ids, file)
            connection.commit()

            print "%s entries imported."%len(new_entry_ids)
                
        # ok, update all bonus entries

        if self.addl_columns_added:
            print "Additional columns were added.  Complete list (for consistency): "
            print "importer.set_column_types(%s)"%self.types



