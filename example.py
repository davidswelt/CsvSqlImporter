#!/usr/bin/env python2.7

# Usage example:

from csvsql import CsvSqlImporter
import sys

importer = CsvSqlImporter(sys.argv[1:], "admin", "mypass", "AccountingTable", "PaymentsTable", "server.example.com", "jsmith")

# the following is optional, but helpful.  After the first run, it is output by the importer
importer.set_column_types({'Name': 'STRING', 'Title': 'STRING','AcceptTime': 'DATETIME', 'Amount': 'FLOAT-X'})   


importer.run()
