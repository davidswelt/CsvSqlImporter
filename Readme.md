CSV -> SQL Import Class
==================

Python 2.7 compatible

To use, create a CsvSqlImporter object with arguments indicating
database credentials, target database and table, and SSH login if
tunneling is desired (SSH required).

CsvSqlImporter is intended to be subclassed to provide functionality
specific to the application.

See example.py  for an example.

See git log for changes and version numbers.



Written by
--------

David Reitter, Penn State, reitter@psu.edu


License
GPL v.3 or later.  Use at your own risk.


To Do
-----

Replace MySqlDb with a more generic library or Oracle's connector
(something that supports Python3 perhaps)

Recognize more types
Lookahead for type recognition into more than the first row
