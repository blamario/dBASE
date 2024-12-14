The present package provides some rudimentary functionality for handling dBASE files, *i.e.*, files with the `.dbf`
extension. There are different versions of dBASE format, but the most typical and what this package supports is dBASE
III.

There's a library, a test suite, and four executables:

* `dbf2csv` converts a dBASE file to CSV,
* `csv2dbf` does the opposite,
* `dbfcut` selects a subset of the existing input dBASE columns to the output dBASE file, and
* `dbfzip` takes two dBASE files *with the same number of records* and combines all their columns.

Note that `dbf2csv` is necessarily a lossy transformation, since the only column metadata that can be represented in
CSV is the name.
