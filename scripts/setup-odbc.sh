# set path to unixODBC library.
export LD_LIBRARY_PATH=/opt/unixodbc/lib64:$LD_LIBRARY_PATH
# if connecting to Microsoft SQL Server
export LD_LIBRARY_PATH=/opt/unixodbc/lib64:/opt/microsoft/msodbcsql/lib64:$LD_LIBRARY_PATH
# if connecting to Sybase ASE then add the path to the Sybase ASE ODBC Library
export LD_LIBRARY_PATH=/opt/unixodbc/lib64:/opt/sybase/ASE/SDK/15.7/SP136/DataAccess64/ODBC/lib:$LD_LIBRARY_PATH
 
 
### PLEASE NOTE SYBASE IQ 15.4 and IQ 16.1 CAN NOT BE IN THE SAME PATH. PLEASE SET ONE OR THE OTHER ONLY!
# if connecting to Sybase IQ 15.4 then add the path to the Sybase IQ 15.4 ODBC Library
export LD_LIBRARY_PATH=/opt/unixodbc/lib64:/opt/sybase/ASE/SDK/15.7/SP136/DataAccess64/ODBC/lib:/opt/sybase/IQ/Client/15.4/ESD7/IQ-15_4/lib64:$LD_LIBRARY_PATH
# if connecting to Sybase IQ 16.1 then add the path to the Sybase IQ 16.1 ODBC Library
export LD_LIBRARY_PATH=/opt/unixodbc/lib64:/opt/sybase/ASE/SDK/15.7/SP136/DataAccess64/ODBC/lib:/opt/sybase/IQ/Client/16.1/SP04.PL03/IQ-16_1/lib64:$LD_LIBRARY_PATH
 
 
# set variables for ODBCINI and ODBCSYSINI
export ODBCINI=/opt/unixodbc/etc/odbc.ini
export ODBCSYSINI=/opt/unixodbc/etc
 
#unset ODBCINST if it is set for DataDirect Driver Manager
unset ODBCINST
  
# if using /opt/finance/bin/python, you need a different, unixODBC compatible pyodbc
# look for /opt/finance/alt/lib64/python, if doesn't exist ask to have finance-python27-pyodbc-uocompat installed
# ignore this if you're using a miniconda/anaconda distribution
export PYTHONPATH=/opt/finance/alt/lib64/python:$PYTHONPATH
 
# if using /opt/finance/bin/perl, you need a different, unixODBC compatible DBD::ODBC
# look for /opt/finance/alt/lib64/perl, if doesn't exist ask to have finance-perl-dbd-odbc-uocompat installed
export PERLLIB=/opt/finance/alt/lib64/perl:$PERLLIB
