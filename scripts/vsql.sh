#!/bin/bash

if [ "$#" -ne 3 ]; then
    >&2 echo "[-] Please use the .env file in the elysium-migration installation dir for sys variables."
fi  

vsql -h $VERTICA_HOST -U $VERTICA_USER -w $VERTICA_PASSWORD -d $VERTICA_DB
