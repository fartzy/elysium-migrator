#!/bin/bash

# TODO: Install the vertica helm chart for testing 
# if ! type helm &> /dev/null; then
#     curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/master/scripts/get-helm-3
#     chmod 700 get_helm.sh
#     ./get_helm.sh
#     rm get_helm.sh
# fi


if ! type vsql &> /dev/null; then
    export PATH=$PATH:/opt/vertica/bin
fi

# This path is also for `ybsql`
if ! type ybload &> /dev/null; then
    export PATH=/auto/rutil/sw/ybtools/5.1.1-20210331000622/opt/ybtools/bin:$PATH
fi
