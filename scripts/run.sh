DIR=`pwd`

source "$DIR/scripts/install.sh" 
source "$DIR/scripts/getenv.sh" 
source "$DIR/scripts/setup-odbc.sh"

#formatting
isort **/*.py
black .

# TODO: decide if add VERTICAINI - need for local MACOS
# export VERTICAINI=/usr/local/etc/vertica.ini

#options for ybload
export PGOPTIONS=--search_path=moncust,cesomar,csinomar

# change path to handle vsql also 
export PATH=$PATH:/opt/vertica/bin

# cd $DIR
# eval "$(conda shell.bash hook)"
# conda activate conda_env
