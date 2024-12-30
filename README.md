# Elysium Vertica to Yellowbrick Migration #

## Introduction
This was an actual migration app I made to move over a Petabyte of data from Vertica to YellowBrick (YB) in production for a large fintech company. The object of this app is to create a CLI application to migrate `Elysium` data from Vertica to YB. In summary, `export` will pull data from Vertica, and drop it in a linux directory, and the `import` command will take that data and load it into YB.

## Inspiration ##
This application was started as a grassroots project and used heavily at one of the leading market making firms in the world.  As the firm migrated dozens of databases from Vertica to YellowBrick, this application was used more and more.

## Quick Start ##

### Set up the conda environment ###

From the root directory execute:

```shell script
make install 

conda activate elysium_env
```
__Note: Creating or updating the environment may tke several minutes.__

## Configuration File ##
This migration is driven by a configuration file - **elysium_migration/configuration/config.yaml**. After the `export` is completed, run `import` and pass in the same requried arguments synced with the `export`.  Both `--config-path` and `--output-path` need to be the same. So each `import` should really be using the same *config-path* and *output-path* as the just-completed `export` used. 

The configuration file has two arrays `tables` and `partitioning_columns` which are the only important things as of now in the configuration file.  Later on, schemas and other types of objects could be added.  The partitioning column should be chosen if it has a high cardinality ( being completely unique is the best and an integer is good too ). This application uses this column to split the data into chunks if shunking is turned on.  This chunking is a way to parallelize the exports that will work across all systems.  The parallelization will be a function of the language then. 

### Example `config.yaml` ###

```yaml
name: elysium_migration

objects:
  tables:
  - Elysium.FINGAM_Transactions
  - Elysium.FINGAM_Orders
  - Compliance.DeskLevelUser
  - Compliance.OverTheEdgeUserDB
  
  partitioning_columns:
  - Elysium.FINGAM_Transactions.ID
  - Elysium.FINGAM_Orders.ID
  - Compliance.DeskLevelUser.MartModifiedDate
  - Compliance.OverTheEdgeUserDB.MartModifiedDate
```

## Commands ##

**There are three commands of the application:**

* `export`
* `import`
* `housekeep`


### `export` command ### 

| Short Option | Long Option | Type | Description |
|------------- | ----------- | -----|-----------  |
| -fd | --from-date | TEXT | Min date to put in WHERE clause. i.e., retreive records only after this data date. |
| -td | --to-date | TEXT | Max date to put in WHERE clause. i.e., retreive records only before this data date. |
| -v/-nv | --validate/--no-validate | FLAG | To determine if we will do validation. FYI the import following an export validation wont work unless both exporter and import had validation turned on. |
|-i/-ni | --inject-envs-from-env-file/--no-inject-envs-from-env-file|FLAG| Set to ture if you want to use the .env file to load environment variables. |
|-sc | --script-dir | PATH | The path of the scripts directory.  This is useful to change the scripts without rebuilding the code. |
| -ed | --env-dir | PATH| This option is the path fo the directory of the .env file. |
| -c | --config-path | PATH | The path fo the config file that will determin determine whcih database objects to migrate. |
| -o | --output-path | PATH | The path of the directory where the output migration will be stored. |
| -s | --sample-size | INTEGER | Will determine the amount of records if you want to do a 'sample migration'. This is helpfule the check if the utility works. |
| -ll | --log-level | TEXT | Determins the level of logging. Valid levels are: CRITICAL, ERROR, WARNING, INFO, DEBUG, NOTSET |
| -lp | --log-path | TEXT | This otpion is the whole absolute path of the the log file. It is not checked for existence. |

----------------------------------------------------------------------------------------------------------------------------------------
### `import` command ### 

| Short Option | Long Option | Type | Description |
|------------- | ----------- | -----|-----------  |
| -vd | --val-dir | TEXT | Path where the results of the validation will be output to. |
| -v/-nv | --validate/--no-validate | FLAG | To determine if we will do validation. FYI the import following an export validation wont work unless both exporter and import had validation turned on. |
|-i/-ni | --inject-envs-from-env-file/--no-inject-envs-from-env-file|FLAG| Set to ture if you want to use the .env file to load environment variables. |
|-sc | --script-dir | PATH | The path of the scripts directory.  This is useful to change the scripts without rebuilding the code. |
| -ed | --env-dir | PATH| This option is the path fo the directory of the .env file. |
| -c | --config-path | PATH | The path of the config file that will determine which database objects to migrate. |
| -o | --output-path | PATH | The path of the directory where the output migration will be stored. |
| -ll | --log-level | TEXT | Determins the level of logging. Valid levels are: CRITICAL, ERROR, WARNING, INFO, DEBUG, NOTSET |
| -lp | --log-path | TEXT | This otpion is the whole absolute path of the the log file. It is not checked for existence. |

----------------------------------------------------------------------------------------------------------------------------------------
### `housekeep` command ### 

| Short Option | Long Option | Type | Description |
|------------- | ----------- | -----|-----------  |
| -t | --truncate-tables | FLAG | Ususally when doing migration you might ned to do many test runs. This could help expediate testing. |
|-i/-ni | --inject-envs-from-env-file/--no-inject-envs-from-env-file|FLAG| Set to ture if you want to use the .env file to load environment variables. |
|-sc | --script-dir | PATH | The path of the scripts directory.  This is useful to change the scripts without rebuilding the code. |
| -ed | --env-dir | PATH| This option is the path fo the directory of the .env file. |
| -c | --config-path | PATH | The path of the config file that will determine which database objects to migrate. |
| -ll | --log-level | TEXT | Determins the level of logging. Valid levels are: CRITICAL, ERROR, WARNING, INFO, DEBUG, NOTSET |
| -lp | --log-path | TEXT | This otpion is the whole absolute path of the the log file. It is not checked for existence. |

----------------------------------------------------------------------------------------------------------------------------------------
## Migration Process Configurations ##
These are different than in the configuration file above. These configs are the 'how', the other config file is the 'what'.

| Configuration | Suggested Default | Description |
|---------------|-------------------|-------------|
|DATA_FILES_EXTENSION | "csv" | File extension of data files.
|IDEMPOTENT EXPORT | True | Flag to delete the date range "logical partition". Needs to be kept true to ensure no double loading. |
|IMPORT_BATCH_FILES_FLAG | True | This is true by default. This should be optimized and only run when needed. It could make the migration slow. |
|IMPORT_FILES_BATCH_SIZE | 100 | Size of the batches of files to be imported. |
|EXPORT_CHUNK_SIZE_MB | 5000 | This could grow to much more when on disk. 5G in the database might mean 50G on disk decompressed. |
|EXPORT_WHOLE_TABLE_THRESHOLD_MB | 20000 | COmbined with EXPORT_WEEKS_WINDOW to determine if the data range is small enough for no parallelization. |
|EXPORT_WEEKS_WINDOW| 150 | If window is bigger, and table is smaller than EXPORT_WHOLE_TABLE_THRESHOLD_MB, then the whole exported in one process. |

## Examples ##
To execute an export with sampling...
```shell 
elysium-migrate-cli export  \
      -o /datadump/outputdata/ \
      -c $(pwd)/configuration/config.yaml \
      -s 5000
```

To execute an import...
```shell 
elysium-migrate-cli import \
      -o /datadump/outputdata/ \
      -c $(pwd)/configuration/config.yaml \
      -vd $(pwd)/validation
```

To execute full import and export for one day with the script "execute-elysium-migration-per-day.sh", using all the defaults...
```shell
. ./execute-elysium-migration-per-day.sh \
    "2019-02-05" \
    "2019-02-07" \
    "2019-02-06.app.log" \
    /datadumpdir/2019-02-06
```
