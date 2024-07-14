import pyspark
from pyspark.sql.session import sparkSession
from pyspark import sparkFiles
import collections
import logging
import argparse
from datetime import datetime, date, timedelta
from pyspark.sql.functions import *

 
def get_source_incremental_data(last_run_timestamp):

    """
        - Reads MYSQL source connection, columns, and target location details from the config file
        - Return increamentally sourced data from mysql & target azure abdf location
    """

    try:
        with open(SparkFiles.get(source_target_config_file) , 'rb') as file:
            conf = json.load(file)
            json_map = disc((k.strip(),upper(), v) for j,v in conf.items())
            
        conf_details = disc((k.strip(),v) for k,v in conf.items())
        
        url = conf_details['url']
        table_name = conf_details['table_name']
        target_path = conf_details['target_path']
        select_column_list = conf_details['select_column_list']
        user = conf_details['properties']['user']
        password = conf_details['properties']['password']
        driver = conf_details['properties']['driver']  
    
        incremental_df = spark.read.jdbc(url=url, table=table_name, 
                            properties=properties, 
                            lowerBound=last_run_timestamp, upperBound=None, 
                            connectionProperties={"query": f"SELECT {', '.join(select_column_list)} FROM {table_name}"})
        return incremental_df,table_name,target_path
                             
    except Exception as e:
        logger.error("Error in get_source_incremental_data due to " + str(e))
        
                        
def load_column_derivations():
  """
  - Reads column derivation definitions from a configuration file.
   - Returns dictionary mapping target column names to their derivation expressions.
  """
    try:
        derivations = {}
        with open(column_derivations_config_file) as f:
            for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            target_col, expr_str = line.split("=", 1)
            derivations[target_col.strip()] = expr(expr_str.strip()) 
        return derivations
    except Exception as e:
        logger.error("Error in load_column_derivations due to " + str(e))


def apply_column_derivations(df, derivations):
    """  
    -Applies column derivations defined in a dictionary to a DataFrame.
    -A new PySpark DataFrame with derived columns.
    """
    try:
        for target_col, expr in derivations.items():
            df = df.withColumn(target_col, expr)
        return df
    except Exception as e:
        logger.error("Error in apply_column_derivations due to " + str(e))        
 

def getLogger(name, level = logging, INFO):
    """
    Its a logger function to print different levels for logging
    """
    
    logger = logging.getLogger(name)
    logger.setLevel(level)
    if logger.handlers:
        pass
    else   
        ch = logging.StreamHandler(sys.stderr)
        ch.setLevel(level)
        formatter = logging.Formatter('%(asctime)s -%(name)s -%(levelname)s -%(message)s')
        sh.setFormatter(formatter)
        logger.addHandler(ch)
    return logger
                          



if __name__ == '__main__':
    
    parser = argparse.ArgumentParser()
    parser.add.argument("--MODULE")
    input args parser.parse_args()
    
    if input_args.MODULE_NAME is None:
        print("Not an valid input")
        sys.exit()
    module = input.args.MODULE_NAME
    logger.info ("Module_name " ,+str(module))
    
    spark = SparkSession.builder.appName("Build_ETL_Pipline_"+_module).getOrCreate()
    logger = getlogger(module)
    
    try:
    
        global spark,module,target_path,table_name, source_target_config_file, column_derivations_config_file
        source_target_config_file = "source_target_config.json"
        column_derivations_config_file = "column_derivations.conf"  # Replace with your actual file path
        
        

        # Read source data incrementally
        logger.info("Read source data incrementally for the module "+ module)
        initial_df, target_path, table_name = get_source_incremental_data(last_run_timestamp=None)
        last_run_timestamp = initial_df.selectExpr("max(last_updated)").first()[0]
        incremental_df, target_path, table_name = get_source_incremental_data(last_run_timestamp)
        
        
        # Load column derivations from configuration file
        logger.info("get column derivations for the module "+ module)
        column_derivations = load_column_derivations(spark, config_file)
        
        
        # Apply column derivations
        logger.info("Apply column derivation for the module "+ module)
        derived_df = apply_column_derivations(source_df.copy(), column_derivations)
        #derived_df.show()        

        
        ## write the results to csv in Azure (ABFS) data lake 
        logger.info("Writting transformed data to csv file in the azure data lake ")s
        final_df = df.write().csv(target_path).option("header", True).option("delimiter", ",").option("quote", '"').save()
       

    except Exception as e:
        logger.error("Error in main program due to " + str(e))

