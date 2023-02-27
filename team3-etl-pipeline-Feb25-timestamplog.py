##########################################################################################################
# Import Libraries/Packages
##########################################################################################################
import os

import pymysql
import math
import csv
import pymysql as mysql
import sqlalchemy
from sqlalchemy import text
import mysql.connector
import traceback
from sqlalchemy import create_engine
import pandas as pd
import pyodbc
import time
from sqlalchemy import exc
import psutil
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine import reflection
import mariadb
from sqlalchemy.types import Integer, Text, String, DateTime
from sqlalchemy import create_engine, Table, Column, Integer, Unicode, MetaData, String, Text, update, and_, select, func, types
import sqlalchemy as sqla
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy import inspect
import sys
import logging.config
import logging
import time
import configparser
import sqlite3

##########################################################################################################
# Logging Configuration
##########################################################################################################
# Create a module specific new logging object for the ETL pipeline
logger = logging.getLogger(__name__)

# Capture logs at DEBUG level of lower (this includes INFO level)
# By default, logs are written to the standard console.  Here we will write all logs to the filename
timestr = time.strftime("%Y%m%d-%H%M%S")
log_filename = timestr + '.log'
#'etl-pipeline.log'
logging.basicConfig(filename=log_filename, encoding='utf-8', filemode='a', level=logging.DEBUG)

# Set the log message format
logging.basicConfig(format='%(levelname)s: %(asctime)s %(message)s', level=logging.DEBUG)

# Create handler to output to console as application executes
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

use_database = "USE ConsumerExpenditures8;"
dbName = "ConsumerExpenditures8"

local_sql_user = "root"
local_sql_password = "1234"
##########################################################################################################
# Extract Helper Method: extract data from Data Source 1 - Bureau of Labor Statistics Consumer Expenditures
##########################################################################################################
def extract_bls_consumer_expenditures():

    logger.info('Start Consumer Expenditures Database Extract Session')

    try:

        # Read tables from the Consumer Expenditures database and load it into the local MySQL database
        start1 = time.time()
        srcEngine = sqlalchemy.create_engine("mariadb+mariadbconnector://guest:relational@relational.fit.cvut.cz:3306/ConsumerExpenditures")
        conn = srcEngine.connect()
        end1 = time.time() - start1
        logger.info("Connect to relational.fit.cvut.cz ConsumerExpenditures database : {} seconds".format(end1))

        start1 = time.time()
        sql = "SELECT * FROM EXPENDITURES;"
        df_expenditures = pd.read_sql_table('EXPENDITURES', conn)
        df_expenditures.to_csv('expenditures.csv', encoding='utf-8', index=False)
        end1 = time.time() - start1
        logger.info("Writing expenditures table to CSV file : {} seconds".format(end1))

        start1 = time.time()
        sql = "SELECT * FROM HOUSEHOLDS;"
        df_households = pd.read_sql_table('HOUSEHOLDS', conn)
        df_households.to_csv('households.csv', encoding='utf-8', index=False)
        end1 = time.time() - start1
        logger.info("Writing households table to CSV file : {} seconds".format(end1))

        start1 = time.time()
        sql = "SELECT * FROM HOUSEHOLD_MEMBERS;"
        df_household_members = pd.read_sql_table('HOUSEHOLD_MEMBERS', conn)
        df_household_members.to_csv('household_members.csv', encoding='utf-8', index=False)
        end1 = time.time() - start1
        logger.info("Writing household members table to CSV file : {} seconds".format(end1))

        # connect to local database
        database_uri = 'mysql+pymysql://{}:{}@localhost:3306'.format(local_sql_user, local_sql_password)
        localEngine = sqlalchemy.create_engine(database_uri)

        try:
            with localEngine.connect() as conn_local:

                # create ConsumerExpenditures database if not exists else start copying over tables
                #database = 'ConsumerExpenditures10'
                result = conn_local.execute(text("CREATE DATABASE IF NOT EXISTS {0} ".format(dbName)))

                df_expenditures_csv = pd.read_csv("expenditures.csv",
                                                 sep=",",
                                                 engine='python',
                                                 index_col=False)

                df_households_csv = pd.read_csv("households.csv",
                                                  sep=",",
                                                  engine='python',
                                                  index_col=False)

                df_household_members_csv = pd.read_csv("household_members.csv",
                                                  sep=",",
                                                  engine='python',
                                                  index_col=False)

                result = conn_local.execute(text(use_database))

                start1 = time.time()
                result_table1 = conn_local.execute(text("""
                CREATE TABLE IF NOT EXISTS EXPENDITURES (EXPENDITURE_ID varchar(11) PRIMARY KEY NOT NULL,
                    HOUSEHOLD_ID VARCHAR(10) NOT NULL,
                    YEAR INT(11) NOT NULL,
                    MONTH INT(11) NOT NULL, 
                    PRODUCT_CODE VARCHAR(6) NOT NULL,
                    COST DOUBLE NOT NULL,
                    GIFT INT(11) NOT NULL,
                    IS_TRAINING INT(255) NOT NULL,
                    INDEX NAME(HOUSEHOLD_ID))
                """))
                end1 = time.time() - start1
                logger.info("Creating EXPENDTIURES table : {} seconds".format(end1))

                start1 = time.time()
                result_table2 = conn_local.execute(text("""
                                CREATE TABLE IF NOT EXISTS HOUSEHOLDS (HOUSEHOLD_ID varchar(10) PRIMARY KEY NOT NULL,
                                    YEAR int(11) NOT NULL,
                                    INCOME_RANK double NOT NULL,
                                    INCOME_RANK_1 double NOT NULL,
                                    INCOME_RANK_2 double NOT NULL, 
                                    INCOME_RANK_3 double NOT NULL,
                                    INCOME_RANK_4 double NOT NULL,
                                    INCOME_RANK_5 double NOT NULL,
                                    INCOME_RANK_MEAN double NOT NULL,
                                    AGE_REF int(11))
                                """))
                end1 = time.time() - start1
                logger.info("Creating HOUSEHOLDS table : {} seconds".format(end1))

                start1 = time.time()
                result_table3 = conn_local.execute(text("""
                                CREATE TABLE IF NOT EXISTS HOUSEHOLD_MEMBERS (HOUSEHOLD_ID varchar(10) NOT NULL,
                                    YEAR INT(11) NOT NULL,
                                    MARITAL VARCHAR(1) NOT NULL,
                                    SEX VARCHAR(1) NOT NULL, 
                                    AGE INT(11) NOT NULL,
                                    WORK_STATUS VARCHAR(2) DEFAULT NULL,
                                    INDEX NAME(HOUSEHOLD_ID))
                """))
                end1 = time.time() - start1
                logger.info("Creating HOUSEHOLD_MEMBERS table : {} seconds".format(end1))

                # Load dataframe into datawarehouse - this is the LOAD portion of ELT
                load_ce_expenditures(df_expenditures_csv, 'EXPENDITURES')
                load_ce_households(df_households_csv, 'HOUSEHOLDS')
                load_ce_household_members(df_household_members_csv, 'HOUSEHOLD_MEMBERS')


        except exc.SQLAlchemyError as e:
            print(type(e))

        except:
            # printing stack trace
            traceback.print_exc()

    except exc.SQLAlchemyError as e:
        print(type(e))

    except:
        # printing stack trace
        traceback.print_exc()



##########################################################################################################
# Helper Extract Method - Read from Interest Rates
##########################################################################################################
def extract_gdp():
    logger.info('Start GDP Database Extract Session')

    try:
        database_uri = 'mysql+pymysql://{}:{}@localhost:3306'.format(local_sql_user, local_sql_password)
        localEngine = sqlalchemy.create_engine(database_uri)

        with localEngine.connect() as conn_local:

            result = conn_local.execute(text(use_database))

            start1 = time.time()
            result_table1 = conn_local.execute(text("""
                            CREATE TABLE IF NOT EXISTS GDP (gdp_year YEAR NOT NULL,
                                MONTH INT NOT NULL,
                                DAY int NOT NULL,
                                FEDERAL_FUNDS_TARGET_RATE double NOT NULL, 
                                FEDERAL_FUNDS_UPPER_TARGET double NOT NULL,
                                FEDERAL_FUNDS_LOWER_TARGET DOUBLE NOT NULL,
                                EFFECTIVE_FEDERAL_FUNDS_RATE double NOT NULL,
                                REAL_GDP double NOT NULL,
                                UNEMPLOYMENT_RATE double not null,
                                INFLATION_RATE double not null)
                            """))
            end1 = time.time() - start1
            logger.info("Creating GDP table : {} seconds".format(end1))

            start1 = time.time()
            with open('C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/gdp.csv', 'r') as f:
                reader = csv.reader(f)
                next(reader)  # Skip the header row

                for row in reader:
                    gdp_year = row[0]
                    if gdp_year == '0':
                        gdp_year = '0'
                    month = row[1]
                    if month == '0':
                        month = '0'
                    day = row[2]
                    ff_target_rate = row[3]
                    if ff_target_rate == "":
                        ff_target_rate = '0'
                    ff_upper_target = row[4]
                    if ff_upper_target == "":
                        ff_upper_target = '0'
                    ff_lower_target = row[5]
                    if ff_lower_target == "":
                        ff_lower_target = '0'
                    ff_rate = row[6]
                    if ff_rate == "":
                        ff_rate = '0'
                    real_gdp = row[7]
                    if real_gdp == "":
                        real_gdp = '0'
                    unemp_rate = row[8]
                    if unemp_rate == "":
                        unemp_rate = '0'
                    inflation_rate = row[9]
                    if inflation_rate == "":
                        inflation_rate = '0'
                    query = 'INSERT INTO GDP (GDP_YEAR, MONTH, DAY,' + 'FEDERAL_FUNDS_TARGET_RATE,' + 'FEDERAL_FUNDS_UPPER_TARGET,' + 'FEDERAL_FUNDS_LOWER_TARGET,'+ 'EFFECTIVE_FEDERAL_FUNDS_RATE,' + 'REAL_GDP,' + 'UNEMPLOYMENT_RATE,' + 'INFLATION_RATE) VALUES (' + gdp_year + "," + month + "," + day + "," + ff_target_rate + "," + ff_upper_target + "," + ff_lower_target + "," + ff_rate + "," + real_gdp + "," + unemp_rate + "," + inflation_rate + ')'
                    result = conn_local.execute(text(query))

                conn_local.commit()

            end1 = time.time() - start1
            logger.info("Inserted dataframe rows into GDP table : {} seconds".format(end1))

    except:
       # printing stack trace
       traceback.print_exc()


##########################################################################################################
# Helper Extract Method - Read from Data Source 3
##########################################################################################################
def extract_cpi():
    logger.info('Start CPI Database Extract Session')

    try:
        # Connect to local database
        database_uri = 'mysql+pymysql://{}:{}@localhost:3306'.format(local_sql_user, local_sql_password)
        localEngine = sqlalchemy.create_engine(database_uri)

        with localEngine.connect() as conn_local:

            result = conn_local.execute(text(use_database))

            start1 = time.time()
            result_table1 = conn_local.execute(text("""
                               CREATE TABLE IF NOT EXISTS CPI (YEARMON DATE NOT NULL,
                                   CPI DOUBLE NOT NULL)
                               """))
            end1 = time.time() - start1
            logger.info("Creating CPI table : {} seconds".format(end1))

            start1 = time.time()
            with open('C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/USCPI.csv', 'r') as f:
                reader = csv.reader(f)
                next(reader)  # Skip the header row

                for row in reader:
                    yearmon = row[0]
                    cpi = row[1]
                    query = 'INSERT INTO CPI (YEARMON, CPI) VALUES (STR_TO_DATE(\'' + yearmon + "\', '%m-%d-%Y')," + cpi + ')'
                    result = conn_local.execute(text(query))

                conn_local.commit()

            end1 = time.time() - start1
            logger.info("Inserted dataframe rows into CPI table : {} seconds".format(end1))

    except:
        # printing stack trace
        traceback.print_exc()


##########################################################################################################
# Extract Method
# Read data from various data sources:
# log source file name, source file count if needed, format of the file, file size, source table name,
# source DB connection details, any exceptions/errors if file/source table missing or failed to fetch data
##########################################################################################################
def extract():
    logger.info('Start Extract Session')

    try:
        extract_bls_consumer_expenditures()
        extract_gdp()
        extract_cpi()

    except ValueError as e:
        logger.error(e)

    logger.info("Extract Complete")

##########################################################################################################
# Transform/Format/Validate the Data
##########################################################################################################
def transformation():
    logger.info('Start Transformation Session')

    try:
        # Connect to local database
        database_uri = 'mysql+pymysql://{}:{}@localhost:3306'.format(local_sql_user, local_sql_password)
        localEngine = sqlalchemy.create_engine(database_uri)

        with localEngine.connect().execution_options(
            stream_results=True) as conn_local:

            result = conn_local.execute(text(use_database))


            # TRANSFORMATION #1 - convert cpi column of datetime to year column
            #query = text("""
            #                ALTER TABLE CPI ADD CPI_YEAR year;
            #                UPDATE cpi SET CPI_YEAR = EXTRACT(YEAR FROM cpi.YEARMON )
            #                """)
            #result = conn_local.execute(query)


            # TRANSFORMATION #2 - update household_members marital column with readable values
            # using values from here: https://www.bls.gov/cex/pumd/ce_pumd_interview_diary_dictionary.xlsx
            query = text("ALTER TABLE household_members modify MARITAL VARCHAR(25);")
            result = conn_local.execute(query)
            query = text("UPDATE household_members SET `MARITAL`='Married' where `MARITAL`='1';")
            result = conn_local.execute(query)
            query = text("UPDATE household_members SET `MARITAL`='Widowed' where `MARITAL`='2';")
            result = conn_local.execute(query)
            query = text("UPDATE household_members SET `MARITAL`='Divorced' where `MARITAL`='3';")
            result = conn_local.execute(query)
            query = text("UPDATE household_members SET `MARITAL`='Separated' where `MARITAL`='4';")
            result = conn_local.execute(query)
            query = text("UPDATE household_members SET `MARITAL`='Never Married' where `MARITAL`='5';")
            result = conn_local.execute(query)


            # TRANSFORMATION #3 - update household_members work_status column with readable values
            # using values from here: https://www.bls.gov/cex/pumd/ce_pumd_interview_diary_dictionary.xlsx
            query = text("ALTER TABLE household_members modify WORK_STATUS VARCHAR(25);")
            result = conn_local.execute(query)
            query = text("UPDATE household_members SET `WORK_STATUS`= 'NA' where `WORK_STATUS`='nan';")
            result = conn_local.execute(query)
            query = text("UPDATE household_members SET `WORK_STATUS`= 'Self-Employed' where `WORK_STATUS` like '%2%';")
            result = conn_local.execute(query)
            query = text("UPDATE household_members SET `WORK_STATUS`= 'Working without pay' where `WORK_STATUS`like '%3%';")
            result = conn_local.execute(query)
            query = text("UPDATE household_members SET `WORK_STATUS`= 'Employed' where `WORK_STATUS`like '%1%';")
            result = conn_local.execute(query)


            # TRANSFORMATION #4 - update household_members SEX column with readable values
            # using values from here: https://www.bls.gov/cex/pumd/ce_pumd_interview_diary_dictionary.xlsx
            query = text("ALTER TABLE household_members modify SEX VARCHAR(25);")
            result = conn_local.execute(query)
            query = text("UPDATE household_members SET `SEX`= 'Male' where `SEX` = '1';")
            result = conn_local.execute(query)
            query = text("UPDATE household_members SET `SEX`= 'Female' where `SEX` = '2';")
            result = conn_local.execute(query)


            # TRANSFORMATION #5 - update household_members YEAR column to YEAR date type
            query = text("ALTER TABLE household_members modify COLUMN `YEAR` varchar(25);")
            result = conn_local.execute(query)
            query = text("ALTER TABLE household_members modify COLUMN `YEAR` YEAR;")
            result = conn_local.execute(query)


            # TRANSFORMATION #6 - update households YEAR column to YEAR date type
            query = text("ALTER TABLE households modify COLUMN `YEAR` varchar(25);")
            result = conn_local.execute(query)
            query = text("ALTER TABLE households modify COLUMN `YEAR` YEAR;")
            result = conn_local.execute(query)


            # TRANSFORMATION #7 - update expenditures YEAR column to YEAR date type
            query = text("ALTER TABLE expenditures modify COLUMN `YEAR` varchar(25);")
            result = conn_local.execute(query)
            query = text("ALTER TABLE expenditures modify COLUMN `YEAR` YEAR;")
            result = conn_local.execute(query)


            # TRANSFORMATION #8 - update expenditures PRODUCT_CODE with product descriptions
            query = text("ALTER TABLE expenditures modify COLUMN `YEAR` varchar(25);")
            result = conn_local.execute(query)
            query = text("ALTER TABLE expenditures modify COLUMN `YEAR` YEAR;")
            result = conn_local.execute(query)


            # TRANSFORMATION #9 - Create comprehensive SQL View of GDP, CPI and Consumer Expenditures Data
            # with each row being one Consumer Expenditure Purchase
            # This view will be made available as the business warehouse interface
            # for Applications like Tableau users to create dashboards
            start1 = time.time()
            query = text("""create view `BUSINESS_WAREHOUSE` as select e.expenditure_id, e.household_id, e.year, e.month, e.product_code, 
                            e.cost, e.gift, e.is_training, 
                            hm.marital, hm.sex, hm.age, hm.work_status, h.income_rank, 
                            h.income_rank_1, h.income_rank_2, h.income_rank_3, h.income_rank_4, 
                            h.income_rank_5, h.income_rank_mean, g.FEDERAL_FUNDS_TARGET_RATE, 
                            g.FEDERAL_FUNDS_UPPER_TARGET, g.FEDERAL_FUNDS_LOWER_TARGET, 
                            g.EFFECTIVE_FEDERAL_FUNDS_RATE, 
                            g.REAL_GDP, g.UNEMPLOYMENT_RATE, g.INFLATION_RATE, c.CPI
                            from expenditures e
                            inner join household_members hm 
                            on hm.household_id = e.HOUSEHOLD_ID 
                            inner join households h
                            on h.household_id = hm.HOUSEHOLD_ID
                            inner join gdp g 
                            on g.gdp_year = e.`YEAR`
                            inner join cpi c 
                            on year(c.YEARMON) = g.gdp_year""")

            result = conn_local.execute(query)
            end1 = time.time() - start1
            logger.info("Create SQL View business_warehouse : {} seconds".format(end1))


            # https://pythonspeed.com/articles/pandas-sql-chunking/
            # https://stackoverflow.com/questions/69711599/pandas-read-sql-from-ms-sql-gets-stuck-for-queries-with-275-chars-in-linux
            # Takes too long to execute this query: df_final_table = pd.read_sql(query, conn_local)
            # So we have to do it in chunks to load into a pandas dataframe and then write that to the loading_table
            #for chunk_dataframe in pd.read_sql_query(query, conn_local, chunksize=1000):
            #    print(
            #        f"Got dataframe w/{len(chunk_dataframe)} rows"
            #    )

            #    # write this dataframe chunk into the LOADING_TABLE
            #    result = chunk_dataframe.to_sql(name='LOADING_TABLE', con=conn_local, if_exists='append', index=False)

            #result = df_final_table.to_sql(name='LOADING_TABLE', con=conn_local, if_exists='append', index=False)
            conn_local.commit()

        logger.info("Transformation completed,data ready to load!")
        # log failed/exception messages if out of memory during processing, any data/format conversion required

    except:
        # printing stack trace
        traceback.print_exc()


##########################################################################################################
# Load Consumer Expenditures - Expenditures Table Data Into Business Warehouse
# https://www.confessionsofadataguy.com/pandas-dataframe-to_sql-how-you-should-configure-it-to-not-be-that-guy/
##########################################################################################################
def load_ce_expenditures(df_datasource_table, tbl):

    logger.info('Start Load Session - Consumer Expenditures - Expenditures Table')
    #logger.info("Loading dataframe %s into table %s", df_datasource_table.name, tbl.name)

    try:
        # Connect to local MySQL database
        start1 = time.time()
        #conn = mysql.connector.connect(
        #    host="localhost",
        #    user="root",
        #    password="1234",
        #    database="ConsumerExpenditures10")
        conn = mysql.connector.connect(
            host="localhost",
            user=local_sql_user,
            password=local_sql_password,
            database=dbName)
        end1 = time.time() - start1
        logger.info("Connect to local database : {} seconds".format(end1))

        cursor = conn.cursor()
        start1 = time.time()

        # Now we will load the data in the dataframe into the EXPENDITURES table
        # First, check if EXPENDITURES table is filled
        sql = "SELECT COUNT(*) FROM `EXPENDITURES`"
        query_result = cursor.execute(sql)
        result = cursor.fetchone()

        if result[0] < 851342:

            # Insert records from EXPENDITURES csv file
            cols = "`,`".join([str(i) for i in df_datasource_table.columns.tolist()])
            for i, row in df_datasource_table.iterrows():
                expenditure_id = str(row[0])
                expenditure_id = expenditure_id[:expenditure_id.find('.')]
                household_id = str(row[1])
                household_id = household_id[:household_id.find('.')]
                year = str(row[2])
                year = year[0:4]
                month = str(row[3])
                month = month[:month.find('.')]
                product_code = str(row[4])
                product_code = product_code[:product_code.find('.')]
                cost = str(row[5])
                gift = str(row[6])
                is_training = str(row[7])
                sql = "INSERT IGNORE INTO `EXPENDITURES`(`" + cols + "`) VALUES(" + expenditure_id + "," + household_id + "," + year + "," + month + "," + product_code + "," + cost + "," + gift + "," + is_training + ")"
                query_result = cursor.execute(sql)
                # logger.info("INSERT IGNORE INTO EXPENDITURES query result: ".format(query_result))
            conn.commit()
            logger.info("INSERT IGNORE INTO EXPENDITURES Completed")

            end1 = time.time() - start1
            logger.info("INSERT df_expenditures_csv took : {} seconds".format(end1))

    except:
        # printing stack trace
        traceback.print_exc()


##########################################################################################################
# Load Consumer Expenditures HOUSEHOLD Data Into Business Warehouse
##########################################################################################################
def load_ce_households(df_datasource_table, tbl):

    logger.info('Start Load Session - Consumer Expenditures - Households Table')
    #logger.info("Loading dataframe %s into table %s", df_datasource_table.name, tbl.name)

    try:
        # Connect to local MySQL database
        start1 = time.time()
        #conn = mysql.connector.connect(
        #    host="localhost",
        #    user="root",
        #    password="1234",
        #    database="ConsumerExpenditures10")
        conn = mysql.connector.connect(
            host="localhost",
            user=local_sql_user,
            password=local_sql_password,
            database=dbName)
        end1 = time.time() - start1
        logger.info("Connect to local database : {} seconds".format(end1))

        cursor = conn.cursor()
        start1 = time.time()

        # Insert records from HOUSEHOLDS csv file
        sql = "SELECT COUNT(*) FROM `HOUSEHOLDS`"
        query_result = cursor.execute(sql)
        result = cursor.fetchone()

        if result[0] < 56812:

            start1 = time.time()
            cols = "`,`".join([str(i) for i in df_datasource_table.columns.tolist()])
            for i, row in df_datasource_table.iterrows():
                household_id = str(row[0])
                household_id = household_id[:household_id.find('.')]
                year = str(row[1])
                year = year[:year.find('.')]
                income_rank = str(row[2])
                income_rank1 = str(row[3])
                income_rank2 = str(row[4])
                income_rank3 = str(row[5])
                income_rank4 = str(row[6])
                income_rank5 = str(row[7])
                income_rank_mean = str(row[8])
                age_ref = str(row[9])
                age_ref = age_ref[:age_ref.find('.')]
                sql = "INSERT IGNORE INTO `HOUSEHOLDS`(`" + cols + "`) VALUES(" + household_id + "," + year + "," + income_rank + "," + income_rank1 + "," + income_rank2 + "," + income_rank3 + "," + income_rank4 + "," + income_rank5 + "," + income_rank_mean + "," + age_ref + ")"
                query_result = cursor.execute(sql)
                #logger.info("INSERT IGNORE INTO HOUSEHOLDS query result: ".format(query_result))

            conn.commit()
            end1 = time.time() - start1
            logger.info("INSERT df_households_csv took : {} seconds".format(end1))

    except:
        # printing stack trace
        traceback.print_exc()



##########################################################################################################
# Load Consumer Expenditures - HOUSEHOLD_MEMBERS data Into Business Warehouse
##########################################################################################################
def load_ce_household_members(df_datasource_table, tbl):

    logger.info('Start Load Session - Consumer Expenditures - HOUSEHOLD_MEMBERS Table')
    #logger.info("Loading dataframe %s into table %s", df_datasource_table.name, tbl.name)

    try:
        # Connect to local MySQL database
        start1 = time.time()
        #conn = mysql.connector.connect(
        #    host="localhost",
        #    user="root",
        #    password="1234",
        #    database="ConsumerExpenditures10")
        conn = mysql.connector.connect(
            host="localhost",
            user=local_sql_user,
            password=local_sql_password,
            database=dbName)
        end1 = time.time() - start1
        logger.info("Connect to local database : {} seconds".format(end1))

        cursor = conn.cursor()
        start1 = time.time()

        # Insert records from HOUSEHOLD_MEMBERS csv file
        sql = "SELECT COUNT(*) FROM `HOUSEHOLD_MEMBERS`"
        query_result = cursor.execute(sql)
        result = cursor.fetchone()

        if result[0] < 137355:

            start1 = time.time()
            cols = "`,`".join([str(i) for i in df_datasource_table.columns.tolist()])
            for i, row in df_datasource_table.iterrows():
                household_id = str(row[0])
                household_id = household_id[:household_id.find('.')]
                year = str(row[1])
                year = year[:year.find('.')]
                marital = str(row[2])
                sex = str(row[3])
                age = str(row[4])
                work_status = str(row[5])

                if work_status == 'nan':
                    work_status = '0'

                sql = "INSERT IGNORE INTO `HOUSEHOLD_MEMBERS`(`" + cols + "`) VALUES(" + household_id + "," + year + "," + marital + "," + sex + "," + age + "," + work_status + ")"
                query_result = cursor.execute(sql)
                #logger.info("INSERT IGNORE INTO HOUSEHOLD_MEMBERS query result: ".format(query_result))

            conn.commit()
            end1 = time.time() - start1
            logger.info("INSERT df_household_members_csv took : {} seconds".format(end1))


    except Exception as e:
        logger.error(e)
    # log file/target locations, number of records loaded, constraints on any DB loads, load summary details



##########################################################################################################
# Main Routine - this is where execution of the ETL pipeline application starts
##########################################################################################################
def main():
    # log initialized elements/components like folder location,
    # file location, server id, user id details, process job details

    start = time.time()
    ##extract
    start1 = time.time()
    extract()
    end1 = time.time() - start1
    logger.info("Extract function took : {} seconds".format(end1))


    ##transformation
    start2 = time.time()
    transformation()
    end2 = time.time() - start2
    logger.info("Transformation took : {} seconds".format(end2))

    ##load
    start3 = time.time()
    #load()
    end3 = time.time() - start3
    logger.info("Load took : {} seconds".format(end3))
    end = time.time() - start
    logger.info("ETL Job took : {} seconds".format(end))
    logger.info('Session Summary')
    print("multiple threads took : {} seconds".format(end))

    # display job summary like process run time, memory usage, CPU usage,


if __name__ == "__main__":
    logger.info('ETL Process Initialized')
    main()
