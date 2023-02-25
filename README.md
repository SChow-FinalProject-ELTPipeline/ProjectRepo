# Building and ELT Pipeline to House Consumer Expenditures & Economic Indicators Data
This is a final project for ADS507: Fundamentals of Data Engineering
Connie (Sau) Chow (Individual Project)

## Project Description
The purpose of this project is to present to code a basic ELT pipeline implementation using Python & SQL.  The purpose of this pipeline is to extract Consumer Expenditures data as well as relevant economic indicators data (ie GDP and CPI) and preprocess/clean it and then house it in a "Business Warehouse" where it is ready to be used and consumed by end users such as data scientists, data analysts, executives who need dashboards to make business decisions, in our case maybe an economists or researcher.  This application uses various python libraries including SQLAlchemy and Pandas.  The 'Staging Area' and 'Business Warehouse' utilizes a MySQL database locally installed on a personal laptop. The following are descriptions of each file in this repository:


## Design Document & Presentation
* [Design Document ](https://docs.google.com/document/d/1fSQvwDQKXq48kGfZs4CZgP_DDEtUIvGiAxJxwymWQPA/edit?usp=sharing)
* [Presentation slides ](https://docs.google.com/presentation/d/1nSNaFQdMmkmSau_M2z2EZJFIgPv_qZwLtgnsIeeTwh0/edit?usp=sharing)<br>
If links not accessible, both files are also available on this GitHub page under "Prensentation" and "Design Document" folders.


## Oveview of Repository Content
1. ConnieChow-elt-pipeline-Feb24.py<br>
This is the ELT pipeline application.  Just follow the setup requirements below and run the file.

2. USCPI.csv<br>
This is the CPI data taken from Kaggle.  The application directly accesses this CSV file from this GitHub page.

3. gdp.csv<br>
This is the GDP data taken from Kaggle.  The application directly accesses this CSV file from this GitHub page.

4. README<br>
You are reading the README.  It contains all the information you need to setup and run this pipeline.  I recommend you look at the Design Document so that you understand what is going on in the code.


## Setup Requirements
1. Clone this repo using raw data.
2. Download and Install the most current version of Python
3. Download and Install MySQL database
4. Download and Install PyCharm (this is recommended but you can also run python from the command line)
5. Ensure that your environment path and variables are set for Python
6. Ensure that all python libraries listed above are installed in your IDE of choice or in PyCharm


## How To Deploy This Pipeline
1. Download from GitHub and then open up this file "ConnieChow-elt-pipeline-Feb24.py" in your IDE
2. At line 52 and line 53, enter your username and password to your local MySQL database INSIDE of the quotes.
3. At line 49 and line 50, ensure that the database name is not already taken in your MySQL database.  If that name already exists, then change the name to something that does not exist already in your database.
3. Run the file "ConnieChow-elt-pipeline.py"


## How to Monitor This Pipeline
1. The log messages are written to the etl-pipeline.log file.  This file will be available in the same directory that you run your python program from.
2. The log messages are also printed to the console at runtime


## How to Access The Data Warehouse Output Of This Pipeline
1. Connect or access your local MySQL server and the database that was created with this pipeline
2. You can access the repository through the "business_warehouse" SQL View



### Technologies
* Python (current version)
* PyCharm IDE (current version)
* MySQL (current version)


## Datasets
* [Consumer Expenditures Relational Database ](https://relational.fit.cvut.cz/dataset/ConsumerExpenditures)
* [CPI ](https://www.kaggle.com/datasets/varpit94/us-inflation-data-updated-till-may-2021)
* [GDP ](https://www.kaggle.com/datasets/federalreserve/interest-rates?resource=download)


## Required Python Packages
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
