
[//]: # (I must include tools section and add tools images, check proper README design tutorial)
[//]: # (images and style from https://simpleicons.org/)
![Python](https://img.shields.io/badge/Python-3.8-blueviolet?style=for-the-badge&logo=python)
![PySpark](https://img.shields.io/badge/Apache_Spark-3.3.0-D22128?style=for-the-badge&logo=apachespark)
![Pandas](https://img.shields.io/badge/pandas-1.4.3-150458?style=for-the-badge&logo=pandas)
![Selenium](https://img.shields.io/badge/Selenium-3.141.0-43B02A?style=for-the-badge&logo=selenium)
![Airflow](https://img.shields.io/badge/Apache_Airflow-2.3.3-017CEE?style=for-the-badge&logo=apacheairflow)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-2B2B2B?style=for-the-badge&logo=postgreSql)
![Jupyter](https://img.shields.io/badge/jupyter-2B2B2B?style=for-the-badge&logo=jupyter)

## Table of Contents
1. [Project Description](#desc)
2. [About Nigeria HFR](#about)
3. [ Project Development](#dcue)
4. [ Environment Setup ](#setup)
5. [ Run Programs ](#installation)
6. [ References ](#ref)


<a name="desc"></a>
# NIGERIA Health Facility Registry (HFR) Data Warehousing (*in progress*)

This is an end-to-end data engineering project that entails the design and implementation of a data warehouse for the Nigeria Health Facility Registry which is itself a program under the Nigeria Ministry of Health.
The goal of this project is to develop a complete reporting solution to help the Ministry monitor and manage the contry's [Master Health Facility List (MFL)](#MFL).

This  project development emcompasses:
1. The design of a **data ingestion pipeline architecture** showing the different tools and framework I would be  proposing to the organization in achieving the result.
2. The design of the **ERD(Entity Relationship Diagram)** of the
system and schema of my final data mart that would be used for reporting.
3. **The implementation of Data Extraction, Transformation and Loading (ETL)**
4. The **Automation and Scheduling** of the processes
 

<a name='about'></a>
# About Nigeria HFR
The Nigeria Health Facility Registry (HFR) was developed in 2017 as part of effort to dynamically manage the MFL in the country. <span id="MFL"><b>The MFL "is a complete listing of health facilities in a country (both public and private) and is comprised of a set of identification items for each facility (signature domain) and basic information on the service capacity of each facility (service domain)"</b></span>.

The Federal Ministry of Health had previously identified the need for an information system to manage the MFL in light of different shortcomings encountered in maintaining an up-to-date paper based MFL. The benefits of the HFR are numerous including serving as the hub for connecting different information systems thereby enabling integration and interoperability, eliminating duplication of health facility lists and for planning the establishment of new health facilities.


<a name='dcue'></a>
# Project Development

## Data Ingestion Pipeline Architecture
This design has been made using  https://www.app.diagrams.net/ .

![Dele_HFR_architecture](https://user-images.githubusercontent.com/58152694/179662268-03a5b394-9618-4262-a84e-73795e39f160.png)

The architecture is showing my choice of tools and framework for each processes of the data warehousing project.
* The data source as provided remains the [HFR website](https://bit.ly/3lVu5C6) 
* The Data Extraction shall be carried out by utilizing the **Selenium Python web automation framework**. 
* Data Transformation using the Python Big Data manipulation Framework - **PySpark** 
* The workflow management or orchestration tool of choice for the Scheduling and Automation is the **Airflow**
* And as this is a prototype, the **Postgres Database** shall be used for the final data warehouse while the jupyter notebook with python or any other analytical tools would be used to analyse the data for answers

## ERD(Entity Relationship Diagram) Design
The ERD  of the system as shown below can also be accessed [here](https://lucid.app/lucidchart/3b297f6f-6dd4-40b4-805c-263f42043573/edit?viewport_loc=50%2C288%2C2560%2C1052%2C0_0&invitationId=inv_c95a73c5-49bf-464c-845e-37e7e9b6ba7e#)

![Edited DBMS ER diagram (UML notation)](https://user-images.githubusercontent.com/58152694/183575858-3943cafc-f0db-4bc8-b1ee-bba3d6ce8808.png)

From considering the HFR requirements and studying the value types and forms of data available on the HFR website:
* I have designed a **Star schema** as my final data mart showing six (6) dimension-tables.Having performed 3 levels of normalization (1NF, 2NF, 3NF) where applicable.
* The model has also been designed to provide information for all possible grains. i.e the fact table rows  provide a high level of details.
* This stage I would say has been the most tasking.
  
# Data Extraction, Transformation and Loading (ETL) Implementaion
* The python frameworks and packages leveraged for the web scraping are **Selenium**, **Pandas**, **Numpy** and **BeautifulSoup**.
* To speed up the webscraping process, **multithreading** has been employed
* Output of the scraper is saved as `raw_hfr_data.csv` which is then fed unto the etl processes
* **PySpark** has been largely utilized for the whole **ETL** process
* A **doctors.parquet** file was written (i.e data of health institution that has at least one doctor)
* **Data Loading** into **PostgreSQL** has been performed as proposed in the ERD and schema design 

# Automation and Scheduling
_...in progress_


<a name="setup"></a>
## Environment Setup
To run this system, you must have python installed on your computer.
The dependencies and third party libraries of this program are written in the `requirements.txt` file available in the source folder and the third party libraries include:
* `pandas==1.4.3`
* `numpy==1.20.1`
* `selenium==3.141.0`
* `beautifulsoup4==4.9.3`
* `pyspark==3.3.0`

Other dependencies include:
* `PostgreSQL` 
* `Spark 3.3.0 Pre-built for Apache Hadoop2.7`
* `Chromedriver` _(compatible with your system Chrome browser)_

which is needed to be downloaded, installed and added to System Environment variables on your local machine.

> The program files initially included the `scraper.py` module that does the data scraping, `warehouse_config.cfg` (configuration file for the Database credentials)
> and, the `etl.py` module where the whole ETL implementation happened. All these are contained the `source` folder.


Follow the following instructions and links to set up the non-python dependencies:
* [Setting up a local PostgreSQL database](https://www.prisma.io/dataguide/postgresql/setting-up-a-local-postgresql-database)
* [How to set up Apache Spark on local machine](https://www.geeksforgeeks.org/install-apache-spark-in-a-standalone-mode-on-windows/) 
NB: You will need to install Java and not Scala
* After setting up Apache Spark, You will also need to download PostgreSQL jdbc driver [postgresql-42.2.26.jar](https://jdbc.postgresql.org/download/postgresql-42.2.26.jar) 
and ensure to put into the `jars` folder that is inside the Apache Spark extracted folder.   
This process allows to use PostgreSQl with Apache Spark/pyspark
* Download [chromedriver](https://chromedriver.chromium.org/downloads) and add to the `source` folder of this project

To install the 3rd party Python packages/dependencies once, follow the following procedures:

1. On the cmd/terminal, enter the following command to navigate to this project folder:
```bash
$/> cd <project_directory>
```

2. create and activate a virtual environment - <virtualenv_name> could be any name - **_(optional)_**:

OS X & Linux:
```bash
$ sudo apt-get install python-pip
$ pip3 install virtualenv
$ virtualenv -p /usr/bin/python3 <virtualenv_name>
$ <virtualenv_name>/bin/activate
```

Windows:
```bash
> pip install virtualenv 
> virtualenv <virtualenv_name>
> <virtualenv_name>\Scripts\activate
```

3. Then, install dependencies

OS X & Linux:
```bash
$ pip3 install -r requirements.txt
```
Windows:
```bash
> pip install -r requirements.txt
```

<a name="installation"></a>
## Run Programs
Ensure the Database credentials stored in the `warehouse_config.cfg` are replaced with your Database credentials.
After this, you can do the following to run the programs:

1. Open terminal and ensure the working directory is set to the `source` folder of this project 
(if you created a virtual environment before installing the `requirements.txt` file, ensure to 
activate the environment).\

Then:

### Running the Scraper
2. The scraper module should be run first using the command below.

OS X & Linux:
```bash
$ python3 scraper.py
```
Windows:
```bash
> python scraper.py
```
This process will output a csv file that the `etl.py` module will work on. 

### Running the ETL
2. The etl module should be run after the scraper module is executed.

OS X & Linux:
```bash
$ python3 etl.py
```
Windows:
```bash
> python etl.py
```
This process will output a parquet file, create tables and load data into the tables of the specified database 

### Running Test.
_...in progress_


<a name="ref"></a>
## References

- [About HFR](https://hfr.health.gov.ng/about-us)
- [Architecture Design](https://www.app.diagrams.net/)
- [prisma](https://www.prisma.io/dataguide/postgresql/setting-up-a-local-postgresql-database)
- [Geeksforgeeks](https://www.geeksforgeeks.org/install-apache-spark-in-a-standalone-mode-on-windows/) 
