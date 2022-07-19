
## Table of Contents
1. [ Project Description. ](#desc)
2. [About Nigeria HFR](#about)
3. [ Project Development](#dcue)
4. [ References ](#ref)

<a name="desc"></a>
# NIGERIA Health Facility Registry (HFR) Data Warehousing (*in progress*)

This is an end-to-end data engineering project that entails the design and implementation of a data warehouse for the Nigeria Health Facility Registry which is itself a program under the Nigeria Ministry of Health.

This  project development emcompasses:
1. The design of a **data ingestion pipeline architecture** showing the different tools and framework I would be  proposing to the organization in achieving the result.
2. The design of the **ERD(Entity Relationship Diagram)** of the
system and schema of my final data mart that would be used for reporting.
3. ***The implementation of Data Extraction, Transformation and Loading (ETL)**
4. The **Automation and Scheduling** of the processes
 

<a name='about'></a>
# About Nigeria HFR
The Nigeria Health Facility Registry (HFR) was developed in 2017 as part of effort to dynamically manage the Master Health Facility List (MFL) in the country. The MFL "is a complete listing of health facilities in a country (both public and private) and is comprised of a set of identification items for each facility (signature domain) and basic information on the service capacity of each facility (service domain)".

The Federal Ministry of Health had previously identified the need for an information system to manage the MFL in light of different shortcomings encountered in maintaining an up-to-date paper based MFL. The benefits of the HFR are numerous including serving as the hub for connecting different information systems thereby enabling integration and interoperability, eliminating duplication of health facility lists and for planning the establishment of new health facilities.


<a name='dcue'></a>
# Project Development

## Data Ingestion Pipeline Architecture
This design has been made using  https://www.app.diagrams.net/ .

![Dele_HFR_architecture](https://user-images.githubusercontent.com/58152694/179662268-03a5b394-9618-4262-a84e-73795e39f160.png)

The architecture is showing my choice of tools and framework for each processes of the data warehousing project.
* The data source as provided remains the HFR website (https://bit.ly/3lVu5C6) 
* The Data Extraction shall be carried out by utilizing the **Selenium Python web automation framework**. 
* Data Transformation using the Python Big Data manipulation Framework - **PySpark** 
* The workflow management or orchestration tool of choice for the Scheduling and Automation is the **Airflow**
* And as this is a prototype, the **Postgres Database** shall be used for the final data warehouse while the jupyter notebook with python or any other analytical tools would be used to analyse the data for answers

## ERD(Entity Relationship Diagram) Design
*...in progress*



  







<a name="ref"></a>
## References

- [About HFR](https://hfr.health.gov.ng/about-us)
- [Architecture Design](https://www.app.diagrams.net/)
