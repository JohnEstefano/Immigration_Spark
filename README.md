# Immigration_Spark
Udacity Data Engineering Capstone Project

## Step 1: Scope the Project and Gather Data
### Scope

This project will combine data from 4 different sources (detailed below) and create fact table that can be easily queried. It will detail the movement of immigration in the United States.

### Data origin

- World Temperature Data: comes from kaggle. It details temperature changes in the U.S. since 1850.

- U.S. City Demographic Data: comes from OpenSoft. It details city demographics.

- Airport Code Table: comes from datahub.io. It details airport codes it's corresponding cities.

- I94 Immigration Data: comes from the US National Tourism and Trade Office. It includes details on immigration and ports of entry.


## Step 2: Explore and Assess the Data

### Cleaning Steps

#### 1 - DEMOGRAPHICS CLEANING
- Calculate percentages of applicable columns and add to new columns.

- Create new data-frame with state names and newly calculated percentages.

- Pivot the Race column

- Remove spaces from the header names.

- Calculate average of each column by state.

- Round percentage columns and rename column names

#### 2 - TEMPERATURE CLEANING
- Filter temperature data for U.S. and year 2013 (for latest data)

- Add year, month columns.

- Add Fahrenheit column

- Add state abbreviation

- Drop duplicates

#### 3 - AIRPORTS CLEANING
- Filter for 'small_airport' in the U.S. and use substring to extract state

- Calculate average elevation by state

- Drop duplicates

#### 4 - IMMIGRATION DATA CLEANING
- Remove nulls from i94addr and i94res columns

- Filter for state in list

- Filter for city in list

- Convert i94res and i94addr columns to country of origin and states using country_udf and abbrev_state_udf

- Change i94yr, i94mon to integers type

- Convert i94port column to city port name using city_code_udf


## Step 3: Define the Data Model
### Conceptual Data Model: Star Schema

#### Dimension Tables
1. TABLE NAME: airport
- TABLE COLUMNS:country, state, avg_elevation_ft

2. TABLE NAME: demographics
- TABLE COLUMNS: median_age, percent_male_population, percent_female_population, percent_veterans, percent_foreign_born, native_american, asian, hispanic_or_latino, black, white, State, state_code

3. TABLE NAME: immigration
- TABLE COLUMNS: origin_country, i94port, city_port_us, state, destination_state_us, cicid, year, month,

4. TABLE NAME: temperature
- TABLE COLUMNS:average_temperature, avg_temp_fahrenheit, state_abbreviations, State,Country, year, month

#### Fact Table
1. TABLE NAME: immigration_fact_table
- TABLE COLUMNS: immigration_origin, immigration_state, to_immigration_state_count, avg_temp_fahrenheit, avg_elevation_ft, percent_foreign_born, native_american, asian, hispanic_or_latino, black, white, year, immigration_month,

This star schema was put together because of it's simplicity. It will be easy to query for analytical purposes. Fact table was converted back to a spark dataframe and written as parquet file.

### Data Quality Checks
Run Quality Checks
- Checks for null values in year, immigration_month, immigration_origin, to_immigration_state

- Counts data in fact table
