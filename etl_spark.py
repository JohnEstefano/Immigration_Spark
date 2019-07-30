from pyspark.sql import SparkSession, SQLContext, GroupedData
from pyspark.sql.functions import *
from state_abbreviations import state_udf, abbreviations_state, abbreviations_state_udf,city_code_udf,city_codes
from immigration_codes import country_udf



#Build spark session
spark = SparkSession.builder.\
config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11")\
.enableHiveSupport().getOrCreate()

#Build SQL context object
sqlContext = SQLContext(spark)

# Read in the data here
i94=spark.read.format('com.github.saurfang.sas.spark').load("../../data/18-83510-I94-Data-2016/i94_jun16_sub.sas7bdat")
demographics=spark.read.format("csv").option("header", "true").option("delimiter", ";").load("us-cities-demographics.csv")
airport_codes=spark.read.format("csv").option("header", "true").load("airport-codes_csv.csv")
temperature=spark.read.format("csv").option("header", "true").load("GlobalLandTemperaturesByState.csv")

## 1 - DEMOGRAPHICS CLEANING
#Calculate percentages of applicable columns and add to new columns.
demographics_data=demographics\
.withColumn("Median Age",col("Median Age").cast("float"))\
.withColumn("percent_male_population",demographics["Male Population"]/demographics["Total Population"]*100)\
.withColumn("percent_female_population",demographics["Female Population"]/demographics["Total Population"]*100)\
.withColumn("percent_veterans",demographics["Number of Veterans"]/demographics["Total Population"]*100)\
.withColumn("percent_foreign_born",demographics["Foreign-born"]/demographics["Total Population"]*100)\
.withColumn("percent_race",demographics["Count"]/demographics["Total Population"]*100)\
.orderBy("State")


#Create new dataframe with state names and newly calculated percentages.
new_demographics_data=demographics_data.select("State",col("State Code").alias("state_code"),\
                                               col("Median Age").alias("median_age"),\
                                               "percent_male_population",\
                                               "percent_female_population",\
                                               "percent_veterans",\
                                               "percent_foreign_born",\
                                               "Race",\
                                               "percent_race")

#pivot the Race column
pivot_demographics_data=new_demographics_data.groupBy("State","state_code","median_age","percent_male_population",\
                                                      "percent_female_population","percent_veterans",\
                                                      "percent_foreign_born").pivot("Race").avg("percent_race")

#remove spaces from the header names.
pivot_demographics_data=pivot_demographics_data.select("State","state_code","median_age","percent_male_population",\
                                                       "percent_female_population","percent_veterans","percent_foreign_born",\
                                                       col("American Indian and Alaska Native").alias("native_american"),\
                                                       col("Asian").alias("asian"),\
                                                       col("Black or African-American").alias("black"),\
                                                       col("Hispanic or Latino").alias("hispanic_or_latino"),\
                                                       col("White").alias("white"))


# calculate average of each column by state.
pivot_state=pivot_demographics_data.groupBy("State","state_code").avg("median_age",\
                                                                      "percent_male_population","percent_female_population",\
                                                                      "percent_veterans","percent_foreign_born","native_american",\
                                                                      "asian","black","hispanic_or_latino","white").orderBy("State")

#Round percentage columns and rename column names
final_demographics_data=pivot_state.select("State","state_code",round(col("avg(median_age)"),1).alias("median_age"),\
                                           round(col("avg(percent_male_population)"),1).alias("percent_male_population"),\
                                           round(col("avg(percent_female_population)"),1).alias("percent_female_population"),\
                                           round(col("avg(percent_veterans)"),1).alias("percent_veterans"),\
                                           round(col("avg(percent_foreign_born)"),1).alias("percent_foreign_born"),\
                                           round(col("avg(native_american)"),1).alias("native_american"),\
                                           round(col("avg(asian)"),1).alias("asian"),\
                                           round(col("avg(hispanic_or_latino)"),1).alias("hispanic_or_latino"),\
                                           round(col("avg(black)"),1).alias("black"),\
                                           round(col('avg(white)'),1).alias('white')
                                          )

print('Processing demographics data...' )
final_demographics_data.show(5)
print('Processed demographics data!' )

## 2 - TEMPERATURE CLEANING

#filter temperature data for U.S. and year 2013 (for latest data)
#add year, month columns.
#add fahrenheit column
#add state abbrevation
#drop duplicates
temperature_data=temperature.filter(temperature["country"]=="United States")\
.filter(year(temperature["dt"])==2013)\
.withColumn("year",year(temperature["dt"]))\
.withColumn("month",month(temperature["dt"]))\
.withColumn("avg_temp_fahrenheit",temperature["AverageTemperature"]*9/5+32)\
.withColumn("state_abbreviations",state_udf(temperature["State"]))

final_temperature_data=temperature_data.select("year","month",round(col("AverageTemperature"),1).alias("avg_temp_celcius"),\
                                       round(col("avg_temp_fahrenheit"),1).alias("avg_temp_fahrenheit"),\
                                       "state_abbreviations","State","Country").dropDuplicates()


print('Processing temperature data...')
final_temperature_data.show(5)
print('Processed temperature data!')


## 3 - AIRPORTS CLEANING

#filter for 'small_airport' in the U.S. and use substring to extract state
airport_data=airport_codes.filter(airport_codes["type"]=="small_airport")\
.filter(airport_codes["iso_country"]=="US")\
.withColumn("iso_region",substring(airport_codes["iso_region"],4,2))\
.withColumn("elevation_ft",col("elevation_ft").cast("float"))

#calculate average elevation by state
airport_elevation=airport_data.groupBy("iso_country","iso_region").avg("elevation_ft")

#select columns and drop duplicates
final_airport_data=airport_elevation.select(col("iso_country").alias("country"),\
                                               col("iso_region").alias("state"),\
                                               round(col("avg(elevation_ft)"),1).alias("avg_elevation_ft")).orderBy("iso_region")
print('Processing airport data...')
final_airport_data.show(5)
print('Processed airport data!')

## 4 - IMMIGRATION DATA CLEANING

# remove nulls from i94addr and i94res columns
# filter for state in list
# filter for city in list
# convert i94res and i94addr columns to country of origin and states using country_udf and abbrev_state_udf
# change i94yr, i94mon to integers type
# convert i94port column to city port name using city_code_udf
i94_data=i94.filter(i94.i94addr.isNotNull())\
.filter(i94.i94res.isNotNull())\
.filter(col("i94addr").isin(list(abbreviations_state.keys())))\
.filter(col("i94port").isin(list(city_codes.keys())))\
.withColumn("origin_country",country_udf(i94["i94res"]))\
.withColumn("dest_state_name",abbreviations_state_udf(i94["i94addr"]))\
.withColumn("i94yr",col("i94yr").cast("integer"))\
.withColumn("i94mon",col("i94mon").cast("integer"))\
.withColumn("city_port_name",city_code_udf(i94["i94port"]))

final_i94_data=i94_data.select("cicid",col("i94yr").alias("year"),col("i94mon").alias("month"),\
                             "origin_country","i94port","city_port_name",col("i94addr").alias("state_code"),"dest_state_name")

print('Processing i94 data...')
final_i94_data.show(5)
print('Processed i94 data!')


## Create Dimension tables
final_temperature_data.createOrReplaceTempView("temperature")
print('Created temperature Table')
final_i94_data.createOrReplaceTempView("immigration")
print('Created immigration Table')
final_demographics_data.createOrReplaceTempView("demographics")
print('Created demographics Table')
final_airport_data.createOrReplaceTempView("airport")
print('Created airport Table')

#allow unlimited time for SQL joins and parquet writes.
sqlContext.setConf("spark.sql.autoBroadcastJoinThreshold", "0")

# This query will build the fact table by joining to the dimension tables above.
# We are counting how many people immigrated to each state in the U.S.
immigration_to_states=spark.sql("""SELECT
                                    m.year,
                                    m.month AS immigration_month,
                                    m.origin_country AS immigration_origin,
                                    m.dest_state_name AS to_immigration_state,
                                    COUNT(m.state_code) AS to_immigration_state_count,
                                    t.avg_temp_fahrenheit,
                                    a.avg_elevation_ft,
                                    d.percent_foreign_born,
                                    d.native_american,
                                    d.asian,
                                    d.hispanic_or_latino,
                                    d.black,
                                    d.white

                                    FROM immigration m JOIN temperature t ON m.state_code=t.state_abbreviations AND m.month=t.month
                                    JOIN demographics d ON d.state_code=t.state_abbreviations
                                    JOIN airport a ON a.state=t.state_abbreviations

                                    GROUP BY m.year,m.month, m.origin_country,\
                                    m.dest_state_name,m.state_code,t.avg_temp_fahrenheit,a.avg_elevation_ft,\
                                    d.percent_foreign_born,d.native_american,\
                                    d.asian,d.hispanic_or_latino,\
                                    d.hispanic_or_latino,d.white,\
                                    d.black

                                    ORDER BY m.origin_country,m.state_code

""")

# Convert Immigration Fact Table to data frame
print('Converting Immigration Fact Table to data frame...')
immigration_to_states.toDF('year', 'immigration_month', 'immigration_origin', 'to_immigration_state', \
          'to_immigration_state_count', 'avg_temp_fahrenheit', 'avg_elevation_ft',\
          'pct_foreign_born', 'native_american', 'asian', 'hispanic_or_latino', 'black', 'white').show(5)
print('Converted Immigration Fact Table to data frame!')

# Write fact table to parquet
print('Writing Immigration Fact Table to Parquet...')
immigration_to_states.write.mode('overwrite').parquet("immigration_to_states")
print('Wrote Immigration Fact Table to Parquet')

print('Running Data Quality Checks...')
# Count the total number rows in Fact Table.
immigration_to_states.select(sum('to_immigration_state_count').alias('fact_table_count')).show()

# check for NULL values
# false must be returned for it to be vaild
immigration_to_states.select(isnull('year').alias('year'),\
                             isnull('immigration_month').alias('month'),\
                             isnull('immigration_origin').alias('country'),\
                             isnull('to_immigration_state').alias('state')).dropDuplicates().show()
