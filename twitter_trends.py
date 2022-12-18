/**hastags trending on twitter prototype using pyspark*/
import findspark
findspark.init() 
from pyspark.sql import SparkSession
spark = SparkSession.builder.config('spark.port.maxRetries', 100).getOrCreate()
import snscrape.modules.twitter as sntwitter
import pandas as pd
import itertools
df_City= spark.createDataFrame(sntwitter.TwitterSearchScraper(' near:"India" since:2022-12-17_23:00:00_IST until:2022-12-17_23:02:00_IST ').get_items())
hastag=df_City.select(df_City.content)
import re
def hashtagextract(x):
    res=[]
    for i in x:
        if i.startswith('#') == True and i.startswith('#',1,2) == False and i.endswith('#')==False:
           res.append(re.sub("[^a-zA-Z0-9]+", "", i))
    if len(res)>2:
        return res
hashtaglist=[hashtagextract(x.content.split())  for x in hastag.collect() ]
hashtaglist = list(filter(None, hashtaglist))
hashtaglist=list(filter(lambda a: a != '#', hashtaglist))
hashtaglist_iter=[x for x in itertools.chain(*hashtaglist) if len(x)>1]
from pyspark.sql.types import StringType
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import  IntegerType
df1=spark.createDataFrame(hashtaglist_iter,StringType())
from pyspark.sql.functions import sum, col, desc
df1.groupBy("value") .count().sort(desc("count")).limit(5).show()
