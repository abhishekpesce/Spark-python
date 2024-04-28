from pyspark.sql import SparkSession

spark = SparkSession.builder\
    .master("local")\
        .appName("test")\
            .getOrCreate()

data = [
    ('James','','Smith','1919-04-01','M',3000),
    ('Michael','Ross','','1925-10-19','M',3000),
    ('Robert','','William','1999-04-25','M',3000),
    ('Maria','Anne','Jones','1967-05-05','F',3000),
    ('Jen','','Mary','2000-04-01','F',3000),
    
]

columns = ['FirstName','MiddleName','LastName','DOB','SEX','Salary']

df =spark.createDataFrame(data = data,schema = columns)

print(df)