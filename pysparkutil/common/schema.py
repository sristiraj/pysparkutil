import json
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import boto3
import logging
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark
import os
from pyspark.sql import SparkSession


class InvalidSchemaDefinition(Exception):
    def __init__(self, message="Allowed Schema file of type [.avsc]. Please correct schema"):
        self.message = message
        super().__init__(self.message)

class InvalidSchemaLocation(Exception):
    def __init__(self, message = "Allowed schema location should start with ['file://', 's3://', 'dbfs:/']"):
        self.message = message
        super().__init__(self.message)
        
class Schema(object):
    def __init__(self):
        self.name = __name__
        
    def set_location(self, path):
        self.location = path
        
    def _schema_uri_type(self):
        if self.location.startswith("file://"):
            location_type = "local"
        elif self.location.startswith("s3://"):
            location_type = "s3"
        elif self.location.startswith("dbfs:/"):
            location_type = "dbfs"
        else:
            raise InvalidSchemaLocation()
        return location_type
      
    def _schema_type(self):
        if self.location.endswith(".avsc"):
            self.schema_type = "avro"
        else:
            raise InvalidSchemaDefinition()
        return self.schema_type
      
    def _schema_data(self):
        location_type = self._schema_uri_type()
        if location_type == "local":
            with open(self.location.replace("file://",""), "rb") as f:
                data = f.read()
        elif location_type == "s3":
            s3 = boto3.client("s3")
            s3_bucket_index = self.location.replace("s3://","").find("/")
            s3_bucket = self.location[5:s3_bucket_index+5]
            s3_key = self.location[s3_bucket_index+6:]
            obj = s3.get_object(Bucket=s3_bucket, Key=s3_key)            
            data = obj["Body"].read()
        elif location_type == "dbfs":
            with open(self.location.replace("dbfs:","/dbfs")) as f:
                data = f.read()
        else:
          data = None
        return data    
    
    def parse_schema(self):
        if self._schema_type() == "avro":
          parsed_schema = avro.schema.parse(self._schema_data())
        return parsed_schema  
      
    def switch_type(self,datatype):
        data_type_switcher = {
            'date': 'DateType',
            'timestamp': 'TimestampType',
            'string': 'StringType',
            'double': 'DoubleType',
            'long': 'LongType',
            'float': 'FloatType',
            'integer': 'IntegerType',
            'boolean': 'BooleanType',
            'decimal':'DecimalType',
            'binary':'BinaryType',
            'int': 'IntegerType'
        }
        return data_type_switcher.get(datatype,"Invalid Data Type")

    def create_schema(self,colname,datatype,is_null):
      scale=38
      precision=4
      try:
          if "(" in datatype and "," in datatype:
            datatype_without_ps = datatype[0:datatype.find("(")]
            scale = int(datatype[datatype.find("(")+1:datatype.find(",")])
            precision = int(datatype[datatype.find(",")+1:datatype.find(")")])
          elif "(" in datatype and "," not in datatype:
            datatype_without_ps = datatype[0:datatype.find("(")]
            scale = int(datatype[datatype.find("(")+1:datatype.find(")")])
            precision=None
          else:
            datatype_without_ps = datatype
            scale=None
            precision=None
          str_type=self.switch_type(datatype_without_ps)
          data_type=self.type_for_name(str_type,scale,precision)
          tbl_schema = StructField(colname,data_type,is_null)
      except Exception as e:
        print(datatype,str_type)
        raise e
      return tbl_schema
    
    def type_for_name(self,str_type,scale,precision):
        if scale is None:
          return getattr(pyspark.sql.types,str_type)()
        elif scale is not None and precision is None:
          return getattr(pyspark.sql.types,str_type)(scale)
        else:
          return getattr(pyspark.sql.types,str_type)(scale,precision)
        
    def get_schema(self):
        json_data=self.parse_schema().to_json()
        spark = SparkSession.builder.appName("schema").getOrCreate()
        sc = spark.sparkContext
        df=spark.read.json(sc.parallelize([json_data]))
        rowList=df.select(explode(col("fields"))).collect()
        fieldList=[]
        for i in rowList:
            field_name=i[0].name
            is_null=bool(i[0].nullable)
            dtype=i[0].type
            try:
              if (i[0].logicalType.startswith("timestamp")):
                print(i[0].logicalType)
                dtype="timestamp"
            except:
              pass
            structFields=self.create_schema(field_name,dtype,is_null)
            fieldList.append(structFields)
        schema = StructType(fieldList)
        return schema    

    def get_spark_schema(self):
        json_data=self.parse_schema()
        tmp = "/tmp/avro_schema/tmp.avro"
        spark = SparkSession.builder.appName("schema").getOrCreate()
        with open(tmp, "wb") as fw:
          writer = DataFileWriter(fw, DatumWriter(), schema)
          writer.close()
        df = spark.read.format('avro').load("/tmp/avro_schema/tmp.avro")  
        return df.schema  

if __name__=="__main__":
  sch = Schema()
  sch.set_location("file://C:\\Users\\RAJSR1\\Downloads\\pyspark_util\\tests\\resources\\sample.avsc")
  print(sch.get_schema())
  