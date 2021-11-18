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
import io


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
        
    def _schema_uri_type(self, path):
        if path.startswith("file://"):
            location_type = "local"
        elif path.startswith("s3://"):
            location_type = "s3"
        elif path.startswith("dbfs:/"):
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
        location_type = self._schema_uri_type(self.location)
        if location_type == "local":
            try:
                with open(self.location.replace("file://",""), "rb") as f:
                    data = f.read()
            except Exception as e:
                print("Error reading schema file from {}: ".format(self.location)+str(e))
                raise e
        elif location_type == "s3":
            try:
                s3 = boto3.client("s3")
                s3_bucket_index = self.location.replace("s3://","").find("/")
                s3_bucket = self.location[5:s3_bucket_index+5]
                s3_key = self.location[s3_bucket_index+6:]
                obj = s3.get_object(Bucket=s3_bucket, Key=s3_key)            
                data = obj["Body"].read().decode("utf-8")
            except Exception as e:
                print("Error reading schema file from s3 {}: ".format(self.location)+str(e))    
                raise e
        elif location_type == "dbfs":
            try:
                with open(self.location.replace("dbfs:","/dbfs")) as f:
                    data = f.read()
            except Exception as e:
                print("Error reading schema file from {}: ".format(self.location)+str(e))
                raise e
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
      
    def _tmp_schema(self, data):
        cwd = os.getcwd()
        sep = os.sep
        tmp_file = "tmp.avro"
        tmppath = self.location[:self.location.rfind("/")]+"_tmp"+self.location[self.location.rfind("/"):]
        pathtype = self._schema_uri_type(tmppath)
        return_path = ""
        if pathtype=="local":
            with open(tmppath.replace("file://",""), "wb+") as fw:
                writer = DataFileWriter(fw, DatumWriter(), data)
                writer.close()
            return_path = tmppath.replace("file://","")   
        elif pathtype=="s3": 
            s3 = boto3.client("s3")
            s3_bucket_index = tmppath.replace("s3://","").find("/")
            s3_bucket = tmppath[5:s3_bucket_index+5]
            s3_key = tmppath[s3_bucket_index+6:]
            print(f"s3 bucket for tmp path {s3_bucket}")
            print(f"s3 key for tmp path {s3_key}")
            with open(cwd+sep+tmp_file, "wb+") as fw:
                writer = DataFileWriter(fw, DatumWriter(), data)
                writer.close()
            s3.upload_file(cwd+sep+tmp_file, s3_bucket, s3_key.replace(".avsc",".avro"))
            return_path = "s3://"+s3_bucket+"/"+s3_key.replace(".avsc",".avro")
        elif pathtype=="dbfs": 
            with open(tmppath.replace("dbfs:/","/dbfs/"), "wb+") as fw:
                writer = DataFileWriter(fw, DatumWriter(), data)
                writer.close()
            return_path = tmppath.replace("dbfs:/","/dbfs/")
        return return_path        
            
    def get_unstructured_schema(self):
        json_data=self.parse_schema()
        spark = SparkSession.builder.appName("schema").getOrCreate()
        sc = spark.sparkContext
        tmppath = self._tmp_schema(json_data)  
        df = spark.read.format('avro').load(tmppath)  
        return df.schema  

if __name__=="__main__":
  sch = Schema()
  sch.set_location("dbfs:/FileStore/tables/sample_json-2.avsc")
  print(sch.get_unstructured_schema())
  