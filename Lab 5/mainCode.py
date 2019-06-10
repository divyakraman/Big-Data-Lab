import argparse
import sys
import copy
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql.session import SparkSession
from pyspark.ml.feature import StringIndexer,VectorAssembler,Normalizer,IndexToString,VectorIndexer
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder,TrainValidationSplit
from pyspark.ml import Pipeline,Estimator,Transformer
from pyspark.ml.param.shared import HasInputCol,HasOutputCol
from pyspark.ml.regression import LinearRegression,RandomForestRegressionModel,RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.types import IntegerType ,StringType ,StructField,StructType,FloatType
from pyspark.ml.feature import Imputer, OneHotEncoder

class normalizer(Estimator) : 
	def __init__(self,input_col,output_col) : 
		super(normalizer, self).__init__()
		self.inputCol=input_col
		self.outputCol=output_col
	def _fit(self,df) : 
		statistics=df.describe(self.inputCol)
		self.mean=statistics.select(self.inputCol).rdd.flatMap(list).collect()[1]
		self.std=statistics.select(self.inputCol).rdd.flatMap(list).collect()[2]
		return norm_trans(self.inputCol,self.outputCol,self.mean,self.std)		

class norm_trans(Transformer,HasInputCol,HasOutputCol) : 
	def __init__(self,input_col,output_col,mean,std) : 
		super(norm_trans,self).__init__()
		self.inputCol=input_col
		self.outputCol=output_col
		self.mean=mean
		self.std=std
	def _transform(self,df) : 
		return df.withColumn(self.outputCol,(df[self.inputCol]-self.mean)/self.std)


# Build the command line interface
parser = argparse.ArgumentParser(description="Predict house pricing" )
parser.add_argument("input_file", type=str,help="path to input file")

args = parser.parse_args()
conf = SparkConf().setAppName("Predict House Pricing")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
spark = SparkSession(sc)

# schema to load columns in correct datatype
schema=StructType([StructField("Id", FloatType(), True),
	StructField("MSSubClass", FloatType(), True),
	StructField("MSZoning", StringType(), True),
	StructField("LotFrontage", FloatType(), True),
	StructField("LotArea", FloatType(), True),
	StructField("Street", StringType(), True),
	StructField("Alley", StringType(), True),
	StructField("LotShape", StringType(), True),
	StructField("LandContour", StringType(), True),
	StructField("Utilities", StringType(), True),
	StructField("LotConfig", StringType(), True),
	StructField("LandSlope", StringType(), True),
	StructField("Neighborhood", StringType(), True),
	StructField("Condition1", StringType(), True),
	StructField("Condition2", StringType(), True),
	StructField("BldgType", StringType(), True),
	StructField("HouseStyle", StringType(), True),
	StructField("OverallQual", FloatType(), True),
	StructField("OverallCond", FloatType(), True),
	StructField("YearBuilt", FloatType(), True),
	StructField("YearRemodAdd", FloatType(), True),
	StructField("RoofStyle", StringType(), True),
	StructField("RoofMatl", StringType(), True),
	StructField("Exterior1st", StringType(), True),
	StructField("Exterior2nd", StringType(), True),
	StructField("MasVnrType", StringType(), True),
	StructField("MasVnrArea", StringType(), True),
	StructField("ExterQual", StringType(), True),
	StructField("ExterCond", StringType(), True),
	StructField("Foundation", StringType(), True),
	StructField("BsmtQual", StringType(), True),
	StructField("BsmtCond", StringType(), True),
	StructField("BsmtExposure", StringType(), True),
	StructField("BsmtFinType1", StringType(), True),
	StructField("BsmtFinSF1", FloatType(), True),
	StructField("BsmtFinType2", StringType(), True),
	StructField("BsmtFinSF2", FloatType(), True),
	StructField("BsmtUnfSF", FloatType(), True),
	StructField("TotalBsmtSF", FloatType(), True),
	StructField("Heating", StringType(), True),
	StructField("HeatingQC", StringType(), True),
	StructField("CentralAir", StringType(), True),
	StructField("Electrical", StringType(), True),
	StructField("1stFlrSF", FloatType(), True),
	StructField("2ndFlrSF", FloatType(), True),
	StructField("LowQualFinSF", FloatType(), True),
	StructField("GrLivArea", FloatType(), True),
	StructField("BsmtFullBath", FloatType(), True),
	StructField("BsmtHalfBath", FloatType(), True),
	StructField("FullBath", FloatType(), True),
	StructField("HalfBath", FloatType(), True),
	StructField("BedroomAbvGr", FloatType(), True),
	StructField("KitchenAbvGr", FloatType(), True),
	StructField("KitchenQual", StringType(), True),
	StructField("TotRmsAbvGrd", FloatType(), True),
	StructField("Functional", StringType(), True),
	StructField("Fireplaces", FloatType(), True),
	StructField("FireplaceQu", StringType(), True),
	StructField("GarageType", StringType(), True),
	StructField("GarageYrBlt", FloatType(), True),
	StructField("GarageFinish", StringType(), True),
	StructField("GarageCars", FloatType(), True),
	StructField("GarageArea", FloatType(), True),
	StructField("GarageQual", StringType(), True),
	StructField("GarageCond", StringType(), True),
	StructField("PavedDrive", StringType(), True),
	StructField("WoodDeckSF", FloatType(), True),
	StructField("OpenPorchSF", FloatType(), True),
	StructField("EnclosedPorch", FloatType(), True),
	StructField("3SsnPorch", FloatType(), True),
	StructField("ScreenPorch", FloatType(), True),
	StructField("PoolArea", FloatType(), True),
	StructField("PoolQC", StringType(), True),
	StructField("Fence", StringType(), True),
	StructField("MiscFeature", StringType(), True),
	StructField("MiscVal", FloatType(), True),
	StructField("MoSold", FloatType(), True),
	StructField("YrSold", FloatType(), True),
	StructField("SaleType", StringType(), True),
	StructField("SaleCondition", StringType(), True),
	StructField("SalePrice", FloatType(), True)])

print '\n\n'
# integer columns
featuresList = ["MSSubClass","LotArea","OverallQual","OverallCond","YearBuilt","YearRemodAdd","BsmtFinSF1","BsmtFinSF2","BsmtUnfSF","TotalBsmtSF","1stFlrSF","2ndFlrSF","LowQualFinSF","GrLivArea","BsmtFullBath","BsmtHalfBath","FullBath","HalfBath","BedroomAbvGr","KitchenAbvGr","TotRmsAbvGrd","Fireplaces","GarageCars","GarageArea","WoodDeckSF","OpenPorchSF","EnclosedPorch","3SsnPorch","ScreenPorch","PoolArea","MoSold","YrSold"]
# all columns
featuresFullList=["MSSubClass","MSZoning","LotFrontage","LotArea","Street","Alley","LotShape","LandContour","Utilities","LotConfig","LandSlope","Neighborhood","Condition1","Condition2","BldgType","HouseStyle","OverallQual","OverallCond","YearBuilt","YearRemodAdd","RoofStyle","RoofMatl","Exterior1st","Exterior2nd","MasVnrType","MasVnrArea","ExterQual","ExterCond","Foundation","BsmtQual","BsmtCond","BsmtExposure","BsmtFinType1","BsmtFinSF1","BsmtFinType2","BsmtFinSF2","BsmtUnfSF","TotalBsmtSF","Heating","HeatingQC","CentralAir","Electrical","1stFlrSF","2ndFlrSF","LowQualFinSF","GrLivArea","BsmtFullBath","BsmtHalfBath","FullBath","HalfBath","BedroomAbvGr","KitchenAbvGr","KitchenQual","TotRmsAbvGrd","Functional","Fireplaces","FireplaceQu","GarageType","GarageYrBlt","GarageFinish","GarageCars","GarageArea","GarageQual","GarageCond","PavedDrive","WoodDeckSF","OpenPorchSF","EnclosedPorch","3SsnPorch","ScreenPorch","PoolArea","PoolQC","Fence","MiscFeature","MiscVal","MoSold","YrSold","SaleType"]
# categorical columns
featuresStrList=[x for x in featuresFullList if x not in featuresList]
# label
label = "SalePrice"

# Load data
training = spark.read.format("csv").option("header",True).option("nullValue", "NA").schema(schema).load(args.input_file)#.toDF("Id","MSSubClass")

indexers=[StringIndexer(inputCol=x,outputCol=x+"_index",handleInvalid="keep") for x in featuresStrList]
pipeline=Pipeline(stages=indexers)
training=pipeline.fit(training).transform(training)
train,test = training.randomSplit([0.8, 0.2])

featuresListFinal1=featuresList+[x+"_index" for x in featuresStrList]

# normalizing
normalizers=[normalizer(input_col=x,output_col=x+"_norm") for x in featuresListFinal1]
# pipeline=Pipeline(stages=normalizers)
# mod=pipeline.fit(train)
# train=mod.transform(train)
# test=mod.transform(test)
# print 'Normalizing done.'
featuresListFinal=[x+"_norm" for x in featuresListFinal1]

assembler = VectorAssembler(inputCols=featuresListFinal,outputCol="features")
imputer=Imputer(inputCols=featuresListFinal,
	outputCols=featuresListFinal).setStrategy("mean")
lr = LinearRegression(featuresCol = 'features', labelCol=label, maxIter=10, regParam=0.3, elasticNetParam=0.8)
print('Steps: Normalisation, Imputation and Linear Regression')
stages=normalizers
stages.append(assembler)
stages.append(imputer)
stages.append(lr)
pipeline = Pipeline(stages=stages)
mod=pipeline.fit(train)
prediction=mod.transform(test)
print('Results after regression : \n')

eval = RegressionEvaluator(labelCol=label, predictionCol="prediction")
selected = prediction.select(label, "prediction")
print 'RMSE:',eval.evaluate(selected, {eval.metricName: "rmse"})
print 'R-squared:',eval.evaluate(selected, {eval.metricName: "r2"})

# ./spark-submit Desktop/IITM/SEMESTERS/SEMESTER_8/BigDataLaboratory/Lab5/mainCode.py Desktop/IITM/SEMESTERS/SEMESTER_8/BigDataLaboratory/Lab5/house_prices.csv Desktop/IITM/SEMESTERS/SEMESTER_8/BigDataLaboratory/Lab5/output_folder