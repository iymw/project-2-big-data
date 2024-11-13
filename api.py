import pandas as pd

data = pd.read_csv('IOT-temp.csv', index_col="id")
data.head()
print(len(data))

print(data.isnull().sum())

data_1 = data.loc["__export__.temp_log_196134_bd201015":"__export__.temp_log_60240_9177689a"] 
print(len(data_1))
data_1.to_csv('batch_1.csv')

data_2 = data.loc["__export__.temp_log_196134_bd201015":"__export__.temp_log_12544_1a76a951"] 
print(len(data_2))
data_2.to_csv('batch_2.csv')

data_3 = data.loc["__export__.temp_log_148002_0a1a4e64":"__export__.temp_log_133741_32958703"] 
print(len(data_3))
data_3.to_csv('batch_3.csv')

for i in [{"title": "In", "boolean": "In"}, {"title": "Out", "boolean": "Out"}]:
    print(f"{i['title']}: {(data_3['out/in'] == i['boolean']).sum()}")

from pyspark.sql import SparkSession
from pyspark.sql.functions import when
from pyspark.sql.functions import unix_timestamp
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import col
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator

spark = SparkSession.builder.appName("RandomForestClassifier").getOrCreate()

# Load data
data = spark.read.csv("IOT-temp.csv")
# data = spark.read.csv("batch_2.csv")
# data = spark.read.csv("batch_3.csv")
data = data.filter(data._c0 != 'id')
data.limit(5).show()
data = data.withColumn("_c5", when(data["_c4"] == "Out", 1).otherwise(0))
data = data.drop("_c0", "_c1", "_c4")
data.limit(5).show()
data = data.withColumn("_c2", unix_timestamp("_c2", "dd-MM-yyyy HH:mm"))
data.limit(5).show()
# Convert _c3 to numeric type (e.g., IntegerType or DoubleType)
data = data.withColumn("_c3", col("_c3").cast("double"))

assembler = VectorAssembler(inputCols=["_c3", "_c2"], outputCol="features")
data = assembler.transform(data).select("features", "_c5")

data.limit(5).show()
train_data, test_data = data.randomSplit([0.7, 0.3], seed=42)

from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

# Initialize the RandomForestClassifier with the best hyperparameters
rf = RandomForestClassifier(labelCol="_c5", featuresCol="features", 
                            numTrees=90, maxDepth=19, maxBins=128)

# You can still use the ParamGridBuilder if you want to explore other parameters or use a grid search
paramGrid = ParamGridBuilder().addGrid(rf.numTrees, [90]).addGrid(rf.maxDepth, [19]).addGrid(rf.maxBins, [128]).build()
# Define the evaluator
evaluator = BinaryClassificationEvaluator(labelCol="_c5", metricName="areaUnderROC")

# Set up CrossValidator with the updated paramGrid
crossval = CrossValidator(estimator=rf,
                          estimatorParamMaps=paramGrid,
                          evaluator=evaluator,
                          numFolds=3)

# Run cross-validation and choose the best set of parameters
cvModel = crossval.fit(train_data)
predictions = cvModel.transform(test_data)
evaluator = BinaryClassificationEvaluator(labelCol="_c5", rawPredictionCol="prediction", metricName="areaUnderROC")
roc_auc = evaluator.evaluate(predictions)

print(f"Test Area Under ROC: {roc_auc}")
predictions.select("features", "_c5", "prediction").show(5)
for i in [{"title": "In", "boolean": 0}, {"title": "Out", "boolean": 1}]:
    print(f"{i['title']}: {predictions.filter(predictions['prediction'] == i['boolean']).count()}")
predictions = predictions.withColumn("prediction", when(predictions["prediction"] == 1, "Out").otherwise("In"))
predictions.select("features", "_c5", "prediction").show(5)
# Convert to Pandas DataFrame
pandas_df = predictions.select("features", "_c5", "prediction").toPandas()

pandas_df.to_csv("result_from_train.csv", index=False)

import json
from flask import Flask, jsonify, request
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, unix_timestamp, col
from pyspark.ml.feature import VectorAssembler
import os

app = Flask(__name__)

UPLOAD_FOLDER = os.getcwd()  # Current directory
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# Initialize Spark session
spark = SparkSession.builder.appName('FileProcessingApp').getOrCreate()

@app.route('/api_1', methods=['POST'])
# @app.route('/api_2', methods=['POST'])
# @app.route('/api_3', methods=['POST'])
def create():
    try:
        # Access the uploaded file and get its filename
        file = request.files['file']
        file_name = file.filename

        # Extract the pure file name (without any path)
        pure_file_name = os.path.basename(file_name)

        # Define the path where the file will be saved (current folder)
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], pure_file_name)

        print(pure_file_name)
        
        # Save the file to the current folder
        file.save(file_path)

        # Read the CSV file into a Spark DataFrame
        spark_df = spark.read.csv(pure_file_name)
        spark_df = spark_df.filter(spark_df["_c0"] != 'id')
        spark_df = spark_df.drop("_c0", "_c1")
        spark_df = spark_df.withColumn("_c2", unix_timestamp("_c2", "dd-MM-yyyy HH:mm"))
        spark_df = spark_df.withColumn("_c3", col("_c3").cast("double"))

        # Assemble features
        assembler = VectorAssembler(inputCols=["_c3", "_c2"], outputCol="features")
        spark_df = assembler.transform(spark_df).select("features")

        predictions = cvModel.transform(spark_df)

        # Export predictions to pandas DataFrame
        export = predictions.drop("rawPrediction", "probability").withColumn("prediction", when(predictions["prediction"] == 1, "Out").otherwise("In")).toPandas()

        # Save the results to a CSV file
        export.to_csv("result_from_api.csv", index=False)
        
        return jsonify({"message": "File successfully saved as result_from_api.csv"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
   app.run(port=5000)