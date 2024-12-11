# CS-535_Large-Scale-Data-Analysis-ExtraCredit

# ModeFrequencyUDAF: A Custom User-Defined Aggregate Function

## Overview
This task demonstrates the implementation of a User-Defined Aggregate Function (UDAF) in Scala to calculate the mode and its frequency in a dataset. The project integrates this Scala UDAF with a Python script using PySpark and deploys it as a step on an EMR cluster.

The returned result from the UDAF is a struct containing two values: the mode and its frequency.

---

## Features
- A custom Scala UDAF to calculate mode and its frequency.
- Python script to integrate the Scala UDAF and demonstrate its functionality.
- Deployment on AWS EMR as a Spark job.
- Output validated using EMR step logs in stdout.

---

## Project Structure
```
|-- run.py                                                   # Python script to demonstrate UDAF
|-- build.sbt                                                # SBT configuration file
|-- ModeFrequencyUDAF.scala                                  # Scala implementation of UDAF
|-- target/                                                  # Directory for compiled JAR files
|-- .bloop/                                                  # IDE-specific project metadata
|-- .metals/                                                 # IDE-specific project metadata
|-- project/                                                 # SBT build support files
|-- .vscode/                                                 # Visual Studio Code configuration
```

Note: I placed all of these files in the same folder on my machine.

---

## Requirements
### Software
- Scala 2.12
- SBT 1.5 or higher
- Spark 3.5.3
- Python 3.8 or higher
- AWS EMR (Elastic MapReduce)
- boto3 (Python library for AWS)

### Libraries
- PySpark

---

## Setup and Usage

### 1. Compile the Scala Code
1. Navigate to the project directory.
2. Run the following command to compile the Scala code:
   ```bash
   sbt clean compile
   ```
3. Run the following command to package the Scala UDAF into a JAR file:
   ```bash
   sbt package
   ```
4. The JAR file will be generated in the `target/scala-2.12/` directory.

### 2. Upload Files to S3
1. Upload the compiled JAR file (modefrequencyudaf_2.12-0.1.jar) and the Python script (`run.py`) to an S3 bucket (s3://sharadhakasi/extracredit/).
2. Note the S3 URI paths for use in EMR configurations.

### 3. Python Script Configuration
The Python script demonstrates how to use/glue the Scala UDAF in PySpark.

#### Key Configurations:
- **Spark Session:** Includes the `spark.jars` configuration to load the Scala JAR.
- **UDAF Registration:** The `registerJavaUDAF` method links the Scala UDAF to PySpark.

### 4. Deploy to EMR
1. Configure and start an EMR cluster with Spark installed.
2. Alternatively, use the `extra_emr.py` script to configure and run the job flow programmatically. Use the provided `extra_emr.py` to dynamically configure and launch the EMR job for maximum reusability.

### 5. Validate Results
After the EMR step completes:
- Check the EMR logs in the specified EMR Cluster. (My Cluster ID: j-1BMFYWR4HQQZ7)
- The output will display the Mode and its Frequency.

---

## Example Output
Given the following dataset:
```
+-------+
| value |
+-------+
| a     |
| b     |
| b     |
| c     |
| c     |
| c     |
| d     |
| d     |
| d     |
| d     |
+-------+
```
The output will be:
```
+--------------+
|Mode_Frequency|
+--------------+
| {d, 4}       |
+--------------+
```

---

## Explanation Video
A demonstration video explaining the project, the Scala code, Python script, and the EMR deployment process is available [here](#).

---

## Key Insights
1. **Scala-Python Integration**: The UDAF, implemented in Scala, integrates seamlessly with Python using PySpark.
2. **Scalability**: Running the UDAF on EMR ensures it can handle large datasets efficiently.
3. **Dynamic Configuration**: The script is portable across environments by dynamically specifying paths to the JAR file.

---

## References
- [Spark SQL Aggregate Functions Documentation](https://spark.apache.org/docs/3.5.3/sql-ref-functions-udf-aggregate.html)
- ChatGPT and Claude AI
