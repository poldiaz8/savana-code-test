--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Project: Savana Code Test | Author: Pablo Díaz García | Date: 26/01/2022

General explanation:
This script is divided in 4 parts in order to keep the code clean.
-1º -> Definitions, where the functions I use later will be allocated.
-2º -> Load data input.
-3º -> Standardize data and transform it into the desired output.
-4º -> Export the output to a csv file.

Desing Decisions:
-RDD O SPARKSQL: I decided to use SPARKSLQ DataFrames in order to reduce code from rdds. Functions like withColumn and join mean a less heavy code.

-User-Defined Functions (UDF): We use them to define new Column-based functions that extend the vocabulary of Spark SQL’s for transforming Datasets.
This is suitable when working with SPARSQL DataFrames but not really when working with rdds because it makes the code heavier. That is why I decided
to define functions as not udf to not duplicate code in tests but later convert them to udf for the script.

-Data assumptions:
date -> I choose the date output format would be yyyy-MM-dd
gender -> I considered a wrong capital letter and also the last missing letter.
quotes -> I considered this 3 quotes "|”|“
I could have applied this functions to every suitable type of data for example: applying the quotes function to  "original_patient_id" column which is not needed in this case.
But I chose to not do it in order to keep code cleaner.

Test: IntelliJ environment with JUnit and ScalaTest
Load the files as structured and run execute in the test file.

Folder Structure:
data -> where csv's are allocated.
src -> Main -> ClinicalService (Main Script)
src -> test -> ClinicalServiceTest (Tests)
data -> datacsv -> file.csv (Output)
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------