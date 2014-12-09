#  Salesforce.com Analytics Cloud DatasetUtils

Salesforce.com Analytics Cloud DatasetUtils is a reference implementation of the Analytics cloud External data API. 


## Running DatasetUtils
Just download the latest jar from release section (see link at the top) and follow the examples below:

    java -jar datasetutil-<version>.jar --action <action> --u <user@domain.com> --dataset <dataset> --app <app> --inputFile <inputFile> --endpoint <endPoint>

Input Parameter

--action  :"load"  OR "augment"  OR "downloadxmd"  OR "uploadxmd"  OR "detectEncoding". Use load for loading csv, augment for augmenting existing datasets, downloadxmd to download existing xmd files, uploadxmd for uploading user.xmd.json, "extract"  for extracting data from salesforce, "detectEncoding" to detect the encoding of the inputFile.

--u       : Salesforce.com login

--p       : (Optional) Salesforce.com password,if omitted you will be prompted

--token   : (Optional) Salesforce.com token

--endpoint: (Optional) The Salesforce soap api endpoint (test/prod) Default: https://login.salesforce.com/services/Soap/u/31.0

--dataset : (Optional) the dataset alias. required if action=load

--app     : (Optional) the app name for the dataset

--inputFile : (Optional) the input csv file. required if action=load

--rootObject: (Optional) the root SObject for the extract

--rowLimit: (Optional) the number of rows to extract, -1=all, default=1000

--sessionId : (Optional) the Salesforce sessionId. if specified,specify endpoint

--fileEncoding : (Optional) the encoding of the inputFile default UTF-8

--CodingErrorAction:(optional) What to do in case input characters are not UTF8: IGNORE|REPORT|REPLACE. Default REPORT. If you change this option you risk importing garbage characters

--uploadFormat : (Optional) the whether to upload as binary or csv. default binary");



## Usage Example 1: Only Generate the schema file from CSV
    java -jar datasetutils-32.0.0.jar --action load --inputFile Opportunity.csv

## Usage Example 2: Upload a local csv to a dataset
    java -jar datasetutils-32.0.0.jar --action load --u pgupta@force.com --p @#@#@# --inputFile Opportunity.csv --dataset puntest

## Usage Example 3: Download dataset xmd files
    java -jar datasetutils-32.0.0.jar --action downloadxmd --u pgupta@force.com --p @#@#@# --dataset puntest

## Usage Example 4: Upload user.xmd.json
    java -jar datasetutils-32.0.0.jar --action uploadxmd --u pgupta@force.com --p @#@#@# --inputFile user.xmd.json --dataset puntest

## Usage Example 5: Augment datasets
    java -jar datasetutils-32.0.0.jar --action augment --u pgupta@force.com --p @#@#@#

## Usage Example 6: Extract salesforce data
    java -jar datasetutils-32.0.0.jar --action extract --u pgupta@force.com --p @#@#@# --rootObject OpportunityLineItem

## Usage Example 7: Detect inputFie encoding
    java -jar datasetutils-32.0.0.jar --action detectEncoding --inputFile Opportunity.csv


## Building DatasetUtils
    git clone git@github.com:forcedotcom/Analytics-Cloud-Dataset-Utils.git
    mvn clean install
