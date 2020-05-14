

#  DatasetUtils

DatasetUtils is a reference implementation of the Einstein Analytics  External Data API. This tool is free to use, but it is not officially supported by Salesforce.
This is a community project that have not been officially tested or documented. Please do not contact Salesforce for support when using this application.

## Downloading DatasetUtils

Download the latest version from [releases](https://github.com/forcedotcom/Analytics-Cloud-Dataset-Utils/releases) and follow the examples below:

## Running DatasetUtils

## Prerequisite

Download and install Java JDK (not JRE) from Zulu Open JDK

* [Zulu Open JDK](https://www.azul.com/downloads/zulu-community/?&architecture=x86-64-bit&package=jdk)

After installation is complete. Different versions of DatasetUtils require different versions of JDK, the latest release API 48 requires JDK 11. Open a console and check that the java version is correct for your DatasetUtils version  by running the following command:


``java -version``


### Server mode with Web UI


**Windows**: 

Unzip datasetutils.zip to a local folder. To start the jar in server mode: Double click on run.bat

**Mac**: 

Install  datasetutils.dmg by double clicking it and dragging and dropping it into applications folder.

Run datasetutils.app by double clicking it
	 	 

### Console Mode

Best is to run in interactive mode. open a terminal and type in the following command and follow the prompts on the console: 

    java -jar datasetutil-<version>.jar --server false

Or you can pass in all the param in the command line and let it run uninterrupted.
 
    java -jar datasetutil-<version>.jar --action <action> --u <user@domain.com> --dataset <dataset> --app <app> --inputFile <inputFile> --endpoint <endPoint>

Input Parameter

--action  :"load" OR  "downloadxmd"  OR "uploadxmd"  OR "detectEncoding" OR "downloadErrorFile"
 
**load**: for loading csv  

**downloadxmd**: to download existing xmd files  

**uploadxmd**: for uploading user.xmd.json  

**detectEncoding**: To detect the encoding of the inputFile  

**downloadErrorFile**: To downloading the error file for csv upload jobs

--u       : Salesforce.com login

--p       : (Optional) Salesforce.com password,if omitted you will be prompted

--token   : (Optional) Salesforce.com token

--endpoint: (Optional) The Salesforce soap api endpoint (test/prod) Default: https://login.salesforce.com

--dataset : (Optional) the dataset alias. required if action=load

--datasetLabel : (Optional) the dataset label, Defaults to dataset alias.

--app     : (Optional) the app/folder name for the dataset

--operation     : (Optional) the operation for load (Overwrite/Upsert/Append/Delete) Default is Overwrite

--inputFile : (Optional) the input csv file. required if action=load

--rootObject: (Optional) the root SObject for the extract

--rowLimit: (Optional) the number of rows to extract, -1=all, default=1000

--sessionId : (Optional) the Salesforce sessionId. if specified,specify endpoint

--fileEncoding : (Optional) the encoding of the inputFile default UTF-8

--CodingErrorAction:(optional) What to do in case input characters are not UTF8: IGNORE|REPORT|REPLACE. Default REPORT. If you change this option you risk importing garbage characters

--uploadFormat : (Optional) the whether to upload as binary or csv. default binary");

**OR**

--server  : set this to true if you want to run this in server mode and use the UI. **If you give this param all other params will be ignored**

## Usage Example 1: Start the server for using the UI
    java -jar datasetutils-48.1.0jar --server true

## Usage Example 2: Upload a local csv to a dataset in production
    java -jar datasetutils-48.1.0.jar --action load --u pgupta@force.com --p @#@#@# --inputFile Opportunity.csv --dataset puntest
    
## Usage Example 3: Append a local csv to a dataset
	java -jar datasetutils-48.1.0.jar --action load --operation append --u pgupta@force.com --p @#@#@# --inputFile Opportunity.csv --dataset puntest
	
## Usage Example 4: Upload a local csv to a dataset in sandbox
	java -jar datasetutils-48.1.0.jar --action load --u pgupta@force.com --p @#@#@# --inputFile Opportunity.csv --dataset puntest --endpoint https://test.salesforce.com/services/Soap/u/48.0

## Usage Example 5: Download dataset main xmd json file
    java -jar datasetutils-48.1.0.jar --action downloadxmd --u pgupta@force.com --p @#@#@# --dataset puntest

## Usage Example 6: Upload user.xmd.json
    java -jar datasetutils-48.1.0.jar --action uploadxmd --u pgupta@force.com --p @#@#@# --inputFile user.xmd.json --dataset puntest

## Usage Example 7: Detect inputFile encoding
    java -jar datasetutils-48.1.0.jar --action detectEncoding --inputFile Opportunity.csv

## Usage Example 8: download error logs file for csv uploads
    java -jar datasetutils-48.1.0.jar --action downloadErrorFile --u pgupta@force.com --p @#@#@# --dataset puntest

## Building DatasetUtils
    git clone https://github.com/forcedotcom/Analytics-Cloud-Dataset-Utils.git
    mvn clean install
