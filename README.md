# databricks_google_trends

Databricks / Delta google trends project

This repository contains a project that demonstrates how to load data using the Pytrends library in Databricks and store it in Delta tables.

![image](https://github.com/kevinbullock89/databricks_google_trends/blob/Add_Readme/Screenshots/Google_DAG.PNG)

## Prerequisites:

To use this project, you will need the following:

- Azure Databricks
- Azure Storage Account (hierachial namespace enabled)
- App registration
- Azure Key Vault

## Installation

To install and configure the required Azure resources for this project, follow these steps:

### Microsft Azure resources

A list of the required Azure resources is available in the prerequisite list. Make sure to create all resources in a resource group. If you don't have an Azure account yet, you can create one here: https://azure.microsoft.com/en-us/.

### Service Principal 

To access the Storage account in Databricks and load the data, we use a Service Principal. Follow these steps to create a Service Principal and store the necessary credentials in the Key Vault.

1. App registration:
   - Create an App Registration.
   - Store the Application (client) ID and Directory (tenant) ID in the Key Vault.
   - Create a new Client Secret and store it in the Key Vault.
   
2. Key Vault

      | Secret                            	| Value                   	|
      |-----------------------------------	|-------------------------	|
      | databricksDataLakeAccessAppId     	| Application (client) ID 	|
      | databricksDataLakeAccessAppSecret 	| Client Secret           	|
      | tenantId                          	| Directory (tenant) ID   	|
   
3. Storace Account
   - Create a container with the name dls.
   - Grant the previously created App Registration read, write, and execute permissions on the dls container.

4. Databricks
   - Create a cluster
   - Create init-script to install all necessary python packages
       ```sh
      sudo /databricks/python/bin/pip install pytrends pandas pycountry
      ```
   - Create a Secret Scope, add DNS name of the Key Vault and Resource ID
   - Create a Access token
   - Open the mounts notebook and run it. https://github.com/kevinbullock89/databricks_google_trends/blob/Add_Readme/config/mount.py
   
   By following these steps, you can easily create a Service Principal and configure the necessary credentials and permissions to access and load the data from the        Storage account in Databricks.
  
   
## Python Scripts

### analytics_load_data_from_api.py

This script uses the pytrend library to load data from Google Trends. With the google_trends() function, you can specify search terms and time frames to load interests by region. To avoid Google's 429 error code, I have included custom headers. More information can be found here:  https://stackoverflow.com/questions/50571317/pytrends-the-request-failed-google-returned-a-response-with-code-429

### analytics_load_country_data.py

This script loads country codes and names using the pycountry library  (https://pypi.org/project/pycountry/)

### analytics_google_trend_rank.py

This script calculates the rank of search terms per time frame using a SQL script. First, a CTE is created to calculate the rank column using the ROW_NUMBER() function. Then, the rank is calculated as a percentage and the results are saved in the correct order.

The output looks like this, e.g. for Austria:

![image](https://github.com/kevinbullock89/databricks_google_trends/blob/Add_Readme/Screenshots/Google_Rank.PNG)

## Databricks

Azure Databricks is a cloud-based big data processing and analytics platform built on top of Apache Spark. It provides a powerful and scalable environment for running data engineering and machine learning workloads. More information here: https://azure.microsoft.com/en-us/products/databricks/

### delta

Delta Lake is a reliable and performant data storage solution built on top of Apache Spark's SQL engine. It offers many advanced features such as ACID transactions, versioning, and schema enforcement that make it easier to manage large-scale data lakes. You can learn more about Delta Lake in https://learn.microsoft.com/en-us/azure/databricks/delta/ and https://docs.delta.io/latest/index.html#

### Databricks Workflow

I use Databricks Workflow to orchestrate my tasks in a scalable and automated way. Workflow enables me to schedule Python scripts at specific times or intervals and monitor their execution using a unified interface. It also allows me to send email notifications and create custom alerts based on specific conditions.


## Usage

To use this repository, simply browse the files and documentation included in the project.

## License

This project is licensed under the MIT License.
   
