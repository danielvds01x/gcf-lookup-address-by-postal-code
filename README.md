# Cloud Function - gcf-lookup-address-by-postal-code

Cloud Function aims to search addresses using the postal codes in Company's data repositories and feed the zip code dimension table.

URL Cloud Function (Console): [https://console.cloud.google.com/functions/details/us-central1/gcf-lookup-address-by-postal-code?authuser=0&project=company-project](https://console.cloud.google.com/functions/details/us-central1/gcf-lookup-address-by-postal-code?authuser=0&project=company-project)

URL Cloud Scheduler (Console): [https://console.cloud.google.com/cloudscheduler?authuser=0&project=company-project](https://console.cloud.google.com/cloudscheduler?authuser=0&project=company-project)

<br/>

## 🔧 Operation:

- The function is triggered by http request
- It expects to receive a JSON via POST, as in the following example:

```
curl --location --request POST 'https://us-central1-company-project.cloudfunctions.net/gcf-lookup-address-by-postal-code' \
--header 'Content-Type: application/json' \
--header 'Authorization: Bearer  $(gcloud auth print-identity-token)' \
--data-raw '{
   "table_bq":{
      "postal_code_field":"postal_code_column",
      "project":"company-project-prd",
      "dataset":"example_dataset",
      "table_name":"example_table"
   }
}'
```

* Where: 
    * "postal_code_field" : The column where the zip codes to be searched are **(it must be one column only per request)**
    * "project" : GCP project
    * "dataset" : Dataset where the table resides
    * "table"  : Source table with postal codes column

### Steps:
- Receiving the request, the function will get the zip codes in the informed table, filtering the zip codes that are already in the final zip code base (<span style="color:green">**dataset.example_table_with_postal_codes**</span>)
- This postal code list, will be searched through Cep Aberto API [(Documentation)](https://cepaberto.com/api_key).
- To increase the acuracy of geolocation information (latitude/longitude), will be made an aditional search (if the postal code)
- To increase the accuracy of the geolocation information (latitude/longitude) an additional search will be performed (if the zip code is found in the aforementioned API) for the address located in the OpenStreetMap.org API (using the [geopy.py](https://github.com/geopy/geopy) lib)
- Finally, with the data located, the address will be inserted into the final zip code table.
- If the zip code is not found, it will be inserted into a base of **invalid zip codes** for data quality analysis purposes:
     - dataset: example_dataset
     - table: table_street_address_invalid_postalcodes
     
### Cloud Scheduler:

Example of deploying a schedule in Cloud Scheduler to the Function:
- Note that in the description of the scheduler we are putting the link to this documentation.

```
#Criar Schedulers
env='prd'
dataset='example_dataset'
for i in \
	table_01,01 \
	table_02,02 \
	table_03,03 \
	table_04,04 \
	table_05,05 \
	table_06,06; do
    IFS=',' read table hour <<< "${i}"
	gcloud scheduler jobs create http $env-schd-gcf-lookup-address-by-postal-code-$dataset-$table \
	  --description "https://bitbucket.org/company_repo/functions-dataprocessing/src/master/gcf-lookup-address-by-postal-code/" \
	  --schedule "0 ${hour} * * *" \
	  --time-zone "America/Sao_Paulo" \
	  --uri "https://us-central1-company-project.cloudfunctions.net/gcf-lookup-address-by-postal-code" \
	  --http-method POST \
	  --headers "Content-Type=application/json" \
	  --message-body '{
	   "table_bq":{
		  "postal_code_field":"postal_code_column",
		  "project":"'"company-project-$env"'",
		  "dataset":"'"$dataset"'",
		  "table_name":"'"${table}"'"
	   }
	}'
done
```  



<br/>
<br/>

___

#### Author: 
Daniel Vieira Dias e Silva : <daniel01@gmx.com> 

<br/>
<br/>

___


Copyright [2022] [Daniel Vieira]

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

