# ATV: Automatic Transaction Verification

## Motivation

* Make Venmo transfer process faster and easier for users. Avoid confirmation step in case transaction is verified.
* Reduce amount of data for fraud analysis. It’s not necessary to double check verified transactions
## Idea
This project automatically verifies Venmo transactions between two users if they have second degree connection

![alt text][2dc]

[2dc]: https://github.com/Samariya57/ATV/blob/master/images/seconddegreeconnection.jpg "Second degree connection schema"

## Data
### Initial
Venmo transactions from 2010 to april 2017 (1TB+) stored in S3 bucket with JSON format. 
### Stored by project
* Venmo users (id, fullname)
* Friends (people with at least 1 transaction between them)
* Transactions
## Pipeline
![alt text][pipeline]

[pipeline]: https://github.com/Samariya57/ATV/blob/master/images/pipeline.jpg "Current pipeline"

* **Spark** - batch process on historical data
* **Kafka** - real time process simulation by kafka-producer.py script and 3 different consumer types (3 topics)
* **MySQL** - storing Users, Friends(people. who have at least one transaction) and Transactions tables
* **Redis** - storing 3 DB, for unverified transaction _from_ and _to_ users and quantity of all and verified real time transactions 
* **Flask** - frontend for users and analysts
## GitHub repo structure  

- `./src/` contains all relevant files for replicating the batch and streaming portions of the project

- `./flask/` contains the relevant files for the Flask web application

- `./sample-data` contains some sample Venmo transaction data
