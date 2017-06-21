# ATV: Automatic Transaction Verification

## Motivation

* Make Venmo transfer process faster and easier for users. Avoid confirmation step in case transaction is verified.
* Reduce amount of data for fraud analysis. It’s not necessary to double check verified transactions
## Idea
This project automatically verifies Venmo transactions between two users if they have second degree connection
## Data
### Initial
Venmo transactions from 2010 to april 2017 (1TB+) stored in S3 bucket with JSON format. 
### Stored by project
* Venmo users (id, fullname)
* Friends (people with at least 1 transaction between them)
* Transactions
## Pipeline
## Where to start?
