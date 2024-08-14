# Data Engineering Assignment (PySpark)
Before starting to work on the assignment - please make sure you fork this repository.


## Requirements:
This is an assignment for a Data Engineer role. You are requested to:
- Read and understand the requirements. You may contact the interviewer for further clarification
- Write code that answers the objectives
- Deploy the code to the provided AWS account

You are a Data Engineer in a financial institute. Your task is to calculate and answer the business questions (objectives) provided by the analysts team. You’re provided here with a small dataset, but in a real-world scenario you'll have a huge dataset, so the code needs to be deployed and run on a cloud environenment.


## Coding instructions:
- The file `stock_prices.csv` contains daily closing price of a few stocks on the NYSE/NASDAQ
- Load the file as a DataFrame, Dataset, or RDD and complete the assignment objectives
- The result of each question should be saved as a separate file in an S3 Bucket


## Assumptions:
- Use only the closing price to determine returns
- If a price is missing on a given date, you can compute returns from the closest available date
- Return can be trivially computed as the % difference of two prices


## Objectives:
1. Compute the average daily return of all stocks for every date

   | date    | average_return |
   | -------- | ------- |
   | yyyy-MM-dd  | return of all stocks on that date   |


2. Which stock was traded most frequently - as measured by **closing price * volume** - on average? 

    | ticker | frequency |
    | ------ | ---------------------- |
    |   |  |


3. Which stock was the most volatile as measured by the annualized standard deviation of daily returns?

    | ticker | standard_deviation |
    | ------ |--------------------|
    |   |                    |


4. What were the top three 30-day return dates (% increase in closing price as compared to the closing price 30 days prior) — present ticker and date combinations

    | ticker | date |
    | ------ | ---- |
    |   |      |


## AWS Deployment
- At Vi, we manage and provision cloud infrastructure through definition files ([IaC](https://en.wikipedia.org/wiki/Infrastructure_as_code)). Please use IaC (such as CloudFormation) to deploy the code you created to perform the below tasks.
  - If your not familiar with Cloudformation - please use AWS console
- Infrastructure tasks:
  - Create a glue job
  - Create a Glue Catalog Database
  - Create a Glue Catalog Table for each result file
  - Create crawler/s
- **Expected result: Questions (objectives) results should be queryable from Athena**
- If you encounter issues with reading/writing from/to S3 buckets, it is recommended to add your name as a prefix to the bucket’s name. For example name the bucket: “data-engineer-assignment-my-name”
- Use the AWS credentials provided in the email to deploy the code. **DO NOT COMMIT THEM IN THE CODE.**
- Deploy your resources in the **Europe (Frankfurt) eu-central-1**
- You can use the template cloudformation and deploy command to deploy the stack file, example:
  ```bash
  aws cloudformation deploy --template-file cloudformation.yml --stack-name my-stack
  ```


## Submission
Please share your Github repo by replying to the email.
Write us any assumptions you made or additional information you think is relevant.


## Evaluation
- Objectives completion
- Code quality & efficiency
- AWS deployment
