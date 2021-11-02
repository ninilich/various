# Capstone project

## Introduction

BigData Tasks: Marketing Analytics

You work at a data engineering department of a company building an ecommerce platform. There is a mobile application that is used by customers to transact with its on-line store. Marketing department of the company has set up various campaigns (e.g. “Buy one thing and get another one as a gift”, etc.)  via different marketing channels (e.g. Google / Yandex / Facebook Ads, etc.).
Now the business wants to know the efficiency of the campaigns and channels.
Let’s help them out!
Given datasets

Full task description you can find [here](
https://docs.google.com/document/d/e/2PACX-1vTnYKo-FpJQ2GL_YgVIfZeTMiu5bnnH-aTbBaOyxdzl43T2zm5nhe4YYPd7c44xItTk8Ot8teVfuFqf/pub?referrer=https%3A%2F%2Fgridu.litmos.com%2F#)

## Solution

1. Generate dataset - see [this repo](https://github.com/gridu/INTRO_SPARK-SCALA_FOR_STUDENTS) and follow instructions there.
2. Compile and run main-method 

When running main-method, you need to pass 2 parameters:
* inputPath - path with input data
* outputPath - path to save output data

F.e. running with sbt-shell:
```
run /input/path /output/path
```
InputPath has to contain 2 folders with input data:
```
/input/Path/
|-- mobile_app_clickstream
|-- user_purchases
```

Output data will be saved in outputPath:
* Task 1.1 - folder "PurchasesAttributionProjection1"
* Task 1.2 - folder "PurchasesAttributionProjection2"
* Task 2.1 - folders "top10CampaignsSQL" and "top10campaignWithOutSql"
* Task 2.2 - folder "popularChannelSql" and "popularChannelWithOutSql"
