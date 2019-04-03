# Welcome to the interview data repo.

## Background:
Our customers sell products via an online marketplace (Amazon). They also pay for their products to appear when particular searches are made - these are 'keywords' paid for as part of an advertising group or 'ad group'.

Our products help our customers understand how their inventory is selling, helps calculate profits and losses, and helps provide insight into whether advertising spend is effective.

This repository includes some trimmed down example data for this problem space.

### Concepts/Acronyms

* *ASIN* - product id
* *ad_spend* - amount spent on advertising
* *keyword* - the search term entered by a user where one of our customer's products appeared as a sponsored result/ad
* *impression* - when a user performs a search and sees a sponsored result/ad, regardless of whether they click on it
* *clicks* - when a user clicks on a customer's product appearing as a sponsored result/ad
* *ad_group* - a 'bucket' of keywords

### Data Files:

* *ad_report.csv* - Impressions, clicks, sales and other ad stats by date and product.
* *product_report.csv* - Product names and ASIN's.
* *keyword_report.csv* - Keywords managed by our customers.

### Task - CSV Manipulation
From this dataset, we'd like to know:
* Name of the best selling item in the 30 days prior to 6/1/2017, and the total sales for that item. 
