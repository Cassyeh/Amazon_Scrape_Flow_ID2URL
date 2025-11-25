# Amazon_Scrape_Flow_ID2URL

ETL pipeline that enriches a public Amazon review dataset by adding corresponding product URLs to extracted top-rated product IDs.

This project demonstrates a simple ETL (Extract, Transform, Load) pipeline that processes **Amazon Customer Review Data** to identify and display the **top 10000 products with the highest 5-star ratings**. 

In addition, the pipeline **enriches the dataset** by mapping product IDs to their corresponding Amazon product URLs.
This is done by:

- Using the product ID from the dataset.
- Searching the internet using the Python requests module, USER AGENTS and a helper function for links that contain the product ID. 
- Selecting only URLs that:
  - Are associated with **www.amazon.**.
  - End with or contain the exact product ID or the parent product ID.
- Retrieves the product name associated with the URL.
- Stores both the URL and product name to the augmented dataset.

The resulting enriched dataset allows you to directly access the product pages for the top-rated items.

## Example

Product ID in dataset:

```bash
B0CJZMP7L1
```

Enriched data added:

```bash
Product_Name: Stanley Quencher H2.0 Tumbler with Handle and Straw 30 oz | Flowstate 3 ...
URL: https://www.amazon.ca/Stanley-Quencher-FlowState-Stainless-Insulated/dp/B0CJZMP7L1
```

The goal is to showcase how data can be cleaned, processed, and stored to derive actionable business insights.

## Ultimate Business Purpose

The ultimate purpose of this pipeline is to **identify and list the top 10000 products** with the **highest frequency of 5-star ratings** from the **Amazon Customer Reviews** dataset. This can help businesses identify their **best-performing products**, allowing for better decision-making, targeted marketing, and product strategy.
