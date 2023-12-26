# Description: 
Extract data of a retail store from MySQL database, transform with pandas then load it into data warehouse (PostgreSQL). Build a classification model to predict which customers will be interested in coupon redemption, helping marketing team to run campaigns more efficiently.

# Dataset: 
<url>https://www.kaggle.com/datasets/frtgnn/dunnhumby-the-complete-journey</url>

![alt text](https://github.com/huynhdoanho/dagster_etl_dunnhumby/blob/81132c78cf80d03e35ba92f92df43098f439458b/img/data.png)

# Overview:

![alt text](https://github.com/huynhdoanho/dagster_etl_dunnhumby/blob/0050634e632f2bdab49af08742002942e7000dc2/img/overview.png)

# Description:
This project is an ETL pipeline that:
- Extract data from MYSQL
- Transform with pandas
- Load to data warehouse: PostgreSQL

# How to run ?
```
git clone https://github.com/huynhdoanho/dagster_etl_dunnhumby
```

```
docker compose build
```

```
docker compose up -d
```

Check  <b> localhost:3001 </b>, our Dagster Asset Lineage will be like this:

![alt text](https://github.com/huynhdoanho/dagster_etl_dunnhumby/blob/81132c78cf80d03e35ba92f92df43098f439458b/img/dags.png)

- <b>Note: Before materializing, make sure that you have imported the Brazillian Ecommerce dataset (I have attached the link above) to data source (MySQL). You can connect to MySQL through port 3307, user: admin, password: admin123, database: brazillian-ecommerce
</b>

Click materialize button to run

Check PostgreSQL at:
- Port: 5434
- Database: postgres
- User: admin
- Password: admin123

You can also check Minio at <b> localhost:9001 </b>
- User: admin
- Password: admin123

```
docker compose down
```
