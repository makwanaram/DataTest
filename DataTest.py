from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import argparse

def getSession():
	sc= SparkSession.builder.appName('TestApp').master('local').getOrCreate()
	return sc

def getDataFrames(sc , paths):
	trans_df = sc.read.option('multiline',True).json(paths['transactions_location'])
	cust_df=sc.read.option('header',True).csv(paths['customers_location'])
	prod_df = sc.read.option('header',True).csv(paths['products_location'])
	return trans_df,cust_df,prod_df


def get_params() -> dict:
    parser = argparse.ArgumentParser(description='DataTest')
    parser.add_argument('--customers_location', required=False, default="./input_data/starter/customers.csv")
    parser.add_argument('--products_location', required=False, default="./input_data/starter/products.csv")
    parser.add_argument('--transactions_location', required=False, default="./input_data/starter/transactions/")
    parser.add_argument('--output_location', required=False, default="./output_data/outputs/")
    return vars(parser.parse_args())





#json_df = sc.read.option('multiline',True).json('starter/transactions')
#cust_df=sc.read.option('header',True).csv('starter/customers.csv')
#prod_df = sc.read.option('header',True).csv('starter/products.csv')



def joinDf(trans_df,cust_df,prod_df):

	join_cust_json=cust_df.join(trans_df,['customer_id'],"inner")
	add_col= join_cust_json.withColumn('basket_explode',explode('basket'))\
	.withColumn('product_id',col('basket_explode.product_id'))\
	.withColumn('product_price',col('basket_explode.price'))
	join_all=add_col.join(prod_df,['product_id'],"inner")
	not_req=['basket','d','product_description']
	final_df=join_all.drop(*(not_req))
	final_df=final_df.withColumn("weekoftheyear",weekofyear(col("date_of_purchase")))
	final_df=final_df.groupBy('weekoftheyear','customer_id','loyalty_score','product_id','product_category').count()
	select_col=['customer_id','loyalty_score','product_id','product_category','weekoftheyear']
	final_df=final_df.select(*(select_col),col("count").alias("purchase_count") )
	return final_df


def generateJsons(df,path):
	df.write.format("json").mode("overwrite").partitionBy("weekoftheyear").save(path)



	if __name__ == '__main__':
	params = get_params()
	session = getSession()
	trans_df,cust_df,prod_df = getDataFrames(session,params)
	final_df = joinDf(trans_df,cust_df,prod_df )
	generateJsons(final_df,params['output_location'])