from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import argparse


def getSession():
	'''This function create Spark Session  '''
	sc= SparkSession.builder.appName('DataTest').master('local').getOrCreate()
	return sc


def getDataFrames(sc , paths):
	'''This function is used to import files from given input path and create dataframe'''
	try:
		trans_df = sc.read.option('multiline',True).json(paths['transactions_location'])
		cust_df=sc.read.option('header',True).csv(paths['customers_location'])
		prod_df = sc.read.option('header',True).csv(paths['products_location'])
		return trans_df,cust_df,prod_df
	except Exception as e:
		print(e)


def get_params() -> dict:
	'''This function is used to capture argument enter by user''' 
	try:
	    parser = argparse.ArgumentParser(description='DataTest')
	    parser.add_argument('--customers_location', required=False, default="./input_data/starter/customers.csv")
	    parser.add_argument('--products_location', required=False, default="./input_data/starter/products.csv")
	    parser.add_argument('--transactions_location', required=False, default="./input_data/starter/transactions/")
	    parser.add_argument('--output_location', required=False, default="./output_data/outputs/")
	    return vars(parser.parse_args())
	except Exception as e:
		print(e)


def joinDf(trans_df,cust_df,prod_df):
	'''This function doing main part of logic.It join multiple dataframe, remove extra columns from dataframe 
	and calulate count'''
	try:
		#Join Customer and transaction dataframe
		join_cust_json=cust_df.join(trans_df,['customer_id'],"inner") 

		# split json array create new column in dataframe 
		add_col= join_cust_json.withColumn('basket_explode',explode('basket'))\
		.withColumn('product_id',col('basket_explode.product_id'))\
		.withColumn('product_price',col('basket_explode.price'))    

		#join with product dataframe
		join_all=add_col.join(prod_df,['product_id'],"inner")	

		#remove not required column from dataframe
		not_req=['basket','d','product_description']
		final_df=join_all.drop(*(not_req)) 	

			# add weekoftheyear column to calulate values every week
		final_df=final_df.withColumn("weekoftheyear",weekofyear(col("date_of_purchase")))	

		# calculate product count for every week per customer
		final_df=final_df.groupBy('weekoftheyear','customer_id','loyalty_score','product_id','product_category').count() 

		# cretae final dataframe with reuired columns
		select_col=['customer_id','loyalty_score','product_id','product_category','weekoftheyear']
		final_df=final_df.select(*(select_col),col("count").alias("purchase_count") ) 
		return final_df
	except Exception as e:
		print(e)


def generateJsons(df,path):
	'''This function is used to create output json files with partition on week. So that it will create json file per week  '''
	df.write.format("json").mode("overwrite").partitionBy("weekoftheyear").save(path)



if __name__ == '__main__':
	try:
		params = get_params()
		session = getSession()
		trans_df,cust_df,prod_df = getDataFrames(session,params)
		final_df = joinDf(trans_df,cust_df,prod_df )
		generateJsons(final_df,params['output_location'])
	except Exception as e:
		print(e)	
