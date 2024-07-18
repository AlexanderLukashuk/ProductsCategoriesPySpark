from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast

spark = SparkSession.builder \
    .appName("ProductsCategories") \
    .getOrCreate()

products_data = [
    (1, "Milk"),
    (2, "Coffee"),
    (3, "Apple"),
    (4, "Book")
]

categories_data = [
    (1, "Category 1"),
    (2, "Category 2"),
    (3, "Category 3")
]

product_category_data = [
    (1, 1),
    (1, 2),
    (2, 2),
    (3, 3)
]

products_df = spark.createDataFrame(products_data, ["product_id", "product_name"])
categories_df = spark.createDataFrame(categories_data, ["category_id", "category_name"])
product_category_df = spark.createDataFrame(product_category_data, ["product_id", "category_id"])

product_category_join_df = product_category_df.join(
    broadcast(categories_df),
    product_category_df.category_id == categories_df.category_id,
    how="inner"
).select("product_id", "category_name")

product_category_pairs_df = product_category_join_df.join(
    products_df,
    product_category_join_df.product_id == products_df.product_id,
    how="inner"
).select("product_name", "category_name")

products_without_categories_df = products_df.join(
    product_category_df,
    products_df.product_id == product_category_df.product_id,
    how="left_anti"
).select("product_name")

product_category_pairs_df.show()
products_without_categories_df.show()
