from pyspark.sql import SparkSession

def get_product_category_pairs(products_df, categories_df, relations_df):

    # Левый join продуктов с связями
    prod_rel = products_df.join(relations_df, on="product_id", how="left")
    # Левый join с категориями
    prod_cat = prod_rel.join(categories_df, on="category_id", how="left")
    result = prod_cat.select("product_name", "category_name")
    return result

def main():
    spark = SparkSession.builder \
        .appName("ProductCategoryPairs") \
        .getOrCreate()

    # Загрузка CSV файлов котрые заранее создали
    products_df = spark.read.csv("data/products.csv", header=True, inferSchema=True)
    categories_df = spark.read.csv("data/categories.csv", header=True, inferSchema=True)
    relations_df = spark.read.csv("data/relations.csv", header=True, inferSchema=True)

    result_df = get_product_category_pairs(products_df, categories_df, relations_df)

    result_df.show(truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()
