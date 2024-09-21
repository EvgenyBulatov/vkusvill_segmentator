from pyspark.sql import SparkSession
import mtspark
from mtspark import init_spark, get_spark
import random
import time

mtspark.fix_pyspark_import(spark_version="3.2.0")
from pyspark.sql import Row
import pyspark.sql.functions as sf
from pyspark.sql.dataframe import DataFrame
import itertools

from pyspark.sql.types import StructType,StructField, StringType, DateType, IntegerType

from datetime import datetime, date, time

def build_order_clusterization() -> None:

    # dates
    today = date.today()

    # init spark session
    spark = init_spark({'appName': 'test_ins_fintech',
                        },
                       spark_version='3.3.1',
                       log_level='WARN')

    sizes_dict_schema = "size_name STRING, size_tag INT"

    sizes_dict_data = [
        ('tiny', 1),
        ('small', 2),
        ('medium', 3),
        ('large', 4),
    ]

    sizes_dict = spark.createDataFrame(sizes_dict_data, sizes_dict_schema)

    products_list_schema = "product_id INT, product_name STRING, size_tag INT, weight FLOAT, category_id INT"

    products_list_data = [
        (1, 'капучино', 1, 0.5, 2),
        (2, 'латте', 1, 0.3, 2),
        (3, 'булочка маковая', 1, 0.185, 1),
        (4, 'кекс лимонный', 1, 0.1, 1),
        (5, 'ассорти сыров', 1, 0.09, 2),
        (6, 'крем-сыр лабне', 2, 0.18, 2),
        (7, 'крем-чиз с зеленью', 1, 0.09, 2),
        (8, 'сыр гауда', 2, 0.3, 2),
        (9, 'круассан миндальный', 1, 0.12, 3),
        (10, 'сочник с творогом', 2, 0.18, 3),
        (11, 'лепешка с зеленью', 2, 0.175, 3),
        (12, 'напиток шорли', 2, 0.5, 4),
        (13, 'вода минеральная', 3, 1.0, 4),
        (14, 'морс клюквенный', 2, 0.5, 4),
        (15, 'квас', 3, 1.0, 4),
        (16, 'пицца римская', 3, 0.43, 5),
        (17, 'пельмени домашние', 3, 0.8, 5),
        (18, 'биточки из курицы', 3, 0.6, 5),
        (19, 'картофель молодой', 3, 1.5, 6),
        (20, 'арбуз', 4, 6.0, 6),
        (21, 'персики', 3, 2.0, 6),
        (22, 'филе грудки', 3, 0.6, 7),
        (23, 'голень цыпленка', 3, 0.75, 7),
        (24, 'фарш из говядины', 2, 0.36, 7),
        (25, 'молоко 3,2', 3, 1.0, 8),
        (26, 'масло сливочное', 2, 0.18, 8),
        (27, 'творог', 2, 0.33, 8),
        (28, 'макарон', 1, 0.15, 9),
        (29, 'ассорти эклеров', 2, 0.15, 9),
        (30, 'тирамису', 2, 0.2, 9),
    ]

    products_list = spark.createDataFrame(products_list_data, products_list_schema)

    product_categories_schema = "category_id INT, category_name STRING"

    product_categories_data = [
        (1, 'кафе'),
        (2, 'сыры'),
        (3, 'хлеб, выпечка'),
        (4, 'напитки'),
        (5, 'замороженные продукты'),
        (6, 'овощи, фрукты, зелень, ягоды'),
        (7, 'мясо, птица'),
        (8, 'молочные продукты, яйцо'),
        (9, 'сладости, десерты'),
    ]

    product_categories = spark.createDataFrame(product_categories_data, product_categories_schema)

    orders_info_schema = "order_id INT, product_id INT, count INT"

    orders_info_data = [
        (1, random.randint(1, 30), 1),
        (1, random.randint(1, 30), 1),
        (1, random.randint(1, 30), 1),
        (1, random.randint(1, 30), 1),
        (1, random.randint(1, 30), 1),
        (1, random.randint(1, 30), 1),
        (2, random.randint(1, 30), 1),
        (2, random.randint(1, 30), 1),
        (2, random.randint(1, 30), 1),
        (2, random.randint(1, 30), 1),
        (2, random.randint(1, 30), 1),
        (2, random.randint(1, 30), 1),
        (2, random.randint(1, 30), 1),
        (2, random.randint(1, 30), 1),
        (3, random.randint(1, 30), 1),
        (3, random.randint(1, 30), 1),
        (3, random.randint(1, 30), 1),
        (3, random.randint(1, 30), 1),
        (3, random.randint(1, 30), 1),
        (3, random.randint(1, 30), 1),
        (4, random.randint(1, 30), 1),
        (4, random.randint(1, 30), 1),
        (4, random.randint(1, 30), 1),
        (4, random.randint(1, 30), 1),
        (4, random.randint(1, 30), 1),
        (4, random.randint(1, 30), 1),
        (4, random.randint(1, 30), 1),
        (4, random.randint(1, 30), 1),
        (5, random.randint(1, 30), 1),
        (5, random.randint(1, 30), 1),
        (5, random.randint(1, 30), 1),
        (5, random.randint(1, 30), 1),
        (5, random.randint(1, 30), 1),
        (6, random.randint(1, 30), 1),
        (6, random.randint(1, 30), 1),
        (6, random.randint(1, 30), 1),
        (6, random.randint(1, 30), 1),
        (7, random.randint(1, 30), 1),
        (7, random.randint(1, 30), 1),
        (7, random.randint(1, 30), 1),
        (7, random.randint(1, 30), 1),
        (8, random.randint(1, 30), 1),
        (8, random.randint(1, 30), 1),
        (8, random.randint(1, 30), 1),
        (8, random.randint(1, 30), 1),
        (9, random.randint(1, 30), 1),
        (9, random.randint(1, 30), 1),
        (9, random.randint(1, 30), 1),
        (9, random.randint(1, 30), 1),
        (9, random.randint(1, 30), 1),
        (10, random.randint(1, 30), 1),
        (10, random.randint(1, 30), 1),
        (10, random.randint(1, 30), 1),
        (10, random.randint(1, 30), 1),
        (10, random.randint(1, 30), 1),
        (11, random.randint(1, 30), 1),
        (11, random.randint(1, 30), 1),
        (12, random.randint(1, 30), 1),
        (12, random.randint(1, 30), 1),
        (13, random.randint(1, 30), 1),
        (13, random.randint(1, 30), 1),
        (13, random.randint(1, 30), 1),
        (13, random.randint(1, 30), 1),
        (14, random.randint(1, 30), 1),
        (14, random.randint(1, 30), 1),
        (14, random.randint(1, 30), 1),
        (15, random.randint(1, 30), 1),
        (15, random.randint(1, 30), 1),
        (16, random.randint(1, 30), 1),
        (16, random.randint(1, 30), 1),
        (16, random.randint(1, 30), 1),
        (16, random.randint(1, 30), 1),
        (17, random.randint(1, 30), 1),
        (17, random.randint(1, 30), 1),
        (17, random.randint(1, 30), 1),
        (17, random.randint(1, 30), 1),
        (18, random.randint(1, 30), 1),
        (18, random.randint(1, 30), 1),
        (18, random.randint(1, 30), 1),
        (19, random.randint(1, 30), 1),
        (19, random.randint(1, 30), 1),
        (19, random.randint(1, 30), 1),
        (19, random.randint(1, 30), 1),
        (19, random.randint(1, 30), 1),
        (19, random.randint(1, 30), 1),
        (19, random.randint(1, 30), 1),
        (19, random.randint(1, 30), 1),
        (19, random.randint(1, 30), 1),
        (19, random.randint(1, 30), 1),
        (19, random.randint(1, 30), 1),
        (19, random.randint(1, 30), 1),
        (20, random.randint(1, 30), 1),
        (20, random.randint(1, 30), 1),
        (20, random.randint(1, 30), 1),
        (20, random.randint(1, 30), 1),
        (20, random.randint(1, 30), 1),
        (20, random.randint(1, 30), 1),
        (20, random.randint(1, 30), 1),
        (20, random.randint(1, 30), 1),
    ]

    orders_info = spark.createDataFrame(orders_info_data, orders_info_schema)

    orders_schema = "order_id INT, start_time TIMESTAMP, is_express BOOLEAN"

    orders_data = [
        (1, datetime.combine(today.date(), time(17, 30)), False),
        (2, datetime.combine(today.date(), time(17, 40)), False),
        (3, datetime.combine(today.date(), time(17, 50)), False),
        (4, datetime.combine(today.date(), time(17, 55)), False),
        (5, datetime.combine(today.date(), time(18, 0)), False),
        (6, datetime.combine(today.date(), time(18, 5)), False),
        (7, datetime.combine(today.date(), time(18, 10)), False),
        (8, datetime.combine(today.date(), time(18, 20)), False),
        (9, datetime.combine(today.date(), time(18, 20)), False),
        (10, datetime.combine(today.date(), time(18, 25)), False),
        (11, datetime.combine(today.date(), time(18, 30)), False),
        (12, datetime.combine(today.date(), time(18, 35)), False),
        (13, datetime.combine(today.date(), time(18, 40)), False),
        (14, datetime.combine(today.date(), time(18, 45)), False),
        (15, datetime.combine(today.date(), time(18, 50)), True),
        (16, datetime.combine(today.date(), time(18, 55)), True),
        (17, datetime.combine(today.date(), time(18, 55)), True),
        (18, datetime.combine(today.date(), time(19, 0)), True),
        (19, datetime.combine(today.date(), time(19, 10)), True),
        (20, datetime.combine(today.date(), time(19, 20)), True),
    ]

    orders = spark.createDataFrame(orders_data, orders_schema)

    # total df with all orders info
    pre_orders = orders_info \
        .join(products_list, 'product_id', how='left') \
        .join(product_categories, 'category_id', how='left') \
        .join(orders, 'order_id', how='left') \
        .join(sizes_dict, 'size_tag', how='left') \
        .select('order_id', 'product_name', 'weight', 'count', 'category_name', 'start_time', 'is_express', 'size_name')

    count_in_cl = 10
    clustering_orders, rest_data_about_orders = order_segmentator(spark, pre_orders, count_in_cl)

def order_segmentator(spark: SparkSession,
                  orders_df: DataFrame,
                  max_product_strings: int):

    # precreate final dataframe
    schema = StructType([
        StructField("first_order_id", IntegerType(), True),
        StructField("second_order_id", IntegerType(), True),
        StructField("comm_categories_count", IntegerType(), True)
    ])

    result_mapping_df = spark.createDataFrame([], schema)

    # segment for clustering by categories
    smart_segmentation_df = orders_df \
        .filter(sf.col('is_express') == False)

    orders_id_unpossible = orders_df \
        .filter(sf.col('size_name') == 'large') \
        .select('order_id').alias('order_id')

    final_smart_segmentation_df = smart_segmentation_df \
        .join(orders_id_unpossible, 'order_id', how='leftanti')

    df_with_count = final_smart_segmentation_df.groupby('order_id') \
        .agg(sf.count(sf.lit(1)).alias("count_in_order"))

    possible_matching = []
    for combination in itertools.combinations(df_with_count.collect(), 2):
        total_count = sum(item['count_in_order'] for item in combination)
        if total_count <= max_product_strings:
            possible_matching.append({
                'order_ids': [item['order_id'] for item in combination],
                'counts': [item['count_in_order'] for item in combination],
                'total_count': total_count
            })

    order_ids_list = [entry['order_ids'] for entry in possible_matching]

    for first_id, second_id in order_ids_list:
        first_order_categories = final_smart_segmentation_df.where(sf.col('order_id') == first_id).select(
            'category_name').collect()
        second_order_categories = final_smart_segmentation_df.where(sf.col('order_id') == first_id).select(
            'category_name').collect()

        unique_categories = set()

        # add categories from first list
        for item in first_order_categories:
            unique_categories.add(item.category_name)

        # add categories from second list
        for item in second_order_categories:
            unique_categories.add(item.category_name)

        unique_count_categories = len(unique_categories)
        empty_df = spark.createDataFrame([], schema)
        new_data = Row(first_order_id=first_id, second_order_id=second_id,
                       comm_categories_count=unique_count_categories)
        new_row_df = spark.createDataFrame([new_data], schema)

        result_mapping_df = result_mapping_df.union(new_row_df)

    # rest segment for standart clustering
    rest_segment = orders_df \
        .filter(sf.col('is_express') == True)

    result_list = []
    while result_mapping_df.select('first_order_id').distinct().count() > 1:
        # Получаем строку с наивысшим значением comm_categories_count (замените на свой столбец)
        first_priority = result_mapping_df.orderBy(sf.col('first_order_id').desc()).limit(1)

        # Извлекаем значения из первой строки
        first_row = first_priority.first()

        # Добавляем значения в список
        result_list.append([first_row.first_order_id, first_row.second_order_id])

        # Удаляем строку из DataFrame (если это необходимо)
        result_mapping_df = result_mapping_df.where((result_mapping_df.first_order_id != first_row.first_order_id) \
                                    | (result_mapping_df.first_order_id != first_row.second_order_id))

    return result_list, rest_segment

    ###############################################
    # пример работоспособности на тестовых данных #
    ###############################################

    # result_list - спиоок спуленных заказов в один СЛ

    # rest_segment - оставшаяся часть датасета для стандарной обработки

if __name__ == "__main__":
    build_order_clusterization()

