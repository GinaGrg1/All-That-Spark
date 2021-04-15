from pyspark.sql.functions import monotonically_increasing_id, struct, to_json

df_data_format = spark.read.format("csv").option('header','true').option('inferSchema', 'true').option('delimiter', ';').load(format_path)
df_data_format = df_data_format.withColumn("order", monotonically_increasing_id()+1)

df_data_format = df_data_format.withColumn("index", to_json(struct([df_data_format["order"]]))).drop("order")

+-------------------+--------------------+----------+--------+------+--------------------+-------------------+--------------------+------+------+------------+
|               Data|               Field|      Type|Nullable|Unique|               Regex|          Integrity|             RegDesc|LenMin|LenMax|       index|
+-------------------+--------------------+----------+--------+------+--------------------+-------------------+--------------------+------+------+------------+
|      purchase_core|          PurchaseID|    bigint|      No|   Yes|            ^[0-9]*$|               null|        Only numbers|     1|    20| {"order":1}|
|      purchase_core|       PanelMemberID|      null|      No|    No|               \d-\d|               null|Must be Number-Nu...|  null|  null| {"order":2}|
|      purchase_core|PurchaseDictionaryID|       int|     Yes|    No|                null|               null|                null|  null|  null| {"order":3}|
|      purchase_core|        PurchaseDate|      null|      No|    No|([1-2][0-9]{3})(0...|               null|Must be YYYYMMDD ...|  null|  null| {"order":4}|
|      purchase_core|        PurchaseTime|      null|      No|    No|^([0-1][0-9]|2[0-...|               null|Must be HHMM and ...|  null|  null| {"order":5}|
|      purchase_core|             VenueID|      null|     Yes|    No|  ^[a-zA-Z0-9_\-,]*$|               null|Allow characters ...|     0|    12| {"order":6}|
|      purchase_core|RetailerDictionaryID|       int|     Yes|    No|                null|               null|                null|  null|  null| {"order":7}|
|      purchase_core|            ShopCode|      null|     Yes|    No|  ^[a-zA-Z0-9_\-,]*$|               null|Allow characters ...|     0|    50| {"order":8}|
|      purchase_core|         ProductCode|      null|      No|    No|  ^[a-zA-Z0-9_\-,]*$|               null|Allow characters ...|     1|    12| {"order":9}|
|      purchase_core| ProductDictionaryID|       int|     Yes|    No|                null|               null|                null|  null|  null|{"order":10}|
