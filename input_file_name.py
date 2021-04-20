def get_all_dataframe_from_blob(file_dict: dict, source_path: str) -> tuple:
  """
  Loop through all keys in MetaData.xml (file_dict), get the file names and load it from blob into spark df.
  Merge all the files of same file_type (e.g purchase_core, occasion_core) into one single spark df.
  
  Returns a tuple of length 2, each a dictionary, with file_type as key and dataframe object as value.
        {'purchase_core': DataFrame[], 'occasion_core': DataFrame[],...}
  
  all_dataframe_dict is a dataframe with inferred schema.
        {'purchase_core': DataFrame[PurchaseID: bigint,..], 'occasion_core': DataFrame[PurchaseID: bigint],...}
  
  all_dataframe_dict_str has all dtypes as string.
  This df is used for qc checks 4.
        {'purchase_core': DataFrame[PurchaseID: string,..], 'occasion_core': DataFrame[PurchaseID: string],...}
  
  """
  all_dataframe_dict, all_dataframe_dict_str = {}, {}
  for file_type, value in file_dict.items():
    delimiter = value.get('delimiter')
    file_paths = [os.path.join(source_path, file) for file in value.get('files')]    
    df = spark.read.format("csv").option('header','true').option('inferSchema', 'true').option('delimiter', delimiter).load(file_paths).withColumn("filename", F.input_file_name())
    df = df.withColumn("filename", F.reverse(F.split(df.filename,'/',-1))[0])
    
    if not df.rdd.isEmpty():
      df_str = spark.read.format("csv").option('header','true').option('delimiter', delimiter).load(file_paths).withColumn("filename", F.input_file_name())
      df_str = df_str.withColumn("filename", F.reverse(F.split(df_str.filename,'/',-1))[0])
      all_dataframe_dict[file_type] = df
      all_dataframe_dict_str[file_type] = df_str
  return all_dataframe_dict, all_dataframe_dict_str

 unique_columns = df_data_format.select('Field').where((df_data_format.Data == file_type) & (df_data_format.Unique == 'Yes')).rdd.map(lambda row: row[0]).collect()
 df = data_file_df.select(unique_columns + ['filename'])
        
 df_duplicate = df.select('*', F.count(F.struct(unique_columns)).over(Window.partitionBy(unique_columns)).alias('dupes')).where('dupes > 1').drop('dupes')

 df_duplicate.show(10, False)
 
 +------------------+----------------+-------------------------+-----+
|PurchaseID        |PurchaseChildKey|filename                 |dupes|
+------------------+----------------+-------------------------+-----+
|100201913000000100|V-ProduitPolling|purchase_child_201913.csv|2    |
|100201913000000100|V-ProduitPolling|purchase_child_201914.csv|2    |
|100201913000000200|C-FidelityCard  |purchase_child_201913.csv|2    |
|100201913000000200|C-FidelityCard  |purchase_child_201914.csv|2    |
|100201913000000100|C-WeekDay       |purchase_child_201913.csv|2    |
|100201913000000100|C-WeekDay       |purchase_child_201914.csv|2    |
|100201913000000100|C-FidelityCard  |purchase_child_201913.csv|2    |
|100201913000000100|C-FidelityCard  |purchase_child_201914.csv|2    |
+------------------+----------------+-------------------------+-----+
