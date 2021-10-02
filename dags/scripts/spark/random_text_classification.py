# pyspark
import argparse

from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, StopWordsRemover
from pyspark.sql.functions import array_contains

# csv columns -> cid,review_str

def random_text_classification(input_loc, output_loc):
    # read input
    df_raw = spark.read.option("header",True).csv(input_loc)

    # Tokenize text (String -> Array<String>)
    tokenizer = Tokenizer(inputCol='review_str',outputCol='review_token')
    df_tokens = tokenizer.transform(df_raw).select('cid','review_token')

    #remove stop words
    # A stop word is a commonly used word (such as “the”, “a”, “an”, “in”) which we can ignore
    # We would not want these words to take up space in our database, 
    # or taking up valuable processing time.
    remover = StopWordsRemover(inputCol='review_token',outputCol='review_clean')
    df_clean = remover.transform(df_tokens).select('cid','review_clean')

    #function to check presense of good
    df_out = df_clean.select(
        'cid',array_contains(df_clean.review_clean,'good').alias('positive_review')
    )

    # save output df in parquet format
    df_out.write.mode('overwrite').parquet(output_loc)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input",type=str, help='HDFS Input', default="/movie")
    parser.add_argument("--output",type=str, help='HDFS Output', default="/output")
    args = parser.parse_args()
    spark = SparkSession.builder.appName("Random Text Classifier").getOrCreate()
    random_text_classification(input_loc=args.input,output_loc=args.output)



