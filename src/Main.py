from Importers import *
from Transformers import *
from datetime import datetime

CSV_Importer = CSV_Importer()
df_raw_source_data = CSV_Importer.import_from_csv(
    "sourcedata/musical_instruments_reviews_raw.csv"
)

musical_instruments_data_transformer = MusicalInstrumentsReviewsTransformer(
    df_raw_source_data
)

df_time_related_trends = (
    musical_instruments_data_transformer.apply_transformation_time_related_trends()
)

print(df_time_related_trends.head())
print()

df_time_related_trends_per_product = (
    musical_instruments_data_transformer.apply_transformation_time_related_trends_per_product()
)

print(df_time_related_trends_per_product.head())
print()

df_statistics_by_product = (
    musical_instruments_data_transformer.apply_transformation_statistics_by_product()
)

print(df_statistics_by_product.head())

df_time_related_trends.to_csv(
    "outputdata/time_related_trends_v1_{}.csv".format(
        str(datetime.today().strftime("%Y_%m_%d"))
    )
)
df_time_related_trends_per_product.to_csv(
    "outputdata/time_related_trends_per_product_v1_{}.csv".format(
        str(datetime.today().strftime("%Y_%m_%d"))
    )
)
df_statistics_by_product.to_csv(
    "outputdata/statistics_by_product_v1_{}.csv".format(
        str(datetime.today().strftime("%Y_%m_%d"))
    )
)
