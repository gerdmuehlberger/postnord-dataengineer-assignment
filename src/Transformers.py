import pandas as pd
from functools import reduce
from datetime import datetime
from time import perf_counter


class MusicalInstrumentsReviewsTransformer:
    def __init__(self, dataframe: pd.DataFrame) -> None:
        self.dataframe = dataframe

    ###########################################################
    ############### Transformation Task Part 1 ################
    ###########################################################

    def apply_transformation_time_related_trends(self) -> pd.DataFrame:
        try:

            print("Applying transformation for time related trends.")
            log_start = perf_counter()

            # removing duplicate values from source dataframe
            self.dataframe = self.dataframe.drop_duplicates(
                subset=["reviewerID", "asin", "reviewTime"]
            )

            # creating hourly timecolumn
            self.dataframe["reviewTimeInHours"] = self.dataframe[
                "unixReviewTime"
            ].apply(lambda x: datetime.utcfromtimestamp(x).strftime("%Y-%m-%d-%H"))

            # transforming helpful list into percentage for aggregation
            self.dataframe["helpful_aslist"] = self.dataframe["helpful"].apply(
                lambda x: x.strip("][").split(", ")
            )
            self.dataframe["helpful_percentage"] = self.dataframe[
                "helpful_aslist"
            ].apply(lambda x: round(int(x[0]) / int(x[1]), 3) if int(x[1]) > 0 else 0)

            # average helpful rating of each hourly partition
            df_average_helpful = (
                self.dataframe[["reviewTimeInHours", "helpful_percentage"]]
                .groupby("reviewTimeInHours")
                .mean()
                .reset_index()
            )
            df_average_helpful.rename(
                columns={"helpful_percentage": "average_helpful_percentage"},
                inplace=True,
            )

            # median helpful rating of each hourly partition
            df_median_helpful = (
                self.dataframe[["reviewTimeInHours", "helpful_percentage"]]
                .groupby("reviewTimeInHours")
                .median()
                .reset_index()
            )
            df_median_helpful.rename(
                columns={"helpful_percentage": "median_helpful_percentage"},
                inplace=True,
            )

            # joining dataframes
            dfs_join = [
                df_average_helpful,
                df_median_helpful,
            ]

            output = reduce(
                lambda left, right: pd.merge(
                    left, right, on=["reviewTimeInHours"], how="outer"
                ),
                dfs_join,
            )

            log_stop = perf_counter()
            print(
                "Transformation for time related trends execution took:",
                log_stop - log_start,
                "seconds.",
            )

            return output

        except Exception as e:
            raise e

    def apply_transformation_time_related_trends_per_product(self) -> pd.DataFrame:
        try:

            print("Applying transformation for time related trends per product.")
            log_start = perf_counter()

            # removing duplicate values from source dataframe
            self.dataframe = self.dataframe.drop_duplicates(
                subset=["reviewerID", "asin", "reviewTime"]
            )

            # creating hourly timecolumn
            self.dataframe["reviewTimeInHours"] = self.dataframe[
                "unixReviewTime"
            ].apply(lambda x: datetime.utcfromtimestamp(x).strftime("%Y-%m-%d-%H"))

            df_average_overall_per_product = (
                self.dataframe.groupby(["reviewTimeInHours", "asin"])["overall"]
                .mean()
                .reset_index()
            )
            df_average_overall_per_product.rename(
                columns={"overall": "average_overall"},
                inplace=True,
            )

            df_median_overall_per_product = (
                self.dataframe.groupby(["reviewTimeInHours", "asin"])["overall"]
                .median()
                .reset_index()
            )
            df_median_overall_per_product.rename(
                columns={"overall": "median_overall"},
                inplace=True,
            )

            # joining dataframes
            dfs_join = [
                df_average_overall_per_product,
                df_median_overall_per_product,
            ]

            output = reduce(
                lambda left, right: pd.merge(
                    left, right, on=["reviewTimeInHours", "asin"], how="outer"
                ),
                dfs_join,
            )

            log_stop = perf_counter()
            print(
                "Transformation for time related trends per product execution took:",
                log_stop - log_start,
                "seconds.",
            )

            return output

        except Exception as e:
            raise e

    ###########################################################
    ############### Transformation Task Part 2 ################
    ###########################################################

    def apply_transformation_statistics_by_product(self) -> pd.DataFrame:
        try:
            print("Applying transformation for statistics by product.")
            log_start = perf_counter()

            # removing duplicate values from source dataframe
            self.dataframe = self.dataframe.drop_duplicates(
                subset=["reviewerID", "asin", "reviewTime"]
            )

            # average overall rating of the product by product ID (asin)
            df_average_overall = (
                self.dataframe[["asin", "overall"]].groupby("asin").mean().reset_index()
            )
            df_average_overall.rename(
                columns={"overall": "average_overall"}, inplace=True
            )

            # median overall rating of the product by product ID (asin)
            df_median_overall = (
                self.dataframe[["asin", "overall"]]
                .groupby("asin")
                .median()
                .reset_index()
            )
            df_median_overall.rename(
                columns={"overall": "median_overall"}, inplace=True
            )

            # number of unique reviewers of the product, using reviewerId
            df_unique_reviewers = (
                self.dataframe.groupby("asin")["reviewerID"].nunique().reset_index()
            )
            df_unique_reviewers.rename(
                columns={"reviewerID": "unique_reviewer_ids"}, inplace=True
            )

            # auxillary column for calculating average and median summary length
            self.dataframe["summary_length"] = self.dataframe["summary"].apply(
                lambda x: len(x)
            )

            # average length of summary i.e. average number of characters in each product's reviews
            df_average_summary_length = (
                self.dataframe[["asin", "summary_length"]]
                .groupby("asin")
                .mean()
                .reset_index()
            )
            df_average_summary_length.rename(
                columns={"summary_length": "average_summary_length"}, inplace=True
            )

            # medianlength of summary i.e. median number of characters in each product's reviews
            df_median_summary_length = (
                self.dataframe[["asin", "summary_length"]]
                .groupby("asin")
                .median()
                .reset_index()
            )
            df_median_summary_length.rename(
                columns={"summary_length": "median_summary_length"}, inplace=True
            )

            # joining dataframes
            dfs_join = [
                df_average_overall,
                df_median_overall,
                df_unique_reviewers,
                df_average_summary_length,
                df_median_summary_length,
            ]

            # merge all DataFrames into one
            output = reduce(
                lambda left, right: pd.merge(left, right, on=["asin"], how="outer"),
                dfs_join,
            )

            log_stop = perf_counter()
            print(
                "Transformation for statistics by product execution took:",
                log_stop - log_start,
                "seconds.",
            )
            return output

        except Exception as e:
            raise e
