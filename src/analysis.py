
from pyspark.sql import functions as F
from pyspark.sql import Window
from src.utils import load_csv_data_to_df, write_output


class CarCrashAnalysis:

    def __init__(self, spark, config):
        input_file_paths = config.get("INPUT_FILENAME")
        self.charges_df = load_csv_data_to_df(spark, input_file_paths.get("Charges"))
        self.damages_df = load_csv_data_to_df(spark, input_file_paths.get("Damages"))
        self.endorse_df = load_csv_data_to_df(spark, input_file_paths.get("Endorse"))
        self.primary_person_df = load_csv_data_to_df(spark, input_file_paths.get("Primary_Person"))
        self.units_df = load_csv_data_to_df(spark, input_file_paths.get("Units"))
        self.restrict_df = load_csv_data_to_df(spark, input_file_paths.get("Restrict"))

    def count_male_accidents(self, output_path, output_format) -> int:
        """
        Find the number of crashes (accidents) in which number of males killed are greater than 2

        Parameters:
            1. output_path (str): The file path for the output file.
            2. output_format (str): The file format for writing the output.
        Returns:
            int: The count of crashes in which number of males killed are greater than 2
        """

        df = self.primary_person_use.filter((F.col("PRSN_GNDR_ID") == "MALE") & (F.col("DEATH_CNT") > 0))\
            .groupBy("CRASH_ID")\
            .agg(F.sum("DEATH_CNT").alias("male_death_count"))\
            .filter(F.col("male_death_count") > 2)

        write_output(df, output_path, output_format)

        return df.count()

    def count_2_wheeler_accidents(self, output_path, output_format) -> int:
        """
        Determine how many two-wheelers are booked for crashes

        Parameters:
            1. output_format (str): The file format for writing the output.
            2. output_path (str): The file path for the output file.
        Returns:
            int: The count of crashes involving 2-wheeler vehicles.
        """
        df = self.units_df.filter(F.col("VEH_BODY_STYL_ID") == "MOTORCYCLE")

        write_output(df, output_path, output_format)

        return df.count()

    def top_5_vehicle_makes_for_fatal_crashes_without_airbags(self, output_path, output_format) -> list:
        """
        Determine the Top 5 Vehicle Makes of the cars present in the crashes in which driver died and
        Airbags did not deploy.

        Parameters:
            1. output_format (str): The file format for writing the output.
            2. output_path (str): The file path for the output file.
        Returns:
            List[str]: Top 5 vehicles Make for killed crashes without an airbag deployment.

        """
        df = self.units_df.join(self.primary_person_df, ["CRASH_ID", "UNIT_NBR"])\
            .filter((F.col("PRSN_INJRY_SEV_ID") == "KILLED") &
                    (F.col("PRSN_AIRBAG_ID") == "NOT DEPLOYED") &
                    (F.col("PRSN_TYPE_ID") == "DRIVER"))\
            .groupBy("VEH_MAKE_ID")\
            .count()\
            .orderBy(F.col("count").desc()).limit(5)

        write_output(df, output_path, output_format)

        return [row[0] for row in df.collect()]

    def count_hit_and_run_with_valid_licenses(self, output_path, output_format) -> int:
        """
        Determine number of Vehicles with driver having valid licences involved in hit-and-run

        Parameters:
        1. output_path (str): The file path for the output file.
        2. output_format (str): The file format for writing the output.
        Returns:
        3. int: The count of vehicles involved in hit-and-run incidents with drivers holding valid licenses.
        """
        df = (self.units_df.join(self.primary_person_df, on = ["CRASH_ID", "UNIT_NBR"]).
              filter((F.col("VEH_HNR_FL") == "Y") &
                     (F.col("DRVR_LIC_TYPE_ID").isin(["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."])))
              .select("CRASH_ID", "UNIT_NBR").distinct()
              )

        write_output(df, output_path, output_format)

        return df.count()

    def get_state_with_no_female_accident(self, output_path, output_format) -> str:
        """
        Determine Which state has highest number of accidents in which females are not involved

        Parameters:
            1. output_format (str): The file format for writing the output.
            2. output_path (str): The file path for the output file.
        Returns:
            str: The state with the highest number of accidents without female involvement.
        """
        df = (self.primary_person_df.filter(self.primary_person_df["PRSN_GNDR_ID"] != "FEMALE")
              .groupby("DRVR_LIC_STATE_ID").count()
              .orderBy(F.col("count").desc())

        )
        top_state = df.first().DRVR_LIC_STATE_ID

        write_output(df, output_path, output_format)

        return top_state

    def get_top_vehicle_contributing_to_injuries(self, output_path, output_format) -> list:
        """
        Find which are the Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death

        Parameters:
            1. output_format (str): The file format for writing the output.
            2. output_path (str): The file path for the output file.
        Returns:
            List[int]: The Top 3rd to 5th VEH_MAKE_IDs that contribute to the largest number of injuries,
            including death.
        """

        # Aggregate casualties by vehicle make
        casualties_by_make = self.units_df.groupBy("VEH_MAKE_ID") \
            .agg(F.sum(F.col("TOT_INJRY_CNT") + F.col("DEATH_CNT")).alias("total_casualties"))

        # Rank the makes by total casualties
        window_spec = Window.orderBy(F.desc("total_casualties"))
        ranked_makes = casualties_by_make.withColumn("rank", F.dense_rank().over(window_spec))

        # Select the 3rd to 5th ranks
        ranked_makes.filter((F.col("rank") >= 3) & (F.col("rank") <= 5)) \
            .select("VEH_MAKE_ID", "total_casualties", "rank") \
            .orderBy("rank")

        write_output(ranked_makes, output_path, output_format)

        return [veh[0] for veh in ranked_makes.select("VEH_MAKE_ID").collect()]

    def get_top_ethnic_ug_crash_for_each_body_style(self, output_path, output_format):
        """
        Determine all the body styles involved in crashes, mention the top ethnic user group of each unique body style

        Parameters:
            1. output_format (str): The file format for writing the output.
            2. output_path (str): The file path for the output file.
        Returns:
            DataFrame
        """
        window_spec = Window.partitionBy("VEH_BODY_STYL_ID").orderBy(F.desc("ethnicity_count"))

        df = (self.units_df.join(self.primary_person_df, on="CRASH_ID")
                .groupBy("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID")
                .agg(F.count("*").alias("ethnicity_count"))
                .withColumn("rank", F.row_number().over(window_spec))
                .filter(F.col("rank") == 1)
                .select("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID")
              )

        write_output(df, output_path, output_format)

        return df

    def get_top_5_zip_codes_with_alcohols_as_cf_for_crash(self, output_path, output_format) -> list:
        """
        Among the crashed cars, Find what are the Top 5 Zip Codes with highest number crashes with alcohols as
        the contributing factor to a crash (Use Driver Zip Code).

        Parameters:
            1. output_format (str): The file format for writing the output.
            2. output_path (str): The file path for the output file.
        Returns:
            List[str]: The top 5 Zip Codes with the highest number of alcohol-related crashes.

        """
        df = (
            self.units_df.join(self.primary_person_df, "CRASH_ID")
                .filter((F.col("CONTRIB_FACTR_1_ID").contains("ALCOHOL") |
                         F.col("CONTRIB_FACTR_2_ID").contains("ALCOHOL")) &
                        F.col("DRVR_ZIP").isNotNull())
                .groupBy("DRVR_ZIP")
                .agg(F.countDistinct("CRASH_ID").alias("crash_count"))
                .orderBy(F.desc("crash_count"))
                .limit(5)
        )

        write_output(df, output_path, output_format)

        return [row[0] for row in df.collect()]

    def get_crash_ids_with_no_damage(self, output_path, output_format) -> list:
        """
        Determine Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~)
        is above 4 and car avails Insurance.

        Parameters:
            1. output_format (str): The file format for writing the output.
            2. output_path (str): The file path for the output file.
        Returns:
            List[str]: The list of distinct Crash IDs meeting the specified criteria.
        """


        df = (
            self.damages_df.join(self.units_df, on=["CRASH_ID"], how="inner")
            .filter(
                (
                    (self.units_df["VEH_DMAG_SCL_1_ID"] > "DAMAGED 4")
                    & (
                        ~self.units_df["VEH_DMAG_SCL_1_ID"].isin(
                            ["NA", "NO DAMAGE", "INVALID VALUE"]
                        )
                    )
                ) |
                (
                    (self.units_df["VEH_DMAG_SCL_2_ID"] > "DAMAGED 4")
                    & (
                        ~self.units_df["VEH_DMAG_SCL_2_ID"].isin(
                            ["NA", "NO DAMAGE", "INVALID VALUE"]
                        )
                    )
                )
            )
            .filter(self.damages_df["DAMAGED_PROPERTY"] == "NONE")
            .filter(self.units_df["FIN_RESP_TYPE_ID"] == "PROOF OF LIABILITY INSURANCE")
        )

        write_output(df, output_path, output_format)

        return [row[0] for row in df.collect()]

    def get_top_5_vehicle_brand(self, output_path, output_format) -> list:
        """
        Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences,
        has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states
        with highest number of offences (to be deduced from the data)

        Parameters:
            1. output_format (str): The file format for writing the output.
            2. output_path (str): The file path for the output file.
        Returns:
            List[str]: The list of top 5 Vehicle Makes/Brands meeting the specified criteria.
        """
        top_25_state_list = [
            row[0]
            for row in self.units_df.filter(
                F.col("VEH_LIC_STATE_ID").cast("int").isNull() &
                (F.col("VEH_LIC_STATE_ID") != "NA")
            )
                .groupby("VEH_LIC_STATE_ID")
                .count()
                .orderBy(F.col("count").desc())
                .limit(25)
                .collect()
        ]

        top_10_used_vehicle_colors = [
            row[0]
            for row in self.units_df.filter(self.units_df["VEH_COLOR_ID"] != "NA")
                .groupby("VEH_COLOR_ID")
                .count()
                .orderBy(F.col("count").desc())
                .limit(10)
                .collect()
        ]

        df = (
            self.charges_df.join(self.primary_person_df, on=["CRASH_ID"], how="inner")
                .join(self.units_df, on=["CRASH_ID"], how="inner")
                .filter(self.charges_df["CHARGE"].contains("SPEED"))
                .filter(
                self.primary_person_df["DRVR_LIC_TYPE_ID"].isin(
                    ["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."]
                )
            )
                .filter(self.units_df["VEH_COLOR_ID"].isin(top_10_used_vehicle_colors))
                .filter(self.units_df["VEH_LIC_STATE_ID"].isin(top_25_state_list))
                .groupby("VEH_MAKE_ID")
                .count()
                .orderBy(F.col("count").desc())
                .limit(5)
        )

        write_output(df, output_path, output_format)

        return [row[0] for row in df.collect()]
