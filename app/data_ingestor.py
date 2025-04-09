"""
Module for processing and analyzing nutrition and obesity data from csv files.
Provides functions to calculate various statistics for states and categories.
"""
import logging
import pandas as pd

# get logger from flask app
logger = logging.getLogger('webserver')

class DataIngestor:
    """
    processes nutrition and obesity data to provide statistical analysis.
    handles data filtering, aggregation, and comparison across states and categories.
    """

    def __init__(self, csv_path: str):
        """
        initialize with a csv file.
        
        args:
            csv_path: path to the csv file
        """
        logger.info("Initializing DataIngestor with CSV: %s", csv_path)
        # read csv from csv_path
        self.df = pd.read_csv(csv_path)
        logger.info("CSV data loaded with %d rows", len(self.df))

        self.questions_best_is_min = [
            'Percent of adults aged 18 years and older who have an overweight classification',
            'Percent of adults aged 18 years and older who have obesity',
            'Percent of adults who engage in no leisure-time physical activity',
            'Percent of adults who report consuming fruit less than one time daily',
            'Percent of adults who report consuming vegetables less than one time daily'
        ]

        self.questions_best_is_max = [
            (
            "Percent of adults who achieve at least 150 minutes a week of "
            "moderate-intensity aerobic physical activity or 75 minutes a week of "
            "vigorous-intensity aerobic activity (or an equivalent combination)"
            ),
            (
                "Percent of adults who achieve at least 150 minutes a week of "
                "moderate-intensity aerobic physical activity or 75 minutes a week of "
                "vigorous-intensity aerobic physical activity and engage in "
                "muscle-strengthening activities on 2 or more days a week"
            ),
            (
                "Percent of adults who achieve at least 300 minutes a week of "
                "moderate-intensity aerobic physical activity or 150 minutes a week of "
                "vigorous-intensity aerobic activity (or an equivalent combination)"
            ),
            (
                "Percent of adults who engage in muscle-strengthening activities on 2 "
                "or more days a week"
            ),
        ]

    def states_mean(self, question):
        """
        calculate mean values for all states for a question.
        
        args:
            question: health metric to analyze
            
        returns:
            dict: state names and mean values
        """
        logger.info("Calculating states mean for question: %s", question)
        filtered_df = self.df[self.df['Question'] == question]
        logger.info("Filtered data has %d rows", len(filtered_df))

        state_means = filtered_df.groupby('LocationDesc')['Data_Value'].mean()

        # sort based on whether low or high values are better
        ascending = question in self.questions_best_is_min
        logger.info("Sorting in ascending order: %s", ascending)

        state_means = state_means.sort_values(ascending=ascending)

        result = {}
        for state, mean_value in state_means.items():
            result[state] = mean_value

        logger.info("Computed means for %d states", len(result))
        return result

    def best5(self, question):
        """
        find the top 5 performing states.
        
        args:
            question: health metric to analyze
            
        returns:
            dict: top 5 states and values
        """
        logger.info("Finding best 5 states for question: %s", question)
        filtered_df = self.df[self.df['Question'] == question]
        state_means = filtered_df.groupby('LocationDesc')['Data_Value'].mean()

        is_min_better = question in self.questions_best_is_min
        logger.info("Lower value is better: %s", is_min_better)

        # sort based on whether lower or higher is better
        if is_min_better:
            state_means = state_means.sort_values(ascending=True)
        else:
            state_means = state_means.sort_values(ascending=False)

        best_states = state_means.head(5)

        result = {}
        for state, mean_value in best_states.items():
            result[state] = mean_value

        logger.info("Found best 5 states: %s", list(result.keys()))
        return result

    def worst5(self, question):
        """
        find the 5 worst performing states.
        
        args:
            question: health metric to analyze
            
        returns:
            dict: worst 5 states and values
        """
        logger.info("Finding worst 5 states for question: %s", question)
        filtered_df = self.df[self.df['Question'] == question]
        state_means = filtered_df.groupby('LocationDesc')['Data_Value'].mean()

        is_min_better = question in self.questions_best_is_min
        logger.info("Lower value is better: %s", is_min_better)

        # invert sorting from best5
        if is_min_better:
            state_means = state_means.sort_values(ascending=False)
        else:
            state_means = state_means.sort_values(ascending=True)

        worst_states = state_means.head(5)

        result = {}
        for state, mean_value in worst_states.items():
            result[state] = mean_value

        logger.info("Found worst 5 states: %s", list(result.keys()))
        return result

    def global_mean(self, question):
        """
        calculate national average.
        
        args:
            question: health metric to analyze
            
        returns:
            dict: global mean value
        """
        logger.info("Calculating global mean for question: %s", question)
        filtered_df = self.df[self.df['Question'] == question]
        mean_value = filtered_df['Data_Value'].mean()
        logger.info("Global mean calculated: %s", mean_value)
        return {"global_mean": mean_value}

    def state_mean_by_category(self, question, state):
        """
        calculate means by demographic categories for a state.
        
        args:
            question: health metric to analyze
            state: state to analyze
            
        returns:
            dict: state and category means
        """
        logger.info("Calculating mean by category for state %s, question: %s", state, question)
        filtered_df = self.df[(self.df['Question'] == question) &
                              (self.df['LocationDesc'] == state)]
        logger.info("Filtered data has %d rows", len(filtered_df))

        # group by category and stratification
        mean_df = filtered_df.groupby(
            ['StratificationCategory1', 'Stratification1']
            )['Data_Value'].mean().reset_index()

        ans = {}
        for _, row in mean_df.iterrows():
            category = row['StratificationCategory1']
            stratification = row['Stratification1']

            if pd.notna(category) and pd.notna(stratification):
                key = f"('{category}', '{stratification}')"
                ans[key] = row['Data_Value']

        logger.info("Found %d category means", len(ans))
        return {state: ans}

    def state_mean(self, question, state):
        """
        calculate mean value for a state.
        
        args:
            question: health metric to analyze
            state: state to analyze
            
        returns:
            dict: state and mean value
        """
        logger.info("Calculating mean for state %s, question: %s", state, question)
        filtered_df = self.df[
            (self.df['Question'] == question) & (self.df['LocationDesc'] == state)
            ]

        if filtered_df.empty:
            logger.warning("No data found for state %s and question %s", state, question)
            return {}

        mean_value = filtered_df['Data_Value'].mean()
        logger.info("Mean calculated: %s", mean_value)
        return {state: mean_value}

    def diff_from_mean(self, question):
        """
        calculate how states differ from national average.
        
        args:
            question: health metric to analyze
            
        returns:
            dict: states with differences from mean
        """
        logger.info("Calculating difference from mean for question: %s", question)
        filtered_df = self.df[self.df['Question'] == question]
        global_mean = filtered_df['Data_Value'].mean()
        logger.info("Global mean: %s", global_mean)

        state_means = filtered_df.groupby('LocationDesc')['Data_Value'].mean()

        # global mean minus state mean
        diff_dict = {}
        for state, mean_value in state_means.items():
            diff_dict[state] = global_mean - mean_value

        # sort by difference
        sorted_diff = dict(sorted(diff_dict.items(), key=lambda x: x[1], reverse=True))
        logger.info("Computed differences for %d states", len(sorted_diff))
        return sorted_diff

    def state_diff_from_mean(self, question, state):
        """
        calculate how a state differs from national average.
        
        args:
            question: health metric to analyze
            state: state to analyze
            
        returns:
            dict: state and difference from mean
        """
        logger.info("Calculating difference from mean for state %s, question: %s", state, question)
        filtered_df = self.df[self.df['Question'] == question]
        global_mean = filtered_df['Data_Value'].mean()
        logger.info("Global mean: %s", global_mean)

        state_filtered_df = filtered_df[filtered_df['LocationDesc'] == state]
        if state_filtered_df.empty:
            logger.warning("No data found for state %s", state)
            return {}

        state_mean = state_filtered_df['Data_Value'].mean()
        logger.info("State mean: %s", state_mean)

        # global minus state difference
        diff_value = global_mean - state_mean
        logger.info("Difference: %s", diff_value)

        return {state: diff_value}

    def mean_by_category(self, question):
        """
        calculate means by demographic categories for all states.
        
        args:
            question: health metric to analyze
            
        returns:
            dict: means by state, category, and stratification
        """
        logger.info("Calculating mean by category for all states, question: %s", question)
        filtered_df = self.df[self.df['Question'] == question]

        result = {}

        # group by state and demographic data
        for (state, category, stratification), group_df in filtered_df.groupby(
            ['LocationDesc', 'StratificationCategory1', 'Stratification1']):

            if pd.notna(category) and pd.notna(stratification):
                mean_value = group_df['Data_Value'].mean()

                # format key as tuple string
                key = f"('{state}', '{category}', '{stratification}')"
                result[key] = mean_value

        logger.info("Computed %d category means across all states", len(result))
        return result
