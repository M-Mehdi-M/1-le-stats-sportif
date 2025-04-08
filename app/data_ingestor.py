"""
Module for processing and analyzing nutrition and obesity data from CSV files.
Provides functions to calculate various statistics for states and categories.
"""
import pandas as pd

class DataIngestor:
    def __init__(self, csv_path: str):
        # Read csv from csv_path
        self.df = pd.read_csv(csv_path)

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
        filtered_df = self.df[self.df['Question'] == question]

        state_means = filtered_df.groupby('LocationDesc')['Data_Value'].mean()

        ascending = question in self.questions_best_is_min

        state_means = state_means.sort_values(ascending=ascending)

        result = {}
        for state, mean_value in state_means.items():
            result[state] = mean_value

        return result

    def best5(self, question):
        filtered_df = self.df[self.df['Question'] == question]
        state_means = filtered_df.groupby('LocationDesc')['Data_Value'].mean()

        is_min_better = question in self.questions_best_is_min

        if is_min_better:
            state_means = state_means.sort_values(ascending=True)
        else:
            state_means = state_means.sort_values(ascending=False)

        best_states = state_means.head(5)

        result = {}
        for state, mean_value in best_states.items():
            result[state] = mean_value

        return result

    def worst5(self, question):
        filtered_df = self.df[self.df['Question'] == question]
        state_means = filtered_df.groupby('LocationDesc')['Data_Value'].mean()

        is_min_better = question in self.questions_best_is_min

        if is_min_better:
            state_means = state_means.sort_values(ascending=False)
        else:
            state_means = state_means.sort_values(ascending=True)

        worst_states = state_means.head(5)

        result = {}
        for state, mean_value in worst_states.items():
            result[state] = mean_value

        return result

    def global_mean(self, question):
        filtered_df = self.df[self.df['Question'] == question]
        mean_value = filtered_df['Data_Value'].mean()
        return {"global_mean": mean_value}

    def state_mean_by_category(self, question, state):
        filtered_df = self.df[(self.df['Question'] == question) &
                              (self.df['LocationDesc'] == state)]

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

        return {state: ans}

    def state_mean(self, question, state):
        filtered_df = self.df[
            (self.df['Question'] == question) & (self.df['LocationDesc'] == state)
            ]
        if filtered_df.empty:
            return {}

        mean_value = filtered_df['Data_Value'].mean()
        return {state: mean_value}

    def diff_from_mean(self, question):
        filtered_df = self.df[self.df['Question'] == question]
        global_mean = filtered_df['Data_Value'].mean()

        state_means = filtered_df.groupby('LocationDesc')['Data_Value'].mean()

        diff_dict = {}
        for state, mean_value in state_means.items():
            diff_dict[state] = global_mean - mean_value

        sorted_diff = dict(sorted(diff_dict.items(), key=lambda x: x[1], reverse=True))

        return sorted_diff

    def state_diff_from_mean(self, question, state):
        filtered_df = self.df[self.df['Question'] == question]
        global_mean = filtered_df['Data_Value'].mean()

        state_filtered_df = filtered_df[filtered_df['LocationDesc'] == state]
        if state_filtered_df.empty:
            return {}

        state_mean = state_filtered_df['Data_Value'].mean()

        diff_value = global_mean - state_mean

        return {state: diff_value}

    def mean_by_category(self, question):
        filtered_df = self.df[self.df['Question'] == question]

        result = {}

        for (state, category, stratification), group_df in filtered_df.groupby(
            ['LocationDesc', 'StratificationCategory1', 'Stratification1']):

            if pd.notna(category) and pd.notna(stratification):

                mean_value = group_df['Data_Value'].mean()

                key = f"('{state}', '{category}', '{stratification}')"
                result[key] = mean_value

        return result
