import pandas as pd
import numpy as np
import os
import json

class DataIngestor:
    def __init__(self, csv_path: str):
        # TODO: Read csv from csv_path
        self.df = pd.read_csv(csv_path)
        
        self.questions_best_is_min = [
            'Percent of adults aged 18 years and older who have an overweight classification',
            'Percent of adults aged 18 years and older who have obesity',
            'Percent of adults who engage in no leisure-time physical activity',
            'Percent of adults who report consuming fruit less than one time daily',
            'Percent of adults who report consuming vegetables less than one time daily'
        ]

        self.questions_best_is_max = [
            'Percent of adults who achieve at least 150 minutes a week of moderate-intensity aerobic physical activity or 75 minutes a week of vigorous-intensity aerobic activity (or an equivalent combination)',
            'Percent of adults who achieve at least 150 minutes a week of moderate-intensity aerobic physical activity or 75 minutes a week of vigorous-intensity aerobic physical activity and engage in muscle-strengthening activities on 2 or more days a week',
            'Percent of adults who achieve at least 300 minutes a week of moderate-intensity aerobic physical activity or 150 minutes a week of vigorous-intensity aerobic activity (or an equivalent combination)',
            'Percent of adults who engage in muscle-strengthening activities on 2 or more days a week',
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
        filtered_df = self.df[(self.df['Question'] == question) & (self.df['LocationDesc'] == state)]
        
        mean_df = filtered_df.groupby(['StratificationCategory1', 'Stratification1'])['Data_Value'].mean().reset_index()
        
        ans = {}
        for _, row in mean_df.iterrows():
            category = row['StratificationCategory1']
            stratification = row['Stratification1']
            
            if pd.notna(category) and pd.notna(stratification):
                key = f"('{category}', '{stratification}')"
                ans[key] = row['Data_Value']
        
        return {state: ans}
