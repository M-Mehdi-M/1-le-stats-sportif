"""
Unit tests for the webserver Flask application.
Tests all routes and calculations in the data_ingestor module.
"""
import os
import sys
import json
import unittest
import pandas as pd
from unittest.mock import patch, Mock, MagicMock

# add the parent directory to sys.path to allow imports from app
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app import webserver
from app.data_ingestor import DataIngestor

class TestWebserver(unittest.TestCase):
    """
    test class for testing routes and calculations in the Flask application.
    """

    def setUp(self):
        """set up test client and test data."""
        # configure Flask for testing
        webserver.testing = True
        self.app = webserver.test_client()
        
        # create test CSV data
        self.test_csv_data = """YearStart,YearEnd,LocationAbbr,LocationDesc,DataSource,Topic,Question,Data_Value_Unit,Data_Value_Type,Data_Value,Data_Value_Alt,Data_Value_Footnote_Symbol,Data_Value_Footnote,Total,Age(years),Education,Gender,Income,Race/Ethnicity,GeoLocation,ClassID,TopicID,QuestionID,DataValueTypeID,LocationID,StratificationCategory1,Stratification1,StratificationCategoryId1,StratificationID1
2017,2017,AL,Alabama,BRFSS,Obesity / Weight Status,"Percent of adults aged 18 years and older who have obesity",Percent,Crude Prevalence,36.3,36.3,,,2453,,,,,,,Obesity / Weight Status,OWS,Q036,CRDPREV,1,,,,
2017,2017,AL,Alabama,BRFSS,Obesity / Weight Status,"Percent of adults aged 18 years and older who have obesity",Percent,Crude Prevalence,34.1,34.1,,,1530,,,"Female",,,ClassID,OWS,Q036,CRDPREV,1,Gender,Female,GEN,GENF
2017,2017,AL,Alabama,BRFSS,Obesity / Weight Status,"Percent of adults aged 18 years and older who have obesity",Percent,Crude Prevalence,38.7,38.7,,,923,,,"Male",,,ClassID,OWS,Q036,CRDPREV,1,Gender,Male,GEN,GENM
2017,2017,AK,Alaska,BRFSS,Obesity / Weight Status,"Percent of adults aged 18 years and older who have obesity",Percent,Crude Prevalence,34.2,34.2,,,2390,,,,,,,Obesity / Weight Status,OWS,Q036,CRDPREV,2,,,,
2017,2017,AK,Alaska,BRFSS,Obesity / Weight Status,"Percent of adults aged 18 years and older who have obesity",Percent,Crude Prevalence,29.4,29.4,,,1272,,,"Female",,,ClassID,OWS,Q036,CRDPREV,2,Gender,Female,GEN,GENF
2017,2017,AK,Alaska,BRFSS,Obesity / Weight Status,"Percent of adults aged 18 years and older who have obesity",Percent,Crude Prevalence,38.7,38.7,,,1118,,,"Male",,,ClassID,OWS,Q036,CRDPREV,2,Gender,Male,GEN,GENM
2017,2017,AL,Alabama,BRFSS,Physical Activity,"Percent of adults who achieve at least 150 minutes a week of moderate-intensity aerobic physical activity or 75 minutes a week of vigorous-intensity aerobic activity (or an equivalent combination)",Percent,Crude Prevalence,45.2,45.2,,,2453,,,,,,,ClassID,PA,Q039,CRDPREV,1,,,,
2017,2017,AL,Alabama,BRFSS,Physical Activity,"Percent of adults who achieve at least 150 minutes a week of moderate-intensity aerobic physical activity or 75 minutes a week of vigorous-intensity aerobic activity (or an equivalent combination)",Percent,Crude Prevalence,40.5,40.5,,,1530,,,"Female",,,ClassID,PA,Q039,CRDPREV,1,Gender,Female,GEN,GENF
2017,2017,AK,Alaska,BRFSS,Physical Activity,"Percent of adults who achieve at least 150 minutes a week of moderate-intensity aerobic physical activity or 75 minutes a week of vigorous-intensity aerobic activity (or an equivalent combination)",Percent,Crude Prevalence,53.3,53.3,,,2390,,,,,,,ClassID,PA,Q039,CRDPREV,2,,,,
2017,2017,AK,Alaska,BRFSS,Physical Activity,"Percent of adults who achieve at least 150 minutes a week of moderate-intensity aerobic physical activity or 75 minutes a week of vigorous-intensity aerobic activity (or an equivalent combination)",Percent,Crude Prevalence,50.2,50.2,,,1272,,,"Female",,,ClassID,PA,Q039,CRDPREV,2,Gender,Female,GEN,GENF
"""
        # save test CSV data to a file
        self.test_csv_path = os.path.join(os.path.dirname(__file__), 'test_data.csv')
        with open(self.test_csv_path, 'w') as f:
            f.write(self.test_csv_data)
        
        # create a test instance of DataIngestor using our test data
        self.data_ingestor = DataIngestor(self.test_csv_path)
        
        # create a mock ThreadPool that doesnt actually shut down
        self.mock_thread_pool = Mock()
        self.mock_thread_pool.is_shutting_down.return_value = False
        self.mock_thread_pool.add_job = self.mock_add_job
        
        # store original attributes
        self.original_data_ingestor = webserver.data_ingestor
        self.original_thread_pool = webserver.tasks_runner
        
        # inject our mocks
        webserver.data_ingestor = self.data_ingestor
        webserver.tasks_runner = self.mock_thread_pool
        
        # reset job counter
        webserver.job_counter = 1
        
        # make sure the results directory exists
        if not os.path.exists('results'):
            os.makedirs('results')

    def mock_add_job(self, job_id, job_func):
        """mock implementation of add_job that executes the job synchronously"""
        # execute the job function immediately
        result = job_func()
        
        # write result to file
        with open(f'results/{job_id}', 'w', encoding='utf-8') as f:
            json.dump(result, f)
        
        return job_id

    def tearDown(self):
        """clean up after tests."""
        # restore original attributes
        webserver.data_ingestor = self.original_data_ingestor
        webserver.tasks_runner = self.original_thread_pool
        
        # clean up test file
        if os.path.exists(self.test_csv_path):
            os.remove(self.test_csv_path)
        
        # clean up result files created during the test
        for i in range(1, 30):  # Assuming we wont create more than 30 jobs in tests
            job_path = f'results/job_id_{i}'
            if os.path.exists(job_path):
                try:
                    os.remove(job_path)
                except:
                    pass

    def _get_job_result(self, response):
        """
        helper method to get result from a job.
        
        args:
            response: The response from the API call
            
        returns:
            dict: The job result data
        """
        # get job ID
        job_data = json.loads(response.data)
        self.assertEqual(job_data['status'], 'accepted')
        job_id = job_data['job_id']
        
        # get job result
        result_response = self.app.get(f'/api/get_results/{job_id}')
        result_data = json.loads(result_response.data)
        
        # the job should be done since we execute synchronously in our mock
        self.assertEqual(result_data['status'], 'done')
        
        return result_data['data']

    def test_index_route(self):
        """test the index route."""
        response = self.app.get('/')
        self.assertEqual(response.status_code, 200)
        
        # check that the response contains expected text
        self.assertIn(b'Hello, World!', response.data)

    def test_states_mean(self):
        """test states_mean endpoint and calculation."""
        # test obesity question
        obesity_question = "Percent of adults aged 18 years and older who have obesity"
        response = self.app.post('/api/states_mean', 
                                json={'question': obesity_question})
        self.assertEqual(response.status_code, 200)
        
        result = self._get_job_result(response)
        self.assertIn('Alabama', result)
        self.assertIn('Alaska', result)
        
        # expected results from our test data
        # Alabama: avg of 36.3, 34.1, 38.7 = 36.366666666666667
        # Alaska: avg of 34.2, 29.4, 38.7 = 34.1
        self.assertAlmostEqual(result['Alabama'], 36.366666666666667, places=2)
        self.assertAlmostEqual(result['Alaska'], 34.1, places=2)
        
        # since were seeing Alabama first in the test results lets verify the values
        # rather than assuming the order
        alabama_value = result['Alabama']
        alaska_value = result['Alaska']
        self.assertLess(alaska_value, alabama_value, "Alaska should have a lower obesity value than Alabama")

    def test_state_mean(self):
        """test state_mean endpoint and calculation."""
        # test obesity question for Alabama
        obesity_question = "Percent of adults aged 18 years and older who have obesity"
        response = self.app.post('/api/state_mean', 
                                json={'question': obesity_question, 'state': 'Alabama'})
        self.assertEqual(response.status_code, 200)
        
        result = self._get_job_result(response)
        self.assertIn('Alabama', result)
        
        # txpected result from our test data
        # Alabama: avg of 36.3, 34.1, 38.7 = 36.366666666666667
        self.assertAlmostEqual(result['Alabama'], 36.366666666666667, places=2)
        
        # test with a state not in the data
        response = self.app.post('/api/state_mean', 
                                json={'question': obesity_question, 'state': 'California'})
        self.assertEqual(response.status_code, 200)
        
        result = self._get_job_result(response)
        self.assertEqual(result, {})  # should return empty dict for nonexistent state

    def test_best5(self):
        """test best5 endpoint and calculation."""
        # test obesity question
        obesity_question = "Percent of adults aged 18 years and older who have obesity"
        response = self.app.post('/api/best5', 
                                json={'question': obesity_question})
        self.assertEqual(response.status_code, 200)
        
        result = self._get_job_result(response)
        
        # we only have 2 states in test data so both should be included
        self.assertEqual(len(result), 2)
        self.assertIn('Alaska', result)
        self.assertIn('Alabama', result)
        
        # verify based on values rather than positions
        alabama_value = result['Alabama']
        alaska_value = result['Alaska'] 
        self.assertLess(alaska_value, alabama_value, "Alaska should have a lower obesity value than Alabama")
        
        # test physical activity question
        activity_question = "Percent of adults who achieve at least 150 minutes a week of moderate-intensity aerobic physical activity or 75 minutes a week of vigorous-intensity aerobic activity (or an equivalent combination)"
        response = self.app.post('/api/best5', 
                                json={'question': activity_question})
        self.assertEqual(response.status_code, 200)
        
        result = self._get_job_result(response)
        
        # verify based on values rather than positions
        alabama_value = result.get('Alabama', 0)
        alaska_value = result.get('Alaska', 0)
        self.assertGreater(alaska_value, alabama_value, "Alaska should have a higher activity value than Alabama")

    def test_worst5(self):
        """test worst5 endpoint and calculation."""
        # test obesity question
        obesity_question = "Percent of adults aged 18 years and older who have obesity"
        response = self.app.post('/api/worst5', 
                                json={'question': obesity_question})
        self.assertEqual(response.status_code, 200)
        
        result = self._get_job_result(response)
        
        # verify based on values rather than positions
        alabama_value = result['Alabama']
        alaska_value = result['Alaska']
        self.assertGreater(alabama_value, alaska_value, "Alabama should have a higher obesity value than Alaska")
        
        # test physical activity question
        activity_question = "Percent of adults who achieve at least 150 minutes a week of moderate-intensity aerobic physical activity or 75 minutes a week of vigorous-intensity aerobic activity (or an equivalent combination)"
        response = self.app.post('/api/worst5', 
                                json={'question': activity_question})
        self.assertEqual(response.status_code, 200)
        
        result = self._get_job_result(response)
        
        # verify based on values rather than positions
        alabama_value = result.get('Alabama', 0)
        alaska_value = result.get('Alaska', 0)
        self.assertLess(alabama_value, alaska_value, "Alabama should have a lower activity value than Alaska")

    def test_global_mean(self):
        """test global_mean endpoint and calculation."""
        # test obesity question
        obesity_question = "Percent of adults aged 18 years and older who have obesity"
        response = self.app.post('/api/global_mean', 
                                json={'question': obesity_question})
        self.assertEqual(response.status_code, 200)
        
        result = self._get_job_result(response)
        self.assertIn('global_mean', result)
        
        # expected result from our test data
        # average of all obesity data points: (36.3 + 34.1 + 38.7 + 34.2 + 29.4 + 38.7)/6 = 35.23333333333333
        self.assertAlmostEqual(result['global_mean'], 35.23333333333333, places=2)

    def test_diff_from_mean(self):
        """test diff_from_mean endpoint and calculation."""
        # test obesity question
        obesity_question = "Percent of adults aged 18 years and older who have obesity"
        response = self.app.post('/api/diff_from_mean', 
                                json={'question': obesity_question})
        self.assertEqual(response.status_code, 200)
        
        result = self._get_job_result(response)
        self.assertIn('Alabama', result)
        self.assertIn('Alaska', result)
        
        # expected results from our test data
        # global mean: 35.23333333333333
        # Alabama mean: 36.366666666666667
        # Alaska mean: 34.1
        # diff for Alabama: 35.23333333333333 - 36.366666666666667 = -1.1333333333333258
        # diff for Alaska: 35.23333333333333 - 34.1 = 1.1333333333333258
        
        self.assertAlmostEqual(result['Alabama'], -1.1333333333333258, places=2)
        self.assertAlmostEqual(result['Alaska'], 1.1333333333333258, places=2)
        
        # check values - Alaska should have positive difference Alabama negative
        self.assertGreater(result['Alaska'], 0)
        self.assertLess(result['Alabama'], 0)

    def test_state_diff_from_mean(self):
        """test state_diff_from_mean endpoint and calculation."""
        # test obesity question for Alaska
        obesity_question = "Percent of adults aged 18 years and older who have obesity"
        response = self.app.post('/api/state_diff_from_mean', 
                                json={'question': obesity_question, 'state': 'Alaska'})
        self.assertEqual(response.status_code, 200)
        
        result = self._get_job_result(response)
        self.assertIn('Alaska', result)
        
        # expected result from our test data
        # global mean: 35.23333333333333
        # Alaska mean: 34.1
        # diff for Alaska: 35.23333333333333 - 34.1 = 1.1333333333333258
        self.assertAlmostEqual(result['Alaska'], 1.1333333333333258, places=2)
        
        # test with a state not in the data
        response = self.app.post('/api/state_diff_from_mean', 
                                json={'question': obesity_question, 'state': 'California'})
        self.assertEqual(response.status_code, 200)
        
        result = self._get_job_result(response)
        self.assertEqual(result, {})  # should return empty dict for nonexistent state

    def test_mean_by_category(self):
        """test mean_by_category endpoint and calculation."""
        # test obesity question
        obesity_question = "Percent of adults aged 18 years and older who have obesity"
        response = self.app.post('/api/mean_by_category', 
                                json={'question': obesity_question})
        self.assertEqual(response.status_code, 200)
        
        result = self._get_job_result(response)
        
        # verify that we have the expected values for each combination
        # first find the keys for each state-gender combination
        female_alabama_key = None
        male_alabama_key = None
        female_alaska_key = None
        male_alaska_key = None
        
        for key in result.keys():
            if "Alabama" in key and "Female" in key:
                female_alabama_key = key
            elif "Alabama" in key and "Male" in key:
                male_alabama_key = key
            elif "Alaska" in key and "Female" in key:
                female_alaska_key = key
            elif "Alaska" in key and "Male" in key:
                male_alaska_key = key
        
        # make sure we found all keys
        self.assertIsNotNone(female_alabama_key, "Could not find key for Alabama Female")
        self.assertIsNotNone(male_alabama_key, "Could not find key for Alabama Male")
        self.assertIsNotNone(female_alaska_key, "Could not find key for Alaska Female")
        self.assertIsNotNone(male_alaska_key, "Could not find key for Alaska Male")
        
        # verify the values
        self.assertEqual(result[female_alabama_key], 34.1)
        self.assertEqual(result[male_alabama_key], 38.7)
        self.assertEqual(result[female_alaska_key], 29.4)
        self.assertEqual(result[male_alaska_key], 38.7)

    def test_state_mean_by_category(self):
        """test state_mean_by_category endpoint and calculation."""
        # test obesity question for Alabama
        obesity_question = "Percent of adults aged 18 years and older who have obesity"
        response = self.app.post('/api/state_mean_by_category', 
                                json={'question': obesity_question, 'state': 'Alabama'})
        self.assertEqual(response.status_code, 200)
        
        result = self._get_job_result(response)
        self.assertIn('Alabama', result)
        
        # find the keys for Female and Male 
        female_key = None
        male_key = None
        
        for key in result['Alabama'].keys():
            if "Female" in key:
                female_key = key
            elif "Male" in key:
                male_key = key
        
        # make sure we found all keys
        self.assertIsNotNone(female_key, "Could not find key for Female")
        self.assertIsNotNone(male_key, "Could not find key for Male")
        
        # verify the values
        self.assertEqual(result['Alabama'][female_key], 34.1)
        self.assertEqual(result['Alabama'][male_key], 38.7)

    def test_graceful_shutdown(self):
        """test graceful_shutdown endpoint."""
        # set up the mocks for graceful_shutdown
        self.mock_thread_pool.is_queue_empty.return_value = True
        
        response = self.app.get('/api/graceful_shutdown')
        self.assertEqual(response.status_code, 200)
        result = json.loads(response.data)
        self.assertEqual(result['status'], 'done')
        
        # now test the case where queue is not empty
        self.mock_thread_pool.is_queue_empty.return_value = False
        
        response = self.app.get('/api/graceful_shutdown')
        self.assertEqual(response.status_code, 200)
        result = json.loads(response.data)
        self.assertEqual(result['status'], 'running')

    def test_jobs(self):
        """test jobs endpoint."""
        # mock get_all_jobs to return a specific value
        mock_jobs = [{'job_id_1': 'done'}, {'job_id_2': 'running'}]
        self.mock_thread_pool.get_all_jobs.return_value = mock_jobs
        
        response = self.app.get('/api/jobs')
        self.assertEqual(response.status_code, 200)
        result = json.loads(response.data)
        self.assertEqual(result['status'], 'done')
        self.assertEqual(result['data'], mock_jobs)

    def test_num_jobs(self):
        """test num_jobs endpoint."""
        # mock pending_jobs_count to return a specific value
        self.mock_thread_pool.pending_jobs_count.return_value = 5
        
        response = self.app.get('/api/num_jobs')
        self.assertEqual(response.status_code, 200)
        result = json.loads(response.data)
        self.assertEqual(result['status'], 'done')
        self.assertEqual(result['data'], 5)

    def test_error_handling(self):
        """test error handling in endpoints."""
        # missing question parameter
        response = self.app.post('/api/states_mean', json={})
        self.assertEqual(response.status_code, 200)
        result = json.loads(response.data)
        self.assertEqual(result['status'], 'error')
        
        # missing state parameter
        response = self.app.post('/api/state_mean', 
                                json={'question': 'test'})
        self.assertEqual(response.status_code, 200)
        result = json.loads(response.data)
        self.assertEqual(result['status'], 'error')
        
        # invalid job ID
        response = self.app.get('/api/get_results/invalid_job_id')
        self.assertEqual(response.status_code, 200)
        result = json.loads(response.data)
        self.assertEqual(result['status'], 'error')
    
    def test_data_ingestor_direct(self):
        """test DataIngestor functions directly."""
        # test states_mean
        obesity_question = "Percent of adults aged 18 years and older who have obesity"
        result = self.data_ingestor.states_mean(obesity_question)
        self.assertAlmostEqual(result['Alaska'], 34.1, places=2)
        
        # test best5
        result = self.data_ingestor.best5(obesity_question)
        self.assertEqual(len(result), 2)  # only 2 states in test data
        
        # verify based on values rather than positions
        alabama_value = result['Alabama']
        alaska_value = result['Alaska']
        self.assertLess(alaska_value, alabama_value, "Alaska should have a lower obesity value than Alabama")
        
        # test global_mean
        result = self.data_ingestor.global_mean(obesity_question)
        self.assertAlmostEqual(result['global_mean'], 35.23333333333333, places=2)
        
        # test diff_from_mean
        result = self.data_ingestor.diff_from_mean(obesity_question)
        self.assertAlmostEqual(result['Alaska'], 1.1333333333333258, places=2)
        self.assertAlmostEqual(result['Alabama'], -1.1333333333333258, places=2)
        
        # test state_mean
        result = self.data_ingestor.state_mean(obesity_question, 'Alabama')
        self.assertAlmostEqual(result['Alabama'], 36.366666666666667, places=2)
        
        # test state_mean with nonexistent state
        result = self.data_ingestor.state_mean(obesity_question, 'California')
        self.assertEqual(result, {})
        
        # test worst5
        activity_question = "Percent of adults who achieve at least 150 minutes a week of moderate-intensity aerobic physical activity or 75 minutes a week of vigorous-intensity aerobic activity (or an equivalent combination)"
        result = self.data_ingestor.worst5(activity_question)
        
        # verify based on values rather than positions
        alabama_value = result.get('Alabama', 0)
        alaska_value = result.get('Alaska', 0)
        self.assertLess(alabama_value, alaska_value, "Alabama should have a lower activity value than Alaska")


if __name__ == '__main__':
    unittest.main()
