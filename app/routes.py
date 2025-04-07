from app import webserver
from flask import request, jsonify

import os
import json

# helper function to register a job and return job_id
def register_job(job_func):
    job_id = f"job_id_{webserver.job_counter}"
    webserver.job_counter += 1

    # check if server is shutting down
    if webserver.tasks_runner.is_shutting_down():
        return jsonify({
            "status": "error",
            "reason": "shutting down"
        }), 503

    # register job with the thread pool
    webserver.tasks_runner.add_job(job_id, job_func)

    return jsonify({
        "status": "accepted",
        "job_id": job_id
    })

@webserver.route('/api/get_results/<job_id>', methods=['GET'])
def get_response(job_id):
    # Check if job_id is valid
    status = webserver.tasks_runner.get_job_status(job_id)

    if status is None:
        return jsonify({
            'status': 'error',
            'reason': 'Invalid job_id'
        })

    if status == 'running':
        return jsonify({
            'status': 'running'
        })

    # job is done load and return the result
    try:
        result_file_path = f'results/{job_id}'
        if not os.path.exists(result_file_path):
            return jsonify({
                'status': 'error',
                'reason': f'Result file not found for job {job_id}'
            })

        with open(result_file_path, 'r') as f:
            file_content = f.read().strip()
            if not file_content:
                return jsonify({
                    'status': 'error',
                    'reason': f'Empty result file for job {job_id}'
                })

            result = json.loads(file_content)

        # check if the result contains an error
        if isinstance(result, dict) and 'error' in result:
            return jsonify({
                'status': 'error',
                'reason': f'Job execution failed: {result["error"]}'
            })

        return jsonify({
            'status': 'done',
            'data': result
        })

    except Exception as e:
        print(f"Error retrieving result for job {job_id}: {str(e)}")
        return jsonify({
            'status': 'error',
            'reason': f'Failed to read result: {str(e)}'
        })

@webserver.route('/api/states_mean', methods=['POST'])
def states_mean_request():
    # Get request data
    data = request.json
    print(f"Got request {data}")

    # Register job. Don't wait for task to finish
    # Increment job_id counter
    # Return associated job_id

    # validate request data
    if 'question' not in data:
        return jsonify({
            'status': 'error',
            'reason': 'Missing question parameter'
        })

    question = data['question']

    # define the job function that will be executed by the thread pool
    def job_func():
        return webserver.data_ingestor.states_mean(question)

    # register job and return job_id
    return register_job(job_func)

@webserver.route('/api/state_mean', methods=['POST'])
def state_mean_request():
    # Get request data
    # Register job. Don't wait for task to finish
    # Increment job_id counter
    # Return associated job_id

    # get request data
    data = request.json

    # validate request data
    if 'question' not in data or 'state' not in data:
        return jsonify({
            'status': 'error',
            'reason': 'Missing question or state parameter'
        })

    question = data['question']
    state = data['state']

    # define the job function
    def job_func():
        return webserver.data_ingestor.state_mean(question, state)

    # register job and return job_id
    return register_job(job_func)

@webserver.route('/api/best5', methods=['POST'])
def best5_request():
    # Get request data
    # Register job. Don't wait for task to finish
    # Increment job_id counter
    # Return associated job_id

    # get request data
    data = request.json

    # validate request data
    if 'question' not in data:
        return jsonify({
            'status': 'error',
            'reason': 'Missing question parameter'
        })

    question = data['question']

    # define the job function
    def job_func():
        return webserver.data_ingestor.best5(question)

    # register job and return job_id
    return register_job(job_func)

@webserver.route('/api/worst5', methods=['POST'])
def worst5_request():
    # Get request data
    # Register job. Don't wait for task to finish
    # Increment job_id counter
    # Return associated job_id

    # get request data
    data = request.json

    # validate request data
    if 'question' not in data:
        return jsonify({
            'status': 'error',
            'reason': 'Missing question parameter'
        })

    question = data['question']
    
    # define the job function
    def job_func():
        return webserver.data_ingestor.worst5(question)

    # register job and return job_id
    return register_job(job_func)

@webserver.route('/api/global_mean', methods=['POST'])
def global_mean_request():
    # Get request data
    # Register job. Don't wait for task to finish
    # Increment job_id counter
    # Return associated job_id

    # get request data
    data = request.json

    # validate request data
    if 'question' not in data:
        return jsonify({
            'status': 'error',
            'reason': 'Missing question parameter'
        })

    question = data['question']

    # define the job function
    def job_func():
        return webserver.data_ingestor.global_mean(question)

    # register job and return job_id
    return register_job(job_func)

@webserver.route('/api/diff_from_mean', methods=['POST'])
def diff_from_mean_request():
    # Get request data
    # Register job. Don't wait for task to finish
    # Increment job_id counter
    # Return associated job_id

    # get request data
    data = request.json

    # validate request data
    if 'question' not in data:
        return jsonify({
            'status': 'error',
            'reason': 'Missing question parameter'
        })

    question = data['question']

    # define the job function
    def job_func():
        return webserver.data_ingestor.diff_from_mean(question)

    # register job and return job_id
    return register_job(job_func)

@webserver.route('/api/state_diff_from_mean', methods=['POST'])
def state_diff_from_mean_request():
    # Get request data
    # Register job. Don't wait for task to finish
    # Increment job_id counter
    # Return associated job_id

    # get request data
    data = request.json

    # validate request data
    if 'question' not in data or 'state' not in data:
        return jsonify({
            'status': 'error',
            'reason': 'Missing question or state parameter'
        })

    question = data['question']
    state = data['state']

    # define the job function
    def job_func():
        return webserver.data_ingestor.state_diff_from_mean(question, state)

    # register job and return job_id
    return register_job(job_func)

@webserver.route('/api/mean_by_category', methods=['POST'])
def mean_by_category_request():
    # Get request data
    # Register job. Don't wait for task to finish
    # Increment job_id counter
    # Return associated job_id

    # get request data
    data = request.json

    # validate request data
    if 'question' not in data:
        return jsonify({
            'status': 'error',
            'reason': 'Missing question parameter'
        })

    question = data['question']

    # define the job function
    def job_func():
        return webserver.data_ingestor.mean_by_category(question)

    # register job and return job_id
    return register_job(job_func)

@webserver.route('/api/state_mean_by_category', methods=['POST'])
def state_mean_by_category_request():
    # Get request data
    # Register job. Don't wait for task to finish
    # Increment job_id counter
    # Return associated job_id

    # get request data
    data = request.json

    # validate request data
    if 'question' not in data or 'state' not in data:
        return jsonify({
            'status': 'error',
            'reason': 'Missing question or state parameter'
        })

    question = data['question']
    state = data['state']

    # define the job function
    def job_func():
        return webserver.data_ingestor.state_mean_by_category(question, state)

    # register job and return job_id
    return register_job(job_func)

@webserver.route('/api/graceful_shutdown', methods=['GET'])
def graceful_shutdown():
    # initiate graceful shutdown
    webserver.tasks_runner.graceful_shutdown()

    # check if there are still jobs in the queue
    if webserver.tasks_runner.is_queue_empty():
        return jsonify({
            "status": "done"
        })
    else:
        return jsonify({
            "status": "running"
        })

@webserver.route('/api/jobs', methods=['GET'])
def jobs():
    # get all jobs and their status
    job_list = webserver.tasks_runner.get_all_jobs()

    return jsonify({
        "status": "done",
        "data": job_list
    })

@webserver.route('/api/num_jobs', methods=['GET'])
def num_jobs():
    # get the count of jobs remaining in the queue
    job_count = webserver.tasks_runner.pending_jobs_count()

    return jsonify({
        "status": "done",
        "data": job_count
    })

# You can check localhost in your browser to see what this displays
@webserver.route('/')
@webserver.route('/index')
def index():
    routes = get_defined_routes()
    msg = f"Hello, World!\n Interact with the webserver using one of the defined routes:\n"

    # Display each route as a separate HTML <p> tag
    paragraphs = ""
    for route in routes:
        paragraphs += f"<p>{route}</p>"

    msg += paragraphs
    return msg

def get_defined_routes():
    routes = []
    for rule in webserver.url_map.iter_rules():
        methods = ', '.join(rule.methods)
        routes.append(f"Endpoint: \"{rule}\" Methods: \"{methods}\"")
    return routes
