"""
Module defining all api routes for the web server.
Provides endpoints for data retrieval, job management and server control.
"""
import os
import json
from threading import Lock

from flask import request, jsonify
from app import webserver

webserver.counter_lock = Lock()
logger = webserver.logger

def register_job(job_func):
    """
    helper function to register a job and return job_id.
    
    args:
        job_func: function to be executed by thread pool
        
    returns:
        response: json response with job id or error
    """
    with webserver.counter_lock:
        job_id = f"job_id_{webserver.job_counter}"
        webserver.job_counter += 1

        # check for shutdown mode
        if webserver.tasks_runner.is_shutting_down():
            logger.info("Rejecting job registration: server is shutting down")
            return jsonify({
                "status": "error",
                "reason": "shutting down"
            }), 503

        # register job with thread pool
        webserver.tasks_runner.add_job(job_id, job_func)
        logger.info("Registered job with ID: %s", job_id)

        return jsonify({
            "status": "accepted",
            "job_id": job_id
        })

@webserver.route('/api/get_results/<job_id>', methods=['GET'])
def get_response(job_id):
    """
    retrieve results for a specific job.
    
    args:
        job_id: id of the job to get results for
        
    returns:
        response: json with job results or error
    """
    logger.info("Enter get_response - Parameters: job_id=%s", job_id)

    # check job validity
    status = webserver.tasks_runner.get_job_status(job_id)

    if status is None:
        logger.info("Exit get_response - Invalid job_id: %s", job_id)
        return jsonify({
            'status': 'error',
            'reason': 'Invalid job_id'
        })

    if status == 'running':
        logger.info("Exit get_response - Job still running: %s", job_id)
        return jsonify({
            'status': 'running'
        })

    # load result from file
    try:
        result_file_path = f'results/{job_id}'
        if not os.path.exists(result_file_path):
            logger.error("Result file not found for job %s", job_id)
            return jsonify({
                'status': 'error',
                'reason': f'Result file not found for job {job_id}'
            })

        with open(result_file_path, 'r', encoding='utf-8') as f:
            file_content = f.read().strip()
            if not file_content:
                logger.error("Empty result file for job %s", job_id)
                return jsonify({
                    'status': 'error',
                    'reason': f'Empty result file for job {job_id}'
                })

            result = json.loads(file_content)

        # check for error in result
        if isinstance(result, dict) and 'error' in result:
            logger.error("Job execution failed: %s", result['error'])
            return jsonify({
                'status': 'error',
                'reason': f'Job execution failed: {result["error"]}'
            })

        logger.info("Exit get_response - Successfully retrieved results for job: %s", job_id)
        return jsonify({
            'status': 'done',
            'data': result
        })

    except Exception as e:
        logger.error("Failed to read result for job %s: %s", job_id, str(e))
        return jsonify({
            'status': 'error',
            'reason': f'Failed to read result: {str(e)}'
        })

@webserver.route('/api/states_mean', methods=['POST'])
def states_mean_request():
    """
    endpoint to calculate mean values for all states.
    
    returns:
        response: json with job id or error
    """
    logger.info("Enter states_mean_request")

    # get and validate request data
    data = request.json
    if 'question' not in data:
        logger.info("Exit states_mean_request - Missing question parameter")
        return jsonify({
            'status': 'error',
            'reason': 'Missing question parameter'
        })

    question = data['question']
    logger.info("Processing states_mean_request with question: %s", question)

    # define job function
    def job_func():
        return webserver.data_ingestor.states_mean(question)

    logger.info("Exit states_mean_request - Job registered")
    return register_job(job_func)

@webserver.route('/api/state_mean', methods=['POST'])
def state_mean_request():
    """
    endpoint to calculate mean for specific state.
    
    returns:
        response: json with job id or error
    """
    logger.info("Enter state_mean_request")

    # get and validate request data
    data = request.json
    if 'question' not in data or 'state' not in data:
        logger.info("Exit state_mean_request - Missing question or state parameter")
        return jsonify({
            'status': 'error',
            'reason': 'Missing question or state parameter'
        })

    question = data['question']
    state = data['state']
    logger.info("Processing state_mean_request with question: %s, state: %s", question, state)

    # define job function
    def job_func():
        return webserver.data_ingestor.state_mean(question, state)

    logger.info("Exit state_mean_request - Job registered")
    return register_job(job_func)

@webserver.route('/api/best5', methods=['POST'])
def best5_request():
    """
    endpoint to find top 5 performing states.
    
    returns:
        response: json with job id or error
    """
    logger.info("Enter best5_request")

    # get and validate request data
    data = request.json
    if 'question' not in data:
        logger.info("Exit best5_request - Missing question parameter")
        return jsonify({
            'status': 'error',
            'reason': 'Missing question parameter'
        })

    question = data['question']
    logger.info("Processing best5_request with question: %s", question)

    # define job function
    def job_func():
        return webserver.data_ingestor.best5(question)

    logger.info("Exit best5_request - Job registered")
    return register_job(job_func)

@webserver.route('/api/worst5', methods=['POST'])
def worst5_request():
    """
    endpoint to find 5 worst performing states.
    
    returns:
        response: json with job id or error
    """
    logger.info("Enter worst5_request")

    # get and validate request data
    data = request.json
    if 'question' not in data:
        logger.info("Exit worst5_request - Missing question parameter")
        return jsonify({
            'status': 'error',
            'reason': 'Missing question parameter'
        })

    question = data['question']
    logger.info("Processing worst5_request with question: %s", question)

    # define job function
    def job_func():
        return webserver.data_ingestor.worst5(question)

    logger.info("Exit worst5_request - Job registered")
    return register_job(job_func)

@webserver.route('/api/global_mean', methods=['POST'])
def global_mean_request():
    """
    endpoint to calculate national average.
    
    returns:
        response: json with job id or error
    """
    logger.info("Enter global_mean_request")

    # get and validate request data
    data = request.json
    if 'question' not in data:
        logger.info("Exit global_mean_request - Missing question parameter")
        return jsonify({
            'status': 'error',
            'reason': 'Missing question parameter'
        })

    question = data['question']
    logger.info("Processing global_mean_request with question: %s", question)

    # define job function
    def job_func():
        return webserver.data_ingestor.global_mean(question)

    logger.info("Exit global_mean_request - Job registered")
    return register_job(job_func)

@webserver.route('/api/diff_from_mean', methods=['POST'])
def diff_from_mean_request():
    """
    endpoint to calculate differences from national average for all states.
    
    returns:
        response: json with job id or error
    """
    logger.info("Enter diff_from_mean_request")

    # get and validate request data
    data = request.json
    if 'question' not in data:
        logger.info("Exit diff_from_mean_request - Missing question parameter")
        return jsonify({
            'status': 'error',
            'reason': 'Missing question parameter'
        })

    question = data['question']
    logger.info("Processing diff_from_mean_request with question: %s", question)

    # define job function
    def job_func():
        return webserver.data_ingestor.diff_from_mean(question)

    logger.info("Exit diff_from_mean_request - Job registered")
    return register_job(job_func)

@webserver.route('/api/state_diff_from_mean', methods=['POST'])
def state_diff_from_mean_request():
    """
    endpoint to calculate difference from national average for specific state.
    
    returns:
        response: json with job id or error
    """
    logger.info("Enter state_diff_from_mean_request")

    # get and validate request data
    data = request.json
    if 'question' not in data or 'state' not in data:
        logger.info("Exit state_diff_from_mean_request - Missing question or state parameter")
        return jsonify({
            'status': 'error',
            'reason': 'Missing question or state parameter'
        })

    question = data['question']
    state = data['state']
    logger.info("Processing state_diff_from_mean_request with question: %s, state: %s",
                 question, state)

    # define job function
    def job_func():
        return webserver.data_ingestor.state_diff_from_mean(question, state)

    logger.info("Exit state_diff_from_mean_request - Job registered")
    return register_job(job_func)

@webserver.route('/api/mean_by_category', methods=['POST'])
def mean_by_category_request():
    """
    endpoint to calculate means by demographic categories for all states.
    
    returns:
        response: json with job id or error
    """
    logger.info("Enter mean_by_category_request")

    # get and validate request data
    data = request.json
    if 'question' not in data:
        logger.info("Exit mean_by_category_request - Missing question parameter")
        return jsonify({
            'status': 'error',
            'reason': 'Missing question parameter'
        })

    question = data['question']
    logger.info("Processing mean_by_category_request with question: %s", question)

    # define job function
    def job_func():
        return webserver.data_ingestor.mean_by_category(question)

    logger.info("Exit mean_by_category_request - Job registered")
    return register_job(job_func)

@webserver.route('/api/state_mean_by_category', methods=['POST'])
def state_mean_by_category_request():
    """
    endpoint to calculate means by demographic categories for specific state.
    
    returns:
        response: json with job id or error
    """
    logger.info("Enter state_mean_by_category_request")

    # get and validate request data
    data = request.json
    if 'question' not in data or 'state' not in data:
        logger.info("Exit state_mean_by_category_request - Missing question or state parameter")
        return jsonify({
            'status': 'error',
            'reason': 'Missing question or state parameter'
        })

    question = data['question']
    state = data['state']
    logger.info("Processing state_mean_by_category_request with question: %s, state: %s",
                 question, state)

    # define job function
    def job_func():
        return webserver.data_ingestor.state_mean_by_category(question, state)

    logger.info("Exit state_mean_by_category_request - Job registered")
    return register_job(job_func)

@webserver.route('/api/graceful_shutdown', methods=['GET'])
def graceful_shutdown():
    """
    endpoint to initiate server shutdown after completing pending jobs.
    
    returns:
        response: json with shutdown status
    """
    logger.info("Enter graceful_shutdown")

    # initiate graceful shutdown
    webserver.tasks_runner.graceful_shutdown()

    # check queue status
    if webserver.tasks_runner.is_queue_empty():
        logger.info("Exit graceful_shutdown - No jobs in queue")
        return jsonify({
            "status": "done"
        })
    logger.info("Exit graceful_shutdown - Jobs still in queue")
    return jsonify({
        "status": "running"
    })

@webserver.route('/api/jobs', methods=['GET'])
def jobs():
    """
    endpoint to get status of all jobs.
    
    returns:
        response: json with list of all jobs
    """
    logger.info("Enter jobs")

    # get all jobs and status
    job_list = webserver.tasks_runner.get_all_jobs()

    logger.info("Exit jobs - Retrieved %d jobs", len(job_list))
    return jsonify({
        "status": "done",
        "data": job_list
    })

@webserver.route('/api/num_jobs', methods=['GET'])
def num_jobs():
    """
    endpoint to get number of pending jobs.
    
    returns:
        response: json with count of pending jobs
    """
    logger.info("Enter num_jobs")

    # get pending job count
    job_count = webserver.tasks_runner.pending_jobs_count()

    logger.info("Exit num_jobs - %d jobs pending", job_count)
    return jsonify({
        "status": "done",
        "data": job_count
    })

@webserver.route('/')
@webserver.route('/index')
def index():
    """
    endpoint for home page displaying available routes.
    
    returns:
        html: page with list of routes
    """
    logger.info("Enter index")
    routes = get_defined_routes()
    msg = "Hello, World!\n Interact with the webserver using one of the defined routes:\n"

    # format routes as html
    paragraphs = ""
    for route in routes:
        paragraphs += f"<p>{route}</p>"

    msg += paragraphs
    logger.info("Exit index")
    return msg

def get_defined_routes():
    """
    get list of all routes defined in the app.
    
    returns:
        list: strings describing each route
    """
    logger.info("Enter get_defined_routes")
    routes = []
    for rule in webserver.url_map.iter_rules():
        methods = ', '.join(rule.methods)
        routes.append(f"Endpoint: \"{rule}\" Methods: \"{methods}\"")
    logger.info("Exit get_defined_routes - Found %d routes", len(routes))
    return routes
