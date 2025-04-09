"""
Module providing thread pool implementation for task execution.
Contains threadpool and taskrunner classes for managing asynchronous jobs.
"""
import os
import json
from queue import Queue, Empty
from threading import Thread, Event, Lock
import logging

# get the logger
logger = logging.getLogger('webserver')

class ThreadPool:
    """
    manages a pool of worker threads to process jobs asynchronously.
    provides job queueing, status tracking, and graceful shutdown.
    """

    def __init__(self):
        """
        initialize thread pool with configurable number of workers.
        uses environment variable or cpu count to determine thread count.
        """
        # check environment variable for thread count
        thread_count_env = os.environ.get('TP_NUM_OF_THREADS')

        if thread_count_env is not None:
            self.num_threads = int(thread_count_env)
        else:
            self.num_threads = os.cpu_count() or 4

        logger.info("Initializing ThreadPool with %d threads", self.num_threads)

        # data structures
        self.job_queue = Queue()
        self.shutdown_event = Event()
        self.jobs_status = {}
        self.jobs_lock = Lock()

        # start the worker threads
        self.threads = []
        for i in range(self.num_threads):
            runner = TaskRunner(self.job_queue, self.shutdown_event,
                                self.jobs_status, self.jobs_lock)
            self.threads.append(runner)
            runner.daemon = True
            runner.start()
            logger.info("Started worker thread %d", i+1)

    def add_job(self, job_id, job_func):
        """
        add a job to the queue and mark it as running.
        
        args:
            job_id: unique identifier for the job
            job_func: callable to be executed by a worker thread
            
        returns:
            job_id: the id of the added job
        """
        with self.jobs_lock:
            self.jobs_status[job_id] = 'running'
        self.job_queue.put((job_id, job_func))
        logger.info("Added job %s to the queue", job_id)
        return job_id

    def get_job_status(self, job_id):
        """
        get the status of a job.
        
        args:
            job_id: identifier of the job to check
            
        returns:
            status: current status of the job or none if not found
        """
        with self.jobs_lock:
            status = self.jobs_status.get(job_id)
            logger.info("Retrieved status for job %s: %s", job_id, status)
            return status

    def get_all_jobs(self):
        """
        get all jobs and their status.
        
        returns:
            list: list of dicts with job ids and their status
        """
        with self.jobs_lock:
            job_list = [{job_id: status} for job_id, status in self.jobs_status.items()]
            logger.info("Retrieved all jobs, count: %d", len(job_list))
            return job_list

    def pending_jobs_count(self):
        """
        get the count of jobs remaining in the queue.
        
        returns:
            int: number of jobs in the queue
        """
        count = self.job_queue.qsize()
        logger.info("Pending jobs count: %d", count)
        return count

    def graceful_shutdown(self):
        """
        initiate graceful shutdown - finish pending jobs but don't accept new ones.
        """
        self.shutdown_event.set()
        logger.info("Initiated graceful shutdown")

    def is_shutting_down(self):
        """
        check if the pool is in shutdown mode.
        
        returns:
            bool: true if shutting down, otherwise false
        """
        is_shutting = self.shutdown_event.is_set()
        logger.info("ThreadPool shutdown status: %s", is_shutting)
        return is_shutting

    def is_queue_empty(self):
        """
        check if the job queue is empty.
        
        returns:
            bool: true if queue is empty, otherwise false
        """
        is_empty = self.job_queue.empty()
        logger.info("Job queue empty status: %s", is_empty)
        return is_empty

class TaskRunner(Thread):
    """
    worker thread that processes jobs from the queue.
    executes job functions and saves results to disk.
    """

    def __init__(self, job_queue, shutdown_event, jobs_status, jobs_lock):
        """
        initialize taskrunner with required resources.
        
        args:
            job_queue: queue to get jobs from
            shutdown_event: event to signal shutdown
            jobs_status: shared dict for job status tracking
            jobs_lock: lock to protect shared data
        """
        # init necessary data structures
        super().__init__()
        self.job_queue = job_queue
        self.shutdown_event = shutdown_event
        self.jobs_status = jobs_status
        self.jobs_lock = jobs_lock
        logger.info("TaskRunner initialized with thread ID: %d", id(self))

    def run(self):
        """
        main thread loop to process jobs until shutdown.
        gets jobs from queue, executes them, and saves results.
        """
        logger.info("TaskRunner %d started", id(self))
        while not (self.shutdown_event.is_set() and self.job_queue.empty()):
            try:
                # get job with timeout to check shutdown periodically
                job_id, job_func = self.job_queue.get(timeout=1.0)
                logger.info("TaskRunner %d got job: %s", id(self), job_id)

                try:
                    # execute the job
                    logger.info("Executing job %s", job_id)
                    result = job_func()
                    logger.info("Job %s executed successfully", job_id)

                    # ensure results directory exists
                    if not os.path.exists('results'):
                        os.makedirs('results')
                        logger.info("Created results directory")

                    # save result to disk
                    try:
                        # check json serialization
                        json_string = json.dumps(result)

                        with open(f'results/{job_id}', 'w', encoding='utf-8') as f:
                            f.write(json_string)

                        # update job status
                        with self.jobs_lock:
                            self.jobs_status[job_id] = 'done'
                        logger.info("Result for job %s saved to disk", job_id)
                    except TypeError as e:
                        # handle non-serializable objects
                        logger.warning(
                            "TypeError serializing result for job %s, attempting conversion: %s", 
                            job_id, e
                        )

                        # try to convert result to serializable format
                        if isinstance(result, dict):
                            serializable_result = {}
                            for k, v in result.items():
                                if isinstance(k, tuple):
                                    # convert tuple key to string
                                    serializable_result[str(k)] = v
                                else:
                                    serializable_result[k] = v

                            # serialize converted result
                            json_string = json.dumps(serializable_result)

                            with open(f'results/{job_id}', 'w', encoding='utf-8') as f:
                                f.write(json_string)

                            # update job status
                            with self.jobs_lock:
                                self.jobs_status[job_id] = 'done'
                            logger.info("Converted result for job %s saved to disk", job_id)
                        else:
                            # if not a dict cant convert
                            logger.error("Failed to convert result for job %s: %s", job_id, e)
                            raise e

                except Exception as e:
                    logger.error("Error processing job %s: %s", job_id, e)

                    # save error result
                    with open(f'results/{job_id}', 'w', encoding='utf-8') as f:
                        json.dump({"error": str(e)}, f)

                    # mark job as done
                    with self.jobs_lock:
                        self.jobs_status[job_id] = 'done'

                # mark task as done in queue
                self.job_queue.task_done()
                logger.info("Job %s marked as done in queue", job_id)

            except Empty:
                continue
            except Exception as e:
                # handle unexpected errors
                logger.error("Unexpected error in TaskRunner %d: %s", id(self), e)

        logger.info("TaskRunner %d stopping", id(self))
