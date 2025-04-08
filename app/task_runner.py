"""
Module providing thread pool implementation for task execution.
Contains ThreadPool and TaskRunner classes for managing asynchronous jobs.
"""
import os
import json
from queue import Queue, Empty
from threading import Thread, Event, Lock
import logging

logging.basicConfig(
    filename='app.log',
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

class ThreadPool:
    def __init__(self):
        # check environment variable for thread count
        thread_count_env = os.environ.get('TP_NUM_OF_THREADS')

        if thread_count_env is not None:
            self.num_threads = int(thread_count_env)
        else:
            self.num_threads = os.cpu_count() or 4

        # ensure results directory exists
        if not os.path.exists('results'):
            os.makedirs('results')

        # data structures
        self.job_queue = Queue()
        self.shutdown_event = Event()
        self.jobs_status = {}
        self.jobs_lock = Lock()

        # start the worker threads
        self.threads = []
        for _ in range(self.num_threads):
            runner = TaskRunner(self.job_queue, self.shutdown_event,
                                self.jobs_status, self.jobs_lock)
            self.threads.append(runner)
            runner.daemon = True
            runner.start()

    def add_job(self, job_id, job_func):
        """add a job to the queue and mark it as running"""
        with self.jobs_lock:
            self.jobs_status[job_id] = 'running'
        self.job_queue.put((job_id, job_func))
        return job_id

    def get_job_status(self, job_id):
        """get the status of a job"""
        with self.jobs_lock:
            return self.jobs_status.get(job_id)

    def get_all_jobs(self):
        """get all jobs and their status"""
        with self.jobs_lock:
            return [{job_id: status} for job_id, status in self.jobs_status.items()]

    def pending_jobs_count(self):
        """get the count of jobs remaining in the queue"""
        return self.job_queue.qsize()

    def graceful_shutdown(self):
        """initiate graceful shutdown - stop accepting new jobs but finish pending ones"""
        self.shutdown_event.set()

    def is_shutting_down(self):
        """check if the pool is in shutdown mode"""
        return self.shutdown_event.is_set()

    def is_queue_empty(self):
        """check if the job queue is empty"""
        return self.job_queue.empty()

class TaskRunner(Thread):
    def __init__(self, job_queue, shutdown_event, jobs_status, jobs_lock):
        # init necessary data structures
        super().__init__()
        self.job_queue = job_queue
        self.shutdown_event = shutdown_event
        self.jobs_status = jobs_status
        self.jobs_lock = jobs_lock

    def run(self):
        while not (self.shutdown_event.is_set() and self.job_queue.empty()):
            # Get pending job
            # Execute the job and save the result to disk
            # Repeat until graceful_shutdown
            try:
                # try to get a job with a timeout to check shutdown periodically
                job_id, job_func = self.job_queue.get(timeout=1.0)

                try:
                    # execute the job
                    result = job_func()

                    # ensure the results directory exists
                    if not os.path.exists('results'):
                        os.makedirs('results')

                    # save the result to disk
                    try:
                        # verify that the result is JSON serializable
                        json_string = json.dumps(result)

                        with open(f'results/{job_id}', 'w', encoding='utf-8') as f:
                            f.write(json_string)

                        # update job status
                        with self.jobs_lock:
                            self.jobs_status[job_id] = 'done'
                    except TypeError as e:
                        # handle non-serializable objects
                        logging.warning(
                            "TypeError serializing result for job %s, attempting conversion", job_id
                        )

                        # try to convert result to a serializable format
                        if isinstance(result, dict):
                            serializable_result = {}
                            for k, v in result.items():
                                if isinstance(k, tuple):
                                    # convert tuple key to string
                                    serializable_result[str(k)] = v
                                else:
                                    serializable_result[k] = v

                            # try to serialize the converted result
                            json_string = json.dumps(serializable_result)

                            with open(f'results/{job_id}', 'w', encoding='utf-8') as f:
                                f.write(json_string)

                            # update job status
                            with self.jobs_lock:
                                self.jobs_status[job_id] = 'done'
                        else:
                            # if not a dict, raise the original error
                            raise e

                except Exception as e:
                    logging.error("Error processing job %s: %s", job_id, e)

                    # save error result
                    with open(f'results/{job_id}', 'w', encoding='utf-8') as f:
                        json.dump({"error": str(e)}, f)

                    # mark job as done (with error)
                    with self.jobs_lock:
                        self.jobs_status[job_id] = 'done'

                # mark the task as done in the queue
                self.job_queue.task_done()

            except Empty:
                continue
            except Exception as e:
                # general error
                logging.error("Unexpected error in TaskRunner: %s", e)
