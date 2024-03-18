from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from concurrent.futures import as_completed
from multiprocessing.pool import Pool


class ConcurrentUtil:
    @staticmethod
    def do_async_job(f, params_list, max_workers=8):
        """
        create thread pool executor and submit function with set of params in the executor
        :param f: function to execute
        :param params_list: list of tuples of params for the function to execute
        :param max_workers: maximum workers for the executor
        :return: list of result objects
        """

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(f, *p) for p in params_list]
            res = [future.result() for future in as_completed(futures)]
        return res

    @staticmethod
    def run_in_separate_processes(f, params_list, max_processes=4):
        with ProcessPoolExecutor(max_processes) as executor:
            futures = [executor.submit(f, *p) for p in params_list]
            res = [future.result() for future in as_completed(futures)]
        return res
