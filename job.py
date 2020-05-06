import os
import time
from threading import Thread

jobs = {
    'scrapy crawl biorxiv': 3600,
    'scrapy crawl cord_19': 3600,
    'scrapy crawl chemrxiv': 3600,
    'scrapy crawl publichealthontario': 3600,
    'scrapy crawl elsevier_corona': 3600,
    'scrapy crawl lens_patent_spider': 3600,
    'scrapy crawl biorxiv_version_tracker': 86400,
}


def run_job(job):
    os.system(job)


if __name__ == '__main__':
    current_dir = os.path.realpath(os.path.dirname(__file__))
    os.chdir(current_dir)

    timer = jobs.copy()
    running = {name: None for name in jobs}

    def poll_jobs():
        for name in running:
            if running[name] is not None and not running[name].is_alive():
                running[name].join()
                print(f'Job {name} finished.')
                running[name] = None


    def reset_job(name):
        print(f'Scheduling job "{name}" to be run in {jobs[name]} seconds...')
        timer[name] = jobs[name]


    for job in jobs:
        reset_job(job)

    while True:
        for name in list(timer):
            timer[name] -= 1

        poll_jobs()

        execution = [x for x in timer if timer[x] == 0]
        for job in execution:
            if running[job] is not None:
                print(f'Job {job} already running, skipping execution')
            else:
                print(f'Running job "{job}"')
                t = Thread(target=run_job, args=(job,))
                t.start()
                running[job] = t

            reset_job(job)

        time.sleep(1)
