import os
import time

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

    last_run = time.time()

    timer = jobs.copy()


    def reset_job(name):
        print(f'Scheduling job "{name}" to be run in {jobs[name]} seconds...')
        timer[name] = jobs[name]


    for job in jobs:
        reset_job(job)

    while True:
        for name in list(timer):
            timer[name] -= 1

        execution = [x for x in timer if timer[x] == 0]
        for job in execution:
            print(f'Running job "{job}"')
            run_job(job)
            reset_job(job)

        time.sleep(1)
