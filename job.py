import os
import tempfile
import time
import urllib.request
from threading import Thread

from pip._internal.commands.install import InstallCommand

current_dir = os.path.realpath(os.path.dirname(__file__))
os.chdir(current_dir)

wheels_base = 'https://github.com/COVID-19-Text-Mining/scrapers/raw/master/wheels/'
wheels = [
    "bcrypt-3.1.7-cp36-cp36m-linux_x86_64.whl",
    "cffi-1.14.0-cp36-cp36m-linux_x86_64.whl",
    "cryptography-2.9.1-cp36-cp36m-linux_x86_64.whl",
    "lxml-4.5.0-cp36-cp36m-linux_x86_64.whl",
    "numpy-1.18.2-cp36-cp36m-linux_x86_64.whl",
    "pandas-1.0.3-cp36-cp36m-linux_x86_64.whl",
    "pdfminer-20191125-py3-none-any.whl",
    "Protego-0.1.16-py3-none-any.whl",
    "pycryptodome-3.9.7-cp36-cp36m-linux_x86_64.whl",
    "PyDispatcher-2.0.5-py3-none-any.whl",
    "pymongo-3.10.1-cp36-cp36m-linux_x86_64.whl",
    "PyNaCl-1.3.0-cp36-cp36m-linux_x86_64.whl",
    "PyPDF2-1.26.0-py3-none-any.whl",
    "pysftp-0.2.9-py3-none-any.whl",
    "Twisted-20.3.0-cp36-cp36m-linux_x86_64.whl",
    "zope.interface-5.1.0-cp36-cp36m-linux_x86_64.whl",
]

jobs_registry = {
    'scrapy crawl biorxiv': 3600,
    'scrapy crawl cord_19': 3600 * 6,
    'scrapy crawl chemrxiv': 3600,
    'scrapy crawl publichealthontario': 3600,
    'scrapy crawl elsevier_corona': 3600,
    'scrapy crawl lens_patent_spider': 3600,
    'scrapy crawl biorxiv_version_tracker': 86400,
}


def install_pip():
    temp_dir = tempfile.gettempdir()
    wheel_fn = []
    for wheel in wheels:
        fn = os.path.join(temp_dir, wheel)

        print(f'{wheels_base + wheel} => {fn}')
        urllib.request.urlretrieve(wheels_base + wheel, fn)

        wheel_fn.append(fn)

    install_cmd = InstallCommand('install', 'install packages')
    if install_cmd.main(wheel_fn) or install_cmd.main(['-r', 'requirements.txt']):
        print('Failed to execute pip install.')
        exit(1)

    for wheel in wheel_fn:
        print(f'Removing {wheel}')
        os.unlink(wheel)


class Scheduler(object):
    def __init__(self, jobs):
        self.jobs = jobs.copy()
        self.timer = jobs.copy()
        self.running = {name: None for name in jobs}

        for job in jobs:
            self.reset_job(job)

    def poll_jobs(self):
        for name in self.running:
            if self.running[name] is not None and not self.running[name].is_alive():
                self.running[name].join()
                print(f'Job {name} finished.')
                self.running[name] = None

    def reset_job(self, name):
        print(f'Scheduling job "{name}" to be run in {self.jobs[name]} seconds...')
        self.timer[name] = self.jobs[name]

    @staticmethod
    def run_job(job):
        os.system(job)

    def run_forever(self):
        while True:
            for name in list(self.timer):
                self.timer[name] -= 1

            self.poll_jobs()

            execution = [x for x in self.timer if self.timer[x] <= 0]
            for job in execution:
                if self.running[job] is not None:
                    print(f'Job {job} already running, skipping execution')
                else:
                    print(f'Running job "{job}"')
                    t = Thread(target=self.run_job, args=(job,))
                    t.start()
                    self.running[job] = t

                self.reset_job(job)

            time.sleep(1)


if __name__ == '__main__':
    install_pip()

    Scheduler(jobs_registry).run_forever()
