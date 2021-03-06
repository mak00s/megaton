from os import *

from subprocess import Popen, PIPE, STDOUT
from time import sleep


def system(cmd: str):
    process = Popen(cmd, shell=True,
                    stdout=PIPE, stderr=STDOUT,
                    universal_newlines=True)
    while True:
        output = process.stdout.readline()
        print(output, end='')
        sleep(0.1)
        if process.poll() is not None:
            # remaining output
            for output in process.stdout.readlines():
                print(output, end='')
            break
