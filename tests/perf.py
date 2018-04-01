import time

from threading import Thread
from pycache.client import Client

exit = False
keys = 'abcdefghijklmnopqrstuvwxyz'
len_keys = len(keys)
num_workers = 10


class Writer(Thread):
    exit = False
    
    def __init__(self):
        super().__init__()
        self.client = Client()
        self.client.connect()
        self.writes = 0

    def run(self):
        writes = 0
        while not self.exit:
            self.client.put(keys[writes % len_keys], {'value': writes})
            writes += 1

        self.writes = writes


class Reader(Thread):
    exit = False
    
    def __init__(self):
        super().__init__()
        self.client = Client()
        self.client.connect()
        self.reads = 0

    def run(self):
        reads = 0
        while not self.exit:
            self.client.get(keys[reads % len_keys])
            reads += 1

        self.reads = reads


def do_writes():
    writers = []
    for i in range(num_workers):
        w = Writer()
        writers.append(w)

    for w in writers:
        w.start()

    time.sleep(10)
    print('Killing writers')
    Writer.exit = True

    writes = 0
    for w in writers:
        w.join()
        writes += w.writes

    print('{} writes'.format(writes))


def do_reads():
    readers = []
    for i in range(num_workers):
        r = Reader()
        readers.append(r)

    for r in readers:
        r.start()

    time.sleep(10)
    print('Killing readers')
    Reader.exit = True

    reads = 0
    for r in readers:
        r.join()
        reads += r.reads

    print('{} reads'.format(reads))


if __name__ == '__main__':
    do_writes()
    do_reads()

