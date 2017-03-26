"""Problem B Homework 2 ECS145
Group Members:
    John Nguyen 998808398
    Eric Du 913327304
    Joanne Wang 9133360523
    Jeffrey Tai 998935915
"""

import threading
import os
import time

# class for threads, subclassed from threading.Thread class
class Thread(threading.Thread):
    lenList = []
    llock = threading.Condition()
    currThread = 0
    nThreads = 0

    def __init__(self,filename, startPos, to_read, i):
        # invoke constructor of parent class
        threading.Thread.__init__(self)
        self.startPos = startPos
        self.to_read = to_read
        self.threadNum = i
        self.filename = filename

    def is_sliced(self, start):
        start_mid = False
        if start != 0:
            f=open(self.filename, 'rb')
            f.seek(start-1)
            newLine = f.read(1)
            if newLine != '\n':
                start_mid = True
        return start_mid

    def run(self):
        f = open(self.filename, 'rb')
        #starts reading at given offset
        f.seek(self.startPos)

        #reads only the amount of bytes its designated to
        stringRead = f.read(self.to_read)

        #want to split by lines
        splitLine = stringRead.split('\n')

        sliced = self.is_sliced(self.startPos)

        Thread.llock.acquire()
        # enumerate order on which threads can append to lenlist
        # once a thread finishes reading, acquires lock but waits
        # until its turn is up
        while(Thread.currThread != self.threadNum):
            Thread.llock.wait()
        # this means that currthread number is equal to self thread number
        # so it can start adding to lenlist
        self.addtoLenList(splitLine, sliced)
        # done appending, notify other threads so they can
        # check if they meet requirements (while loop above) for their turn
        Thread.llock.notify()
        Thread.llock.release()

    def addtoLenList(self, line_list,sliced):
        line_list = map(len, line_list)
        # first thread has come in
        if(Thread.currThread == 0):
            Thread.lenList = line_list
        elif sliced:
            Thread.lenList[-1] += line_list[0]
            Thread.lenList.extend(line_list[1:])
            sliced = False
        else:
            Thread.lenList.extend(line_list)
        
        # this conditional ensures that EOL's at the end (caused by .split function)
        # isn't counted as a line
        # i.e.     'hi\n'  
        # when split on line 41, will turn to [2, 0] (after using map function on line 60)
        # and append a useless '0' in the end
        if Thread.lenList[-1] == 0 and self.threadNum != Thread.nThreads-1:
            Thread.lenList.pop(-1)

        # updated currThread num expected to be working on so next thread
        # can append its values
        Thread.currThread = Thread.currThread + 1

def linelengths(filenm, ntrh):
    # set all class methods to default values
    Thread.lenList = []
    Thread.currThread = 0
    Thread.nThreads = ntrh
    # create empty list of threads
    threads = []

    fileLength = os.path.getsize(filenm) # get number of bytes

    if fileLength >= ntrh:
        to_read = int(fileLength / ntrh)

    for i in range(ntrh):
        startPos = i * to_read  # offset that each thread starts reading from
        if i == ntrh-1:  # last thread gets designated amount + remaining bytes
            to_read += (fileLength % ntrh)

        t = Thread(filenm, startPos, to_read, i)
        threads.append(t)

    for t in threads:
        t.start()

    for t in threads:
        t.join()

    return Thread.lenList

def timed(filenm, ntrh):
    start = time.time()
    result = linelengths(filenm, ntrh)
    end = time.time()
    print 'time: ', end-start