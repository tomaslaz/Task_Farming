import multiprocessing
from multiprocessing import Process
import Queue
import random
import time

"""
A simple implementation of using a pool of workers to do a repetitive (same) task with multiple input data.

In this example we will calculate the n-th triangle number (brute force) of n random numbers between 100 and 1000.

@author Tomas Lazauskas, 2016
@web www.lazauskas.net
@email tomas.lazauskas[a]gmail.com
"""

class Worker(Process):
  """
  A worker class to perform calculations
  
  """
  
  def __init__(self, procID, workQueue, resultQueue):
    """
    See gangUpWorkers for the input explanation.
      Input:
        procID - worker (process) id number
        workQueue - input data queue
        resultQueue - results queue
      
    """
    
    Process.__init__(self)
    
    self.procID = procID
    self.workQueue = workQueue
    self.resultQueue = resultQueue
    
  def run(self):
    """
    Worker performs its jobs. 
    
    Asks for initial values from the input queue (self.input), calculates the result and 
    puts it and the initial values into the results queue.
    
    """
    
    while True:
      try:
        timeSt = time.time()
        
        # requesting new data from the input queue
        initialValue = self.workQueue.get(False)
        print "Process %2d: got initial data = %d" % (self.procID, initialValue)
        
        # performing the calculation
        resultValue = self.calculation(initialValue)
        
        # saving the result into the results queue
        result = (initialValue, resultValue)
        self.resultQueue.put(result)
        
        # marking the task as finished
        self.workQueue.task_done()
        
        timeElapsed = time.time() - timeSt
        print "Process %2d: finished in %f s., sum = %d" % (self.procID, timeElapsed, resultValue)
      
      # if there are no unfinished tasks in the queue, exit
      except Queue.Empty:
        break
                 
  def calculation(self, inputValue):
    """
    Worker's job - calculates the n-th triangle number.
    
    Input: 
      inputValue - an integer number for which the n-th triangle number needs to be calculated
      
    Returns:
      sum - n-th triangle number
    
    """
    
    sum = 0
    
    inputValueInt = int(inputValue)
    
    if inputValueInt >= 0:
      for i in range(inputValueInt+1):
        sum += i
    
    return sum

def gangUpWorkers():
  """
  Manages workers and their jobs.
  
  """
  
  # number of random integer numbers (number of tasks)
  sumCount = 10
  sumRangeFrom = 1000000
  sumRangeTo   = 10000000
  
  # number of workers (number of available cores)
  cpuCount = multiprocessing.cpu_count()
  print "Running on %d processes." % (cpuCount)

  # a shared input queue
  inputQueue = multiprocessing.JoinableQueue()
  
  # Initializing the inputs queue (random integer numbers)
  for _ in range(sumCount):
    inputQueue.put(random.randint(sumRangeFrom, sumRangeTo))
  
  resultValues = multiprocessing.JoinableQueue()
  
  # Initializing and running workers
  processes = []
  for i in range(cpuCount):
     
    # initializing a worker
    p = Worker(i, inputQueue, resultValues)
    p.daemon = True
    p.start()
    processes.append(p)
  
  inputQueue.join() 
    
  # Printing the results
  results = []
  resultCnt = 0
  while(not resultValues.empty()):
    resultCnt += 1
    result = resultValues.get()
    
    print "%3d. %d? = %d" % (resultCnt, result[0], result[1])
     
if __name__ == "__main__":
  gangUpWorkers()
  