#!/usr/bin/env python3

'''
	Python Multi Process and Multi Thread
'''

import threading, logging
from math import ceil
from multiprocessing import Process, Lock

FORMAT = '%(asctime)s %(levelname)s : %(message)s'
logging.basicConfig(level=logging.DEBUG, format=FORMAT, datefmt='%Y-%m-%d %H:%M:%S')

def multiThread(target, tokens, targetArgs=(), weight=50, limit=20, threadTarget=None, threadArgs=()):
	threads = []
	threadCount = 1
	lock = threading.Lock()
	tokensCount = len(tokens)

	if tokensCount > weight:
		threadCount = int(ceil(tokensCount/weight))

	logging.debug('total threads: %s', threadCount)

	for tCount in range(threadCount):
		start = tCount * weight
		end = start + weight

		if threadTarget:
			args = threadTarget(*threadArgs) + targetArgs
		else:
			args = targetArgs

		thread = threading.Thread(target=target, args=(tokens[start:end],) + args)
		thread.start()
		threads.append(thread)
		logging.debug('current thread: %s', tCount)

		if (tCount+1) % limit == 0 or tCount+1 == threadCount:
			lock.acquire()
			logging.debug('thread locked at %s', tCount)

			# Wait for all threads to complete
			for t in threads:
				t.join()

			threads = []
			lock.release()
			logging.debug('released thread: %s', tCount)

def multiProcess(target, tokens, targetArgs=(), weight=1000, limit=4, ptarget=None, pargs=(), threadArgs=None):
	lock = Lock()
	processes = []
	processCount = 1
	tokensCount = len(tokens)

	if tokensCount > weight:
		processCount = int(ceil(tokensCount/weight))

	logging.debug('total process: %s', processCount)

	for pCount in range(processCount):
		start = pCount * weight
		end = start + weight

		if ptarget:
			args = ptarget(*pargs) + targetArgs
		else:
			args = targetArgs

		process = Process(target=multiThread, args=(target, tokens[start:end], args), kwargs=threadArgs or {})
		process.start()
		processes.append(process)
		logging.debug('current process: %s', pCount)

		if (pCount+1) % limit == 0 or pCount+1 == processCount:
			lock.acquire()
			logging.debug('process locked at %s', pCount)

			# Wait for all threads to complete
			for p in processes:
				p.join()

			processes = []
			lock.release()
			logging.debug('released process: %s', pCount)
