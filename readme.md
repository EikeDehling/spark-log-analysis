# Big data analysis with Spark

Did i get your attention with the keywords in the headline? This article is not about setting up multi machine clusters or crunching hunderds of terabytes of data in the cloud. Rather i used big data tools for a smaller scale problem, because they fitted my needs very well.

At work we're working on a project to use logging data to measure and improve qualityt of search results. In total we had around 100 Gigabytes of logging data from our production servers that we wanted to analyze. The infrastructure to analyze data consisted of a spare virtual machine and some python scripts.

In this blog i will describe how to use spark (on your local machine) to speed up such an analysis task.


## Plain python

Initially the python scripts for this just iterated over a directory of log files and then processed the files sequentially. This was easy to code:


```
	import glob
	for filename in glob.glob('/home/user/folder/*.log.gz'):
		with gzip.open(filename, 'rb') as input:
			for line in input:
				process_log_line(line)
```

Of course it took a long time to process the logs (around 2 hours), while it would be nice to see some numbers sooner to ease and speed up interpretation. Also this approach let to problems with memory usage, even with 16Gb of memory the program crashed. Finally, even the abbreviated code above shows some boilerplate, and how to deal with some compressed and some non-compressed files.

We could of course parallelize the code and keep building on our own scripts, but there must be smart people that dealt with this problem before!


## How does spark help?

Spark is a framework for distributed processing, especially for processing of large volumes of data. It can run on clusters of many machines, but also on your laptop. In this case i ran spark (pyspark) locally on my laptop using the I/O and parallel computing functionality to speed up log analysis:

- I/O functions: Spark has good support for various file formats, compression and directories of files. It takes away the boilerplate code we needed before.

- Parallel processing: Spark is great in parallel (and distributed) processing. You write your code very familiar, like python list operations (map, filter, aggregate) or pandas dataframe operations; and spark then executes in parallel. This speeded up execution almost 10 fold.


## Spark solution

So how does the spark solution look like? In the simple example below, the log files are in a tab-separated format and we want to count the number of times a certain action (the third field of each row) occurs. Then we output the top 10 most frequent actions. The full example code is [here](https://github.com/EikeDehling/spark-log-analysis/blob/master/top-urls.py).

```
	actions_counts = sc \
		.textFile('file:///logs/server-*/activity.2017*.log.gz') \
		.map(lambda line: line.split('\t')) \
		.filter(lambda row: row and len(row) > 1) \
		.map(lambda row: row[3]) \
		.countByValue()

	for action, count in sorted(actions_counts.items(), key=itemgetter(1), reverse=True)[:10]:
		print("{0:<32} -> {1:>12}".format(action, count))
```

As you can see, the boilerplate code is gone and with just a few lines we were able to get the information we wanted to know! Even though this example is simple, it shows the basics of how to program spark. Have fun!


## Running

To install and run, execute these commands:

```
   virtualenv .
   bin/activate
   bin/pip install pyspark
   bin/python ./top-urls.py
```


## Conclusions

With a very practical example i showed how you can use spark to analyse logs, how it speeds up the analysis nicely and how it helps with reducing boilerplate. I hope you enjoyed the read!
