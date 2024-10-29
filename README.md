# What is MapReduce?
> MapReduce is a programming model and an associated implementation for processing and generating large data sets. Users specify a map function that processes a key/value pair to generate a set of intermediate key/value pairs, and a reduce function that merges all intermediate values associated with the same intermediate key. Many real world tasks are expressible in this model, as shown in the paper.

> Programs written in this functional style are automatically parallelized and executed on a large cluster of commodity machines. The run-time system takes care of the details of partitioning the input data, scheduling the program’s execution across a set of machines, handling machine failures, and managing the required inter-machine communication. This allows programmers without any experience with parallel and distributed systems to easily utilize the resources of a large distributed system.

[Dean and Ghemawat, 2004, MapReduce: Simplified Data Processing on Large Clusters](https://research.google.com/archive/mapreduce-osdi04.pdf)

# What is this repository?
This repository contains an implementation of a MapReduce library that can be used to parallelize computing tasks over multiple OS processes, in contrast to the actual MapReduce system that runs on a large cluster of machines. The library was coded as part of [Lab 1](http://nil.csail.mit.edu/6.824/2020/labs/lab-mr.html) in MIT's [6.824: Distributed Systems](http://nil.csail.mit.edu/6.824/2020/). It is parrallel, fault-tolerant (in case of worker crashes), and uses RPC for master-worker communication.

# How do I run the code?
A prerequisite is to have Golang installed and available.

Here's how to run the code on the word-count MapReduce application. First, make sure the word-count plugin is freshly built:

```sh
$ go build -buildmode=plugin ../mrapps/wc.go
```

In the main directory, run the master.

```sh
$ rm mr-out*
$ go run mrmaster.go pg-*.txt
```

The pg-*.txt arguments to mrmaster.go are the input files; each file corresponds to one "split", and is the input to one Map task.

In one or more other windows, run some workers:

```sh
$ go run mrworker.go wc.so
```

When the workers and master have finished, look at the output in mr-out-*. When you've completed the lab, the sorted union of the output files should match the sequential output, like this:

```sh
$ cat mr-out-* | sort | more
A 509
ABOUT 2
ACT 8
...
```

# Tests
To run the test script,
```sh
$ cd main
$ sh test-mr.sh
```
