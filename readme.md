```

Summary

Design a system that assigns a large job to be processed across a fleet of workers. The workers’ outputs are aggregated into a single result which is returned to the user.

The job itself will be to use W workers to compute the mean of every index across F files full of C random numbers.

W = number of workers.

F = number of files.

C = number of random values in each file.

Example

So if F=2 and C=3, and the input files are [1,2,3] and [4,5,6] then the output will be a single file consisting of the numbers [2.5, 3.5, 4.5].

Values for W, F, and C should all be chosen by the user. For the random numbers themselves, feel free to put whatever bound you want on them (between 0 and 1 for example). Files can use a format of your choice (csv, etc).

Components

The components involved should be an API, a queue, and a fleet of workers. Additional components can be added (maybe a bucket or db) depending on how you want to approach it. The API should *ideally* use NodeJS, Express, and Typescript (though you may use a language/framework of your choice). The workers should be in Python. The queue can be any kind, SQS, something local, anything.

Flow

Starting from the user's entry point for the API the first step should be to input a value for W and submit it. This will spawn W workers which will idle while they wait for a job to appear on the queue. Then create a job by inputting values for C and F, and submitting that job. The job will be sent to the API where it will be enqueued. Then once at least one worker is available the job will start being processed. Remember the result of each worker must be combined to create the final averaged file. I'm interested to see how you approach this.

Additional points

To simulate RAM constraints on each worker, let's say that a worker cannot process more than 5 files at a time. Therefore your system will need to figure out how to split up the job when F > 5.
For a job where F > 5 (these are the jobs I’m most interested in) you can place one or multiple items on the queue. There is not a strict 1:1 relationship between jobs and items on the queue.
Should be able to submit multiple jobs, before any complete.
The generated files can be stored on the API's local file system or in a place of your choosing, like a bucket, as long as the workers have a way of pulling/reading them.
Assume that workers process files at different speeds. Given this, design your worker orchestration algorithm such that workers spend as little time as possible idling or waiting on other slower workers.
------------------------------------------------------------------------------------------------------
```

Approaches:
* A series of scatter-gather operations. Where a job splits work into multiple tasks and then waits for all of them to finish. This ensures correctness without maintaining additional metadata as no file is processed by more than one task.

* One thing to note is that average operation can be performed at the end. We can let the workers compute partial sum. A job can be split into multiple tasks where each task can be picke up by a worker and acted upon to compute the worker level partial sum. Once all the partial sums are complete we can do a gather on all of them.
    * How to know if all the partial sums are complete? If we have considered each input file exactly one time then we are good. 
    * Have a completion marker in the task queue. When all the file processing tasks have been consumed any worker that picks up this marker will check if we are ready to perform the final operation. In case, the partial sums are not ready due to some slower worker, we can requeue the marker operation.
