Approaches:
* A series of scatter-gather operations. Where a job splits work into multiple tasks and then waits for all of them to finish. This ensures correctness without maintaining additional metadata as no file is processed by more than one task.

* 