from impl.consts import JOB_DISPATCH_ALGO, JOB_ALGO

from impl.processor.scatter_gather.job import ScatterGatherJob
from impl.processor.partial_sum.worker import setup_workers
from impl.processor.partial_sum.job import PartialSum
from impl.processor.accumulator.job import AccumulatorJob

#  id, num_workers, num_files, cardinality, input_dir, output_dir=None
def get_job(name, num_workers, num_files, cardinality, input_dir, output_dir=None):
    if JOB_DISPATCH_ALGO == JOB_ALGO.SCATTER_GATHER:
        return ScatterGatherJob(name, num_workers, num_files, cardinality, input_dir, output_dir)
    elif JOB_DISPATCH_ALGO == JOB_ALGO.PARTIAL_SUMS:
        setup_workers()
        return PartialSum(name, num_workers, num_files, cardinality, input_dir, output_dir)
    elif JOB_DISPATCH_ALGO == JOB_ALGO.ACCUMULATOR:
        return AccumulatorJob(name, num_workers, num_files, cardinality, input_dir, output_dir)
    else:
        raise ValueError(f'Invalid job dispatch algorithm {JOB_DISPATCH_ALGO}')
