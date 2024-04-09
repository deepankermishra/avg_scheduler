import os
from flask import Flask, request, jsonify

from impl.processor.processor import get_job
from impl.utils import get_csv_file_paths
from impl.schd import scheduler

server = Flask(__name__)


@server.route('/worker', methods=['POST'])
def create_workers():
    pass


# Sample payload
# { "relative_input_path": "./data/input_2/", "relative_output_path": "./data/input_2/output", "cardinality": 3, "num_workers": 2, "id": "test_job_1" }
# { "relative_input_path": "./data/input_1/", "relative_output_path": "./data/input_1/output", "cardinality": 3, "num_workers": 2, "id": "test_job_2" }
# { "relative_input_path": "./data/input/", "relative_output_path": "./data/input/output", "cardinality": 3, "num_workers": 2, "id": "test_job_3" }
@server.route('/job', methods=['POST'])
def submit_job(): 
    data = request.get_json()
    print(data)
    relative_input_path = data.get('relative_input_path')
    relative_output_path = data.get('relative_output_path')
    input_dir =  os.path.abspath(relative_input_path)
    output_dir = os.path.abspath(relative_output_path)
    cardinality = data.get('cardinality')
    num_workers = data.get('num_workers')
    job_name = data.get('name')

    num_files = len(get_csv_file_paths(input_dir))
    job = get_job(job_name, num_workers, num_files, cardinality, input_dir, output_dir)
    
    print(f'Adding job {job}')
    scheduler.add_job(job)
    return jsonify({"id": job.id})

