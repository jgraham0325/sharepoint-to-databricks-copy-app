"""
Prepare manifest paths for the For Each transfer task.
Reads the job parameter manifest_paths_json (JSON array of volume paths), parses it,
and sets the task value 'manifest_paths' so the for_each_transfer task can iterate over it.
"""
import json
import sys

if len(sys.argv) != 2:
    raise ValueError("Usage: prepare_manifest_inputs.py <manifest_paths_json>")

manifest_paths_json = sys.argv[1]
parsed = json.loads(manifest_paths_json)
if not isinstance(parsed, list):
    raise TypeError("manifest_paths_json must be a JSON array of strings")
dbutils.jobs.taskValues.set(key="manifest_paths", value=parsed)
