import json
import sys
import os

# Path to the JSON file
EXPECTED_DURATIONS_FILE = "expected_durations.json"

def check_duration(result_str):
    # Load expected durations from the JSON file
    try:
        with open(EXPECTED_DURATIONS_FILE, 'r') as f:
            loaded_durations = json.load(f)
        # Convert string keys back to tuples for dictionary lookup
        expected_durations = {tuple(k.split(',')): v for k, v in loaded_durations.items()}
    except FileNotFoundError:
        print(f"Error: {EXPECTED_DURATIONS_FILE} not found.", file=sys.stderr)
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON from {EXPECTED_DURATIONS_FILE}: {e}", file=sys.stderr)
        sys.exit(1)

    # Get the JSON output from the provided argument
    try:
        result = json.loads(result_str)
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON input: {e}", file=sys.stderr)
        sys.exit(1)

    # Extract relevant fields from the result
    try:
        bucket = result['bucket']
        layout = result['layout']
        num_objects = str(result['num_objects'])
        parallelism = str(result['parallelism'])
        num_subdir = str(result['num_subdir'])
        operation = result['operation']
        duration_s = result['duration_s']
    except KeyError as e:
        print(f"Missing key in JSON output: {e}", file=sys.stderr)
        sys.exit(1)

    # Create a key to look up the expected duration
    key = (bucket, layout, num_objects, parallelism, num_subdir, operation)

    # Check if we have an expected duration for this operation
    if key in expected_durations:
        expected_duration = expected_durations[key]
        allowed_duration = expected_duration * 1.1

        print(f"Operation: {key}")
        print(f"Actual duration: {duration_s}s")
        print(f"Expected duration: {expected_duration}s")
        print(f"Allowed duration (10% tolerance): {allowed_duration}s")

        if duration_s > allowed_duration:
            print("Error: Duration exceeds the allowed limit.")
            sys.exit(1)
        else:
            print("Success: Duration is within the allowed limit.")
    else:
        print(f"Warning: No expected duration found for operation: {key}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 check_duration.py <json_result_string>", file=sys.stderr)
        sys.exit(1)
    check_duration(sys.argv[1])
