import os
import re

DAG_FOLDER = "dags"

for filename in os.listdir(DAG_FOLDER):
    if filename.endswith(".py"):
        filepath = os.path.join(DAG_FOLDER, filename)

        with open(filepath, "r") as f:
            content = f.read()

        match = re.search(r'dag_id\s*=\s*["\']([^"\']+)["\']', content)
        match_2 = re.search(r'DAG_ID\s*=\s*["\']([^"\']+)["\']', content)

        if match:
            dag_id = match.group(1)
            new_filename = f"{dag_id}.py"
            new_filepath = os.path.join(DAG_FOLDER, new_filename)
        elif match_2:
            dag_id = match_2.group(1)
            new_filename = f"{dag_id}.py"
            new_filepath = os.path.join(DAG_FOLDER, new_filename)

            if filename != new_filename:
                print(f"Rename: {filename} → {new_filename}")
                os.rename(filepath, new_filepath)