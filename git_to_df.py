from git import Repo
import pandas as pd
import json
from datetime import datetime
import os

# ---- CONFIG ----
REPO_PATH = "."              # path to your repo
FILE_PATH = "dmi.dk-NinJo2DmiDk-ninjo2dmidk_cmd=llj_id=2610734.json"      # path inside repo
BRANCH = "main"              # change if needed
# ----------------


def load_json_from_blob(blob):
    """Read JSON content from a git blob."""
    data = blob.data_stream.read().decode("utf-8")
    return json.loads(data)


def build_dataframe(repo_path, file_path, branch="main"):
    repo = Repo(repo_path)
    commits = list(repo.iter_commits(branch, paths=file_path))

    all_rows = []

    # Iterate oldest -> newest
    for commit in reversed(commits):
        try:
            blob = commit.tree / file_path
        except KeyError:
            continue  # file didn't exist in this commit

        try:
            json_data = load_json_from_blob(blob)
        except Exception:
            continue  # skip invalid JSON

        commit_time = datetime.fromtimestamp(commit.committed_date)

        # Explode the "timeserie" list
        if "timeserie" in json_data and isinstance(json_data["timeserie"], list):
            df_part = pd.json_normalize(
                json_data, 
                record_path="timeserie",
                meta=[k for k in json_data.keys() if k != "timeserie"]
            )
        else:
            df_part = pd.json_normalize(json_data)

        # Add commit metadata
        df_part["commit_hash"] = commit.hexsha
        df_part["commit_time"] = commit_time

        all_rows.append(df_part)

    if not all_rows:
        return pd.DataFrame()

    df = pd.concat(all_rows, ignore_index=True)
    df = df.sort_values("commit_time").reset_index(drop=True)
    return df.drop(["aggData", "sixHourSymbols", "twelveHourSymbols"], axis=1)



if __name__ == "__main__":
    df = build_dataframe(REPO_PATH, FILE_PATH, BRANCH)
    print(df.head())
    print(f"\nTotal rows: {len(df)}")

    # Save if needed
    df.to_csv("data_history.csv", index=False)