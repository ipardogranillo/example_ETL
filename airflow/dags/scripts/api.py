import requests
import gzip
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def get_api_response(url):
    print("creating requests session...")
    print("making request...")
    response = requests.get(url, verify=False)
    print(response.request)
    print(response)
    if not response:
        print("ERROR: no response received")
        return None
    else:
        print("decompressing gzip file...")
        decomp_gz = gzip.decompress(response.content)
        if not decomp_gz:
            print("ERROR: decompression unsuccessful")
            return None
        else:
            print("parsing json...")
            json_res = json.loads(decomp_gz)
            if not json_res:
                return None
            else:
                return json_res


def dump_api_response(json_res, run_time, dump_path):
    if type(json_res) == None:
        print("no dataframe to create")
        return False
    else:
        n_rows = len(json_res)
        if n_rows < 1:
            print("ERROR: no rows to process")
            return False
        else:
            run_time_col = [run_time] * n_rows
            print("creating dataframe with run time index...")
            df = pd.DataFrame(json_res, index=run_time_col)
            df.index.name = "run_time"
            print(f"loaded {len(df)} rows and {len(df.columns)} columns")
            print("dataframe to parquet...")
            table = pa.Table.from_pandas(df, preserve_index=True)
            print(f"dumping parquet to {dump_path}")
            pq.write_table(table, dump_path)
            return True


def weather_api(**kwargs):
    api_reponse = get_api_response(kwargs["url"])
    return dump_api_response(api_reponse, kwargs["run_time"], kwargs["api_dump_path"])