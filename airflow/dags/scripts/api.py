import os
import requests
import gzip
import json
import pandas as pd
import pyarrow as pa
from pyarrow import parquet as pq
from  pyarrow import dataset as ds


def get_api_response(url):
    
    # os.system("openssl genrsa -out client.key 4096")
    # os.system("openssl req -new -x509 -text -key client.key -out client.cert")

    print("creating requests session...")
    print("making request...")
    response = requests.get(url, verify=False)
    print(response.request)
    print(response.request.body)
    print(response.request.headers)
    print(response)
    print(response.content)
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
            print(table)
            part_path = dump_path  + str(run_time)
            if(os.path.exists(part_path)):
                print(f"partition already exists for run time {run_time}")
                return None
            else:
                print(f"dumping parquet to {dump_path}")
                print(
                    ds.write_dataset(
                        table,
                        dump_path,
                        format="parquet",
                        partitioning=ds.partitioning(
                            pa.schema([("run_time", pa.int32())]),
                        ),
                        existing_data_behavior="overwrite_or_ignore",
                    )
                )
                return True


def weather_api(**kwargs):
    api_reponse = get_api_response(kwargs["url"])
    return dump_api_response(api_reponse, kwargs["run_time"], kwargs["api_dump_path"])