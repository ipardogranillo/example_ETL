import os
import re
from numpy import outer
import pandas as pd
import pyarrow as pa
from pyarrow import parquet as pq
from pyarrow import dataset as ds

def enrich_current(**kwargs):
    """Enriches parquet data with the most recent csv data"""
    run_time = kwargs["run_time"]
    ext_path = kwargs["ext_path"]
    mun_dump_path = kwargs["mun_dump_path"] + str(run_time)
    dump_path = kwargs["curr_dump_path"] +str(run_time)
    if os.path.exists(dump_path):
        print("dump path already exists")
        return False
    else:
        print('getting most recent file')
        # search for a properly named directory
        # only digits, and most likely a dat
        print(os.listdir(ext_path))
        dirSearch = [
            d for d in os.listdir(ext_path)
            if bool(re.match('^[0-9]+$', d))
            and len(d) == 8
            and os.path.isdir(f"{ext_path}{d}/")
            and d >= "20220501"
            and d <= str(run_time)[:-2]
        ]
        if len(dirSearch) > 0:
            mostRecentDir = max(dirSearch)
            if not mostRecentDir:
                print("most recent folder couldn't be found")
                return False
            else:
                print(f'most recent folder from {mostRecentDir}')
                curr_df=pd.read_csv(f"{ext_path}{mostRecentDir}/data.csv")
                curr_df["esmun"] = curr_df["Cve_Ent"].astype(str) + "-" + curr_df["Cve_Mun"].astype(str)
                trim_df = curr_df[["esmun", "Value"]]
                print(trim_df)
                if os.path.exists(mun_dump_path):
                    print("loading municipalities table for run time")
                    mun_df = pd.DataFrame(
                        pq.read_table(mun_dump_path).to_pandas(),
                    )
                    print(mun_df)
                    print("performing join on esmun keys")
                    df = trim_df.merge(mun_df, how="outer", left_on="esmun", right_on="esmun")
                    print(df)
                    table = pa.Table.from_pandas(df, preserve_index=True)
                    print(table)
                    ds.write_dataset(
                        table,
                        dump_path,
                        format="parquet",
                        partitioning=ds.partitioning(
                            pa.schema([
                                ("run_time", pa.int32()),
                            ]),
                        ),
                        existing_data_behavior="overwrite_or_ignore",
                    )
                    return True
                else:
                    print("ERROR: no municipalities table could be found for run time!!!")
                    return False
        else:
            print('no names with YYYYMMDD format found')
            return False
