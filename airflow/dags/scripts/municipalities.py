from genericpath import exists
import os
import pandas as pd
import pyarrow as pa
from pyarrow import parquet as pq
from pyarrow import dataset as ds


def append_muns(**kwargs):
    run_time = kwargs["run_time"]
    api_dump_path = kwargs["api_dump_path"]
    dump_path = kwargs["mun_dump_path"]
    curr_path = f"{api_dump_path}{str(run_time)}/"
    prev_path = f"{api_dump_path}{str(run_time - 1)}/"
    
    if os.path.exists(curr_path) and os.path.exists(prev_path):
        print("loading current and previous weather tables as dataframes...")
        curr_df = pd.DataFrame(
            pq.read_table(curr_path).to_pandas(),
            columns=["ides", "idmun", "tmin", "tmax"],
            )
        prev_df = pd.DataFrame(
            pq.read_table(prev_path).to_pandas(),
            columns=["ides", "idmun", "tmin", "tmax"],
        )

        print("adding composite es-mun key ")
        curr_df["esmun"] = curr_df["ides"] + "-" + curr_df["idmun"]
        prev_df["esmun"] = prev_df["ides"] + "-" + prev_df["idmun"]

        print("concatenating dataframes...")
        df = pd.concat([curr_df,prev_df])
        df["tmax"] = pd.to_numeric(df["tmax"])
        df["tmin"] = pd.to_numeric(df["tmin"])

        print("performing min and max average temperature calculation...")
        df = df.groupby("esmun").agg(avg_tmax=("tmax", "mean"),
                                            avg_tmin=("tmin", "mean")
        )
        print("adding run_time column...")
        df["run_time"] = [run_time] * len(df)
        print(df)

        print("converting to table...")
        df.set_index(["run_time"])
        table = pa.Table.from_pandas(df, preserve_index=True)

        part_path = dump_path + str(run_time)
        if os.path.exists(part_path):
            print(f"partition already exists for run time {run_time}")
            return False
        else:
            print(f"dumping to {part_path}")
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
    elif os.path.exists(curr_path):
        print("no previous tables could be found; using only current")
        curr_df = pd.DataFrame(
            pq.read_table(curr_path).to_pandas(),
            columns=["ides", "idmun", "tmin", "tmax"],
            )
        print("adding composite es-mun key ")
        curr_df["esmun"] = curr_df["ides"] + "-" + curr_df["idmun"]

        print("renaming min and max temperature as average...")
        curr_df.rename(columns={"tmax": "avg_tmax", "tmin": "avg_tmin"})

        print("adding run_time column...")
        curr_df["run_time"] = [run_time] * len(df)
        print(df)

        print("converting to table...")
        curr_df.set_index(["run_time"])
        table = pa.Table.from_pandas(df, preserve_index=True)

        part_path = dump_path + str(run_time)
        if os.path.exists(part_path):
            print(f"partition already exists for run time {run_time}")
            return False
        else:
            print(f"dumping to {part_path}")
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
        print("ERROR: no execution should've been triggered if no table exists!!!")
        return False
    return True
