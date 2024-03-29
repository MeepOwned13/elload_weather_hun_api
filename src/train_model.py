import mysql.connector as connector
from dotenv import dotenv_values
import pandas as pd
import numpy as np
from warnings import filterwarnings
from library.utils.ai_utils import make_ai_df
from library.utils.torch_model_definitions import Seq2seq
from library.utils.wrappers import S2STSWrapper
from pathlib import Path
import argparse
import sys
from copy import deepcopy

if __name__ == '__main__':
    # Known Warning with pd.read_sql, all cases that are required tested and working
    filterwarnings("ignore", category=UserWarning, message='.*pandas only supports SQLAlchemy connectable.*')

    parser = argparse.ArgumentParser(description="Train model for API's predictions")
    parser.add_argument("-y", "--year", help="Which year should the model be ready for?", type=int, required=True)
    parser.add_argument("-np", "--no_plot", help="Don't plot losses after training", action="store_true")
    parser.add_argument("-tc", "--train_cycles",
                        help="How many training cycles to attempt (saves best performer)", type=int, default=3)
    args = parser.parse_args()

    db_connect_info = dotenv_values(".env")
    db_connect_info = {
        "host": db_connect_info["HOST"],
        "user": db_connect_info["USER"],
        "password": db_connect_info["PASW"],
        "database": db_connect_info["DBNM"]
    }

    try:
        con: connector.CMySQLConnection = connector.connect(**db_connect_info)
        df: pd.DataFrame = pd.read_sql("SELECT Time, NetSystemLoad, Prec, GRad FROM AI_1hour", con=con)
        df.set_index("Time", inplace=True, drop=True)
    finally:
        if con:
            con.close()

    min_year = df.index.min().year + 1
    max_year = df.index.max().year +\
        (1 if df.index.max().month > 11 else 0)  # if next year is close enough we can train
    if args.year < min_year or args.year > max_year:
        print(f"Model can only be trained for years in range [{min_year}, {max_year}] with data from AI_1hour table",
              file=sys.stderr)
        exit(1)

    df = make_ai_df(df)

    train = df[:f"{args.year-1}-09-30 23:00:00"]
    val = df[f"{args.year-1}-10-01 0:00:00":f"{args.year-1}-12-31 23:00:00"]

    x_train = train.to_numpy(dtype=np.float32)
    y_train = train["NetSystemLoad"].to_numpy(dtype=np.float32)
    x_val = val.to_numpy(dtype=np.float32)
    y_val = val["NetSystemLoad"].to_numpy(dtype=np.float32)

    name = f"seq2seq_{args.year}.pth"
    path = Path(f"{__file__}/../../models").resolve()
    path.mkdir(parents=True, exist_ok=True)
    path = path / name

    best_val_loss = 500_000  # More than enough with normalization
    saved_losses = None
    for i in range(1, args.train_cycles + 1):
        wrapper = S2STSWrapper(Seq2seq(11, 3, 10, 1, True, 0.5, 0.05), 24, 3)
        print(f"Training model {i}....")
        losses = wrapper.train_strategy(x_train, y_train, x_val, y_val, epochs=1000,
                                        lr=0.001, batch_size=2048, es_p=20, cp=True)

        end_val_loss = min(losses[1][-21:])  # early stop patience is 20, model checkpoints back to best one
        if best_val_loss > end_val_loss:
            print(f"Training {i}. performed best so far, saving model to {path}")
            wrapper.save_state(path)
            best_val_loss = end_val_loss
            saved_losses = deepcopy(losses)

    if not args.no_plot:
        print("Plotting losses for best training")
        wrapper.plot_losses([saved_losses[0]], [saved_losses[1]], [[]])

    print("Model saved")

