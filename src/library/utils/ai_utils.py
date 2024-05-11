import numpy as np
from torch.utils.data import Dataset
import torch
from torch import nn
from copy import deepcopy
import pandas as pd


def make_ai_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Construct AI ready DataFrame
    :param df: pandas DataFrame that has Time index and NetSystemLoad, Prec, GRad columns and
    :return: df ready for AI training as specified by my own TDK (Sebők Mátyás)
    """
    import holidays
    holidays_hu = holidays.country_holidays('HU', years=list(range(df.index.min().year, df.index.max().year)))

    df['Holiday'] = df.index.map(lambda x: 1 if holidays_hu.get(x) else 0)
    df['Weekend'] = df.index.map(lambda x: 1 if x.weekday() >= 5 else 0)

    df['Hour'] = df.index.hour
    df['Weekday'] = df.index.weekday
    df['DayOfYear'] = df.index.dayofyear
    df['Month'] = df.index.month
    df['Year'] = df.index.year

    df['NetSystemLoadLag24'] = df['NetSystemLoad'].shift(24, fill_value=0)

    return df[['NetSystemLoad', 'Prec', 'GRad', 'Holiday', 'Weekend', 'Hour',
               'Weekday', 'DayOfYear', 'Month', 'Year', 'NetSystemLoadLag24']]


def mape(p, t):
    return np.mean(np.abs(p - t) / t)


def mpe(p, t):
    return np.mean((p - t) / t)


class TimeSeriesDataset(Dataset):
    """
    CPU dataset class for time series data
    """

    def __init__(self, x, y, seq_len=5, pred_len=1):
        """
        Initializes the dataset
        :param x: features
        :param y: what to predict
        :param seq_len: sequence length
        :param pred_len: prediction length
        """
        self.X = x
        self.y = y
        self.seq_len = seq_len
        self.pred_len = pred_len

    def __len__(self):
        """
        Returns the length of the dataset
        :return: length of dataset
        """
        return len(self.X) - self.seq_len - self.pred_len

    def __getitem__(self, idx):
        """
        Returns the item at given index
        :param idx: what index to return
        :return: (features, labels) of correct lengths specified in __init__()
        """
        return self.X[idx: idx + self.seq_len], self.y[idx + self.seq_len: idx + self.seq_len + self.pred_len]


class EarlyStopper:
    """Class implementing early stopping"""

    def __init__(self, patience=1, min_delta=0., model: nn.Module | None = None):
        """
        Initializes the early stopper
        :param patience: how many epochs to wait before stopping
        :param min_delta: what is the minimum delta to not consider as deterioration
        :param model: optional, used for checkpointing
        """
        self.patience: int = patience
        self.min_delta: float = min_delta
        self.counter: int = 0
        self.min_validation_loss: float = np.inf
        self.__model: nn.Module | None = model
        self.__state_dict = None

    def __call__(self, validation_loss):
        """
        Checks if we should stop
        :param validation_loss: current validation loss
        :return: True if we should stop, False otherwise
        """
        if validation_loss < self.min_validation_loss:
            self.min_validation_loss = validation_loss
            self.counter = 0
            if self.__model is not None:
                self.__state_dict = deepcopy(self.__model.state_dict())
        elif validation_loss > (self.min_validation_loss + self.min_delta):
            self.counter += 1
            if self.counter >= self.patience:
                return True
        return False

    def load_checkpoint(self):
        """
        Loads saved model checkpoint
        :return: None
        """
        if self.__model is not None and self.__state_dict is not None:
            with torch.no_grad():
                self.__model.load_state_dict(self.__state_dict)


class Grid:
    """
    Class implementing grid search using iterator protocol.
    """

    def __init__(self, grid: dict):
        """
        :param grid: dictionary of hyperparameters to search through, specify each key and a list of values
        """
        self._keys: list = list(grid.keys())
        self._values: list = list(grid.values())
        self._combinations: list = []

    def __iter__(self):
        """
        Chooses all combinations of hyperparameters without repetition, not taking order into account
        :return: self
        """
        self._combinations = self._values[0]
        if len(self._keys) > 1:
            self._combinations = [[comb] + [item] for item in self._values[1] for comb in self._combinations]
        if len(self._keys) > 2:
            for ls in self._values[2:]:
                self._combinations = [comb + [item] for item in ls for comb in self._combinations]

        return self

    def __next__(self):
        """
        Returns next combination of hyperparameters
        :return: next combination of hyperparameters as a dict
        """
        if len(self._combinations) > 0:
            return dict(zip(self._keys, self._combinations.pop(0)))
        else:
            raise StopIteration()
