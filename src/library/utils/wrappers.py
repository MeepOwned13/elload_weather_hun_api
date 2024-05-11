from .tsm_wrapper import TSMWrapper
from torch import nn
import torch
import numpy as np
from torch.utils.data import Dataset, DataLoader
from overrides import override

WRAPPERS_DEVICE = torch.device("cpu")
if torch.cuda.is_available():
    WRAPPERS_DEVICE = torch.device("cuda")


class MIMOTSWrapper(TSMWrapper):
    """
    Wraps the MIMO strategy for Time-series prediction.
    """

    def __init__(self, model: nn.Module, seq_len: int, pred_len: int):
        """
        Initializes the wrapper
        :param model: model to use
        :param seq_len: sequence length to use
        :param pred_len: length of predictions given
        """
        super(MIMOTSWrapper, self).__init__(model=model, seq_len=seq_len, pred_len=pred_len)

    # region override methods
    @override
    def init_strategy(self):
        super().init_strategy()

    @override
    def _setup_strategy(self, **kwargs):
        if WRAPPERS_DEVICE != torch.device('cpu'):
            torch.cuda.empty_cache()
        if kwargs.get('model', None) is not None:
            self._model = kwargs['model'](**kwargs).to(WRAPPERS_DEVICE)

    @override
    def train_strategy(self, x_train: np.ndarray, y_train: np.ndarray, x_val: np.ndarray, y_val: np.ndarray,
                       x_test: np.ndarray | None = None, y_test: np.ndarray | None = None, epochs=100, lr=0.001,
                       optimizer=None, batch_size=128, loss_fn=nn.MSELoss(), es_p=10, es_d=0.,
                       verbose=1, cp=False, **kwargs):

        train_dataset: Dataset = self._make_ts_dataset(x_train, y_train, store_norm_info=True)
        train_loader: DataLoader = DataLoader(train_dataset, batch_size=batch_size, shuffle=False)

        val_dataset: Dataset = self._make_ts_dataset(x_val, y_val)
        val_loader: DataLoader = DataLoader(val_dataset, batch_size=batch_size, shuffle=False)

        test_loader: DataLoader | None = None
        if x_test is not None and y_test is not None:
            test_dataset: Dataset = self._make_ts_dataset(x_test, y_test)
            test_loader: DataLoader = DataLoader(test_dataset, batch_size=batch_size, shuffle=False)

        return self._train_model(train_loader, val_loader, test_loader, epochs=epochs, lr=lr,
                                 optimizer=optimizer, loss_fn=loss_fn, es_p=es_p, es_d=es_d,
                                 verbose=verbose, cp=cp)

    @override
    def _predict_strategy(self, features: torch.Tensor, labels: torch.Tensor):
        features = features.to(WRAPPERS_DEVICE)
        preds = self._model(features)
        return preds.cpu().numpy(), labels.cpu().numpy()

    # endregion


class S2STSWrapper(MIMOTSWrapper):
    """
    Wraps the sequence-to-sequence strategy for Time-series prediction.
    """

    def __init__(self, model: nn.Module, seq_len: int, pred_len: int, teacher_forcing_decay=0.01):
        """
        Initializes the wrapper
        :param model: model to use
        :param seq_len: sequence length to use
        :param pred_len: length of predictions given
        :param teacher_forcing_decay: how fast teacher forcing should decay
        """
        super(S2STSWrapper, self).__init__(model, seq_len, pred_len)
        if pred_len <= 1:
            raise ValueError("pred_len must be greater than 1")
        self.teacher_forcing_ratio = 0.5
        self.teacher_forcing_decay = teacher_forcing_decay

    # region override methods

    @override
    def init_strategy(self):
        """
        Used to reset the strategy to its initial state.
        Used in cross validation. Should initialize the model.
        Resets teacher forcing ratio too.
        """
        super().init_strategy()
        self.teacher_forcing_ratio = 0.5

    @override
    def _train_epoch(self, data_loader: DataLoader, lr=0.001, optimizer=None, loss_fn=nn.MSELoss()):
        optimizer = optimizer or torch.optim.NAdam(self._model.parameters(), lr=lr)
        self.teacher_forcing_ratio = max(0.0, self.teacher_forcing_ratio - self.teacher_forcing_decay)

        self._model.train()
        total_loss: float = 0

        for features, labels in data_loader:
            features = features.to(WRAPPERS_DEVICE)
            labels = labels.to(WRAPPERS_DEVICE)

            optimizer.zero_grad()
            outputs = self._model(features, labels, self.teacher_forcing_ratio)
            loss = loss_fn(outputs, labels)
            loss.backward()
            optimizer.step()

            total_loss += loss.item()
        return total_loss / len(data_loader)

    # endregion
