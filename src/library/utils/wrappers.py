from tsm_wrapper import TSMWrapper
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


class S2STSWRAPPER(MIMOTSWrapper):
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
        super(S2STSWRAPPER, self).__init__(model, seq_len, pred_len)
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


class RECOneModelTSWrapper(MIMOTSWrapper):
    """
    Wraps single model recursive strategy for Time-series prediction.
    """

    def __init__(self, model: nn.Module, seq_len: int, pred_len: int, main_feature=0, teacher_forcing_decay=0.02):
        """
        Initializes the wrapper
        :param model: model to use
        :param seq_len: sequence length to use
        :param pred_len: length of predictions given
        :param main_feature: which feature to predict and evaluate for
        :param teacher_forcing_decay: how fast teacher forcing should decay
        """
        super(RECOneModelTSWrapper, self).__init__(model, seq_len=seq_len, pred_len=pred_len)
        self._teacher_forcing = self._og_tf = 1.0
        self._teacher_forcing_decay = teacher_forcing_decay
        self._main_feature = main_feature

    # region override methods

    @override
    def init_strategy(self):
        """
        Used to reset the strategy to its initial state.
        Used in cross validation. Should initialize the model.
        Resets teacher forcing ratio too.
        """
        super().init_strategy()
        self._teacher_forcing = self._og_tf

    @override
    def _train_epoch(self, data_loader: DataLoader, lr=0.001, optimizer=None, loss_fn=nn.MSELoss()):
        optimizer = optimizer or torch.optim.NAdam(self._model.parameters(), lr=lr)

        self._teacher_forcing = max(0.0, self._teacher_forcing - 0.02)
        self._model.train()
        total_loss: float = 0

        for features, labels in data_loader:
            for i in range(self._pred_len):
                features = features.to(WRAPPERS_DEVICE)
                labels = labels.to(WRAPPERS_DEVICE)

                optimizer.zero_grad()
                outputs = self._model(features)
                loss = loss_fn(outputs, labels[:, i])
                loss.backward()
                optimizer.step()

                total_loss += loss.item()

                features = torch.cat((features[:, 1:], labels[:, i].unsqueeze(1)), dim=1)
                if torch.rand(1) > self._teacher_forcing:
                    features[:, -1] = outputs.detach()

        return total_loss / len(data_loader)

    @override
    def _test_model(self, data_loader: DataLoader, loss_fn=nn.MSELoss()):
        self._model.eval()
        total_loss: float = 0

        with torch.no_grad():
            for features, labels in data_loader:
                for i in range(self._pred_len):
                    features = features.to(WRAPPERS_DEVICE)
                    labels = labels.to(WRAPPERS_DEVICE)

                    outputs = self._model(features)
                    loss = loss_fn(outputs, labels[:, i])

                    total_loss += loss.item()

                    features = torch.cat((features[:, 1:], outputs.unsqueeze(1)), dim=1)
        return total_loss / len(data_loader)

    @override
    def _predict_strategy(self, features: torch.Tensor, labels: torch.Tensor):
        preds = torch.zeros((features.shape[0], self._pred_len))
        for i in range(self._pred_len):
            features = features.to(WRAPPERS_DEVICE)
            labels = labels.to(WRAPPERS_DEVICE)

            outputs = self._model(features)
            preds[:, i] = outputs[:, self._main_feature]

            features = torch.cat((features[:, 1:], outputs.unsqueeze(1)), dim=1)
        return preds.cpu().numpy(), labels[:, :, self._main_feature].cpu().numpy()

    # endregion


class RECMultiModelTSWrapper(MIMOTSWrapper):
    """
    Wraps multi-model recursive strategy for Time-series prediction.
    """

    def __init__(self, model: nn.Module, seq_len: int, pred_len: int, pred_first_n: int, teacher_forcing_decay=0.02):
        """
        Initializes the wrapper
        :param model: model to use
        :param seq_len: sequence length to use
        :param pred_len: length of predictions given
        :param pred_first_n: first how many features the model predicts, rest are pulled from labels
        :param teacher_forcing_decay: how fast teacher forcing should decay
        """
        super(RECMultiModelTSWrapper, self).__init__(model=model, seq_len=seq_len, pred_len=pred_len)
        if pred_len <= 1:
            raise ValueError("pred_len must be greater than 1")
        self._pred_first_n = pred_first_n
        self._teacher_forcing = self._og_tf = 1.0
        self._teacher_forcing_decay = teacher_forcing_decay

    # region override methods

    @override
    def init_strategy(self):
        super().init_strategy()
        self._teacher_forcing = self._og_tf

    @override
    def _train_epoch(self, data_loader: DataLoader, lr=0.001, optimizer=None, loss_fn=nn.MSELoss()):
        optimizer = optimizer or torch.optim.NAdam(self._model.parameters(), lr=lr)

        self._teacher_forcing = max(0.0, self._teacher_forcing - self._teacher_forcing_decay)
        self._model.train()
        total_loss: float = 0

        for features, labels in data_loader:
            features = features.to(WRAPPERS_DEVICE)
            labels = labels.to(WRAPPERS_DEVICE)

            optimizer.zero_grad()
            outputs = self._model(features, labels, self._teacher_forcing)
            loss = loss_fn(outputs, labels[:, :, 0])
            loss.backward()
            optimizer.step()

            total_loss += loss.item()
        return total_loss / len(data_loader)

    @override
    def _test_model(self, data_loader: DataLoader, loss_fn=nn.MSELoss()):
        self._model.eval()
        total_loss: float = 0

        with torch.no_grad():
            for features, labels in data_loader:
                features = features.to(WRAPPERS_DEVICE)
                labels = labels.to(WRAPPERS_DEVICE)

                outputs = self._model(features, labels[:, :, self._pred_first_n:])
                loss = loss_fn(outputs, labels[:, :, 0])

                total_loss += loss.item()
        return total_loss / len(data_loader)

    @override
    def _predict_strategy(self, features: torch.Tensor, labels: torch.Tensor):
        preds = torch.zeros((features.shape[0], self._pred_len))
        for i in range(self._pred_len):
            features = features.to(WRAPPERS_DEVICE)
            labels = labels.to(WRAPPERS_DEVICE)

            preds = self._model(features, labels[:, :, self._pred_first_n:])
        return preds.cpu().numpy(), labels[:, :, 0].cpu().numpy()

    # endregion
