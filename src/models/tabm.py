"""Official ICLR 2025 TabM adapted to chronological opportunity prediction.

The loss is applied to each implicit member separately; inference averages
probabilities (classification) or predictions (regression), never logits.
No validation data are used to fit preprocessing or target normalization.
"""
import copy
import os
from time import perf_counter

os.environ.setdefault('CUBLAS_WORKSPACE_CONFIG', ':4096:8')

import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import OneHotEncoder, QuantileTransformer
import torch
from torch import nn
from tabm import TabM


def candidates():
    """Eight bounded configurations, declared before outer evaluation.

    k=32 follows the paper. The smaller width range is a compute-bounded
    adaptation, not a replication of the paper's 50-100 trial Optuna search.
    """
    return [dict(n_blocks=b, d_block=d, dropout=p, lr=lr, weight_decay=wd)
            for b,d,p,lr,wd in [
                (2,128,.1,.002,.0003), (1,64,0.,.002,.0003),
                (2,64,.1,.001,.001), (2,128,.2,.001,.01),
                (3,128,.1,.001,.0003), (2,256,.2,.002,.001),
                (1,128,.1,.0005,.01), (3,64,.2,.002,.01)]]


def member_loss(logits, target, task):
    """Independent submodel losses, not loss of the ensemble prediction."""
    truth=target[:,None].expand_as(logits)
    return (nn.functional.binary_cross_entropy_with_logits(logits,truth)
            if task=='classifier' else nn.functional.mse_loss(logits,truth))


def aggregate(logits, task):
    return (logits.sigmoid() if task=='classifier' else logits).mean(dim=1)


class TabMEstimator(BaseEstimator):
    def __init__(self,task='classifier',params=None,seed=2026,k=32,
                 max_epochs=100,patience=16,batch_size=256,device='cpu'):
        self.task,self.params,self.seed,self.k=task,params,seed,k
        self.max_epochs,self.patience=max_epochs,patience
        self.batch_size,self.device=batch_size,device

    def _frame(self,x):
        if list(x.columns)!=list(self.feature_names_in_):
            raise ValueError('TabM feature order/schema differs from fitted schema')
        result=x.copy()
        for c in self.numeric_:
            result[c]=pd.to_numeric(result[c],errors='coerce').replace([np.inf,-np.inf],np.nan)
        for c in self.categorical_:
            result[c]=result[c].fillna('__missing__').astype(str)
        return result

    def fit(self,x,y,validation=None,epochs=None):
        if self.task not in ('classifier','regression'):
            raise ValueError(self.task)
        y=np.asarray(y,dtype=np.float32)
        if len(x)!=len(y) or not len(y) or not np.isfinite(y).all():
            raise ValueError('Finite known training targets required')
        if self.task=='classifier' and not np.isin(y,[0,1]).all():
            raise ValueError('Binary targets required')
        if self.task=='regression' and (y<0).any():
            raise ValueError('Nonnegative opportunity values required')
        if epochs is not None and (epochs<1 or validation is not None):
            raise ValueError('Fixed-epoch refits must not see validation outcomes')
        started=perf_counter()
        self.feature_names_in_=np.asarray(x.columns,dtype=object)
        self.categorical_=[c for c in x if c in ('m_event_type','m_topic')]
        self.numeric_=[c for c in x if c not in self.categorical_]
        x=self._frame(x)
        binary=[c for c in self.numeric_ if set(x[c].dropna().unique()).issubset({0.,1.})]
        continuous=[c for c in self.numeric_ if c not in binary]
        transforms=[]
        if continuous:
            transforms.append(('continuous',make_pipeline(
                SimpleImputer(strategy='median',keep_empty_features=True),
                QuantileTransformer(n_quantiles=min(1000,len(x)),output_distribution='normal',
                                    subsample=None,random_state=self.seed)),continuous))
        if binary:
            transforms.append(('binary',SimpleImputer(strategy='most_frequent',keep_empty_features=True),binary))
        if self.categorical_:
            transforms.append(('categories',OneHotEncoder(handle_unknown='ignore',sparse_output=False),self.categorical_))
        self.preprocessor_=ColumnTransformer(transforms,sparse_threshold=0.)
        array=np.asarray(self.preprocessor_.fit_transform(x),dtype=np.float32)
        self.input_width_=array.shape[1]
        self.training_rows_=len(y)
        self.target_mean_=float(y.mean()) if self.task=='regression' else 0.
        self.target_scale_=max(float(y.std()),1e-6) if self.task=='regression' else 1.
        self.constant_=float(y[0]) if np.unique(y).size==1 else None
        self.history_=[]
        self.best_epoch_=0
        self.epochs_run_=0
        self.parameter_count_=0
        self.model_=None
        if self.constant_ is not None:
            self.fit_seconds_=perf_counter()-started
            return self
        torch.manual_seed(self.seed)
        if torch.cuda.is_available():
            torch.cuda.manual_seed_all(self.seed)
        torch.use_deterministic_algorithms(True)
        torch.backends.cuda.matmul.allow_tf32=False
        params=dict(self.params or candidates()[0])
        self.model_=TabM.make(n_num_features=self.input_width_,d_out=1,k=self.k,
                             arch_type='tabm',**{c:params[c] for c in ('n_blocks','d_block','dropout')}).to(self.device)
        self.parameter_count_=sum(p.numel() for p in self.model_.parameters())
        optimizer=torch.optim.AdamW(self.model_.parameters(),lr=params['lr'],weight_decay=params['weight_decay'])
        tx=torch.as_tensor(array,device=self.device)
        ty=torch.as_tensor((y-self.target_mean_)/self.target_scale_,device=self.device)
        vx=vy=None
        if validation is not None:
            valid_x,valid_y=validation
            valid_y=np.asarray(valid_y,dtype=np.float32)
            if not len(valid_y) or not np.isfinite(valid_y).all():
                raise ValueError('Early stopping requires known validation labels')
            vx=torch.as_tensor(np.asarray(self.preprocessor_.transform(self._frame(valid_x)),dtype=np.float32),device=self.device)
            vy=torch.as_tensor((valid_y-self.target_mean_)/self.target_scale_,device=self.device)
        rng=np.random.default_rng(self.seed)
        best=float('inf')
        best_state=None
        limit=epochs if epochs is not None else self.max_epochs
        for epoch in range(1,limit+1):
            self.model_.train()
            for positions in np.array_split(rng.permutation(len(tx)),max(1,int(np.ceil(len(tx)/self.batch_size)))):
                ix=torch.as_tensor(positions,device=self.device)
                optimizer.zero_grad(set_to_none=True)
                loss=member_loss(self.model_(tx[ix]).squeeze(-1),ty[ix],self.task)
                if not torch.isfinite(loss):
                    raise ValueError('Nonfinite TabM training loss')
                loss.backward()
                nn.utils.clip_grad_norm_(self.model_.parameters(),1.)
                optimizer.step()
            self.epochs_run_=epoch
            if vx is not None:
                self.model_.eval()
                with torch.inference_mode():
                    score=torch.cat([aggregate(self.model_(batch).squeeze(-1),self.task)
                                     for batch in vx.split(1024)])
                    val_loss=float((nn.functional.binary_cross_entropy(score.clamp(1e-7,1-1e-7),vy)
                                    if self.task=='classifier' else nn.functional.mse_loss(score,vy)).item())
                self.history_.append(dict(epoch=epoch,validation_loss=val_loss))
                if val_loss<best-1e-8:
                    best,self.best_epoch_=val_loss,epoch
                    best_state=copy.deepcopy(self.model_.state_dict())
                elif epoch-self.best_epoch_>=self.patience:
                    break
        if best_state is not None:
            self.model_.load_state_dict(best_state)
        else:
            self.best_epoch_=limit
        # Portable CPU artifacts; screening inference is benchmarked on CPU.
        self.model_.to('cpu').eval()
        self.fit_seconds_=perf_counter()-started
        return self

    def predict(self,x):
        x=self._frame(x)
        if self.constant_ is not None:
            return np.full(len(x),self.constant_,dtype=float)
        tx=torch.as_tensor(np.asarray(self.preprocessor_.transform(x),dtype=np.float32))
        with torch.inference_mode():
            score=torch.cat([aggregate(self.model_(batch).squeeze(-1),self.task)
                             for batch in tx.split(1024)]).numpy() if len(tx) else np.empty(0)
        return (np.maximum(0.,score*self.target_scale_+self.target_mean_)
                if self.task=='regression' else score)

    def predict_proba(self,x):
        if self.task!='classifier':
            raise ValueError('Only classification produces probabilities')
        score=self.predict(x)
        return np.column_stack([1-score,score])


class TabMHurdle(BaseEstimator):
    """Frozen unweighted occurrence gate times positive-only amount model."""
    def __init__(self,gate,amount):
        self.gate,self.amount=gate,amount
        self.feature_names_in_=gate.feature_names_in_

    def components(self,x):
        return self.gate.predict(x),self.amount.predict(x)

    def predict(self,x):
        probability,value=self.components(x)
        return probability*value
