import numpy as np
from numpy import random
import polars as pl
import warnings


def state_to_value(state: int, bin_edges: np.array, last_state_scale: float) -> float: # type: ignore
    
    """_summary_

    state: _description_
    bin_edges: _description_
    :return: _description_
    """
    if state == len(bin_edges) - 1:
        return random.exponential(last_state_scale) + bin_edges[state]
    return random.uniform(bin_edges[state], bin_edges[state + 1])


def find_bin_edges(x: np.array, nb_states: int) -> np.array: # type: ignore
    """_summary_

    x: _description_
    nb_states: _description_
    :return: _description_
    """
    npt = len(x)
    return np.interp(
        np.linspace(0, npt, nb_states + 1),
        np.arange(npt),
        np.sort(x))


def transition_matrix(
    profiles: np.array, nb_states: int, hom_rep: bool= True, # type: ignore
    bound_transition: bool= True, transition_threshold: float = 0.60) -> np.array: # type: ignore
    """_summary_

    profiles: _description_
    nb_states: _description_
    :return: _description_
    """
    # Initialize transition matrix
    tran_matrix = np.zeros([nb_states, nb_states])
    if profiles.ndim == 1:
        profiles = profiles[np.newaxis, :] 
    
    # Increment transition matrix when current state i and furur
   
    for profile in profiles:
        profile = profile[~np.isnan(profile)].astype(int)
        for (i, j) in zip(profile, profile[1:]):
            tran_matrix[i][j] += 1
    # We need to filter zero division warning
    warnings.filterwarnings("ignore", category=RuntimeWarning)
    # Divide transition matrix by the sum of each row in order to get probability
    tran_matrix = tran_matrix / tran_matrix.sum(axis=0)
    # If states are not founded set uniformly transition probability
    
    mask = np.isnan(tran_matrix).any(axis=0)
    if hom_rep:
        tran_matrix[:, mask] = 1 / nb_states
    else:
        tran_matrix[:, mask] = np.tile(np.mean(tran_matrix[:, ~mask], axis=1), (mask.sum(), 1)).T
        
    tran_matrix = tran_matrix.transpose()
    if bound_transition:
        for idx in np.where(np.diagonal(tran_matrix) > transition_threshold)[0]:
            resi = tran_matrix[idx, idx] - transition_threshold
            tran_matrix[idx, idx] = transition_threshold
            if idx == 0:
                tran_matrix[idx, idx + 1] += resi
            elif idx == nb_states - 1:
                tran_matrix[idx, idx - 1] += resi
            else:
                tran_matrix[idx, idx - 1] += resi/2
                tran_matrix[idx, idx + 1] += resi/2 
        
    return tran_matrix

def digitize_col_with_custom_bin(col: pl.Expr, bin_edges: np.ndarray) -> pl.Expr:
    return col.map_elements(lambda x: np.digitize(x, bin_edges) - 1, return_dtype=pl.Int64).clip(0)