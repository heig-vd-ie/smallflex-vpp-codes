r"""
The sets :math:`T`, :math:`S_H` and :math:`S_B` are arranged in a specific order such that indexing a variable with 
the first or last element of a set (i.e :math:`t^{0}` and :math:`t^{\text{END}} \in T`) corresponds respectively to 
the lowest/first or highest/final element of that variable.

:math:`S_H` and :math:`S_B` include subset to specify the corresponding hydro powerplants and basins that the 
state is associated with, respectively. The sets are constructed as demonstrated in the following example. To collect 
all states associated with a basin :math:`b` we can use the notation :math:`S_B\{b\}`

:math:`S_B =\begin{cases} 1: \left[1, 2, 3\right] \\ 2: \left[4, 5\right]  \\ 3: \left[6\right] \end{cases}`

In a Pyomo model, it is not possible to directly index variables and parameters using sets that contain subsets, 
such as :math:`S\_B`. To handle this limitation, we need to create new sets (i.e. :math:`BS`) which will explicitly 
represent the deployment of these subsets. These new sets will be structured to map the relationships required for 
indexing variables and parameters in the model effectively.

:math:`SB \in \{b, s_b\}=\{(1,~1),~(1,~2),~(2,~3),~(2,~4),~(2,~5),~(3,~6)\}` 


The set :math:`S\_BH` defines the connections between each basin, its corresponding state, and the hydro powerplants. 
This link is established solely between a hydro powerplant and its associated upstream basin. It is assumed that the 
water level in downstream basins does not affect the behavior of turbined or pumped energy.

:math:`S_{BH} \in \{b, h\, s_b, s_h\}=\{(1, ~1, ~1, ~1),~(1, ~1, ~2, ~2),~(1, ~1, ~3, ~3)\}` 

"""

import pyomo.environ as pyo

def first_stage_sets(model):
    model.T = pyo.Set()
    model.H = pyo.Set()
    model.B = pyo.Set()
    # subset of hydro powerplants with discrete and continuous control
    model.DH = pyo.Set(within=model.H)
    model.CH = pyo.Set(initialize=lambda m: [h for h in m.H if h not in m.DH])
    # index gathering the state per basin and the hydro powerplants
    model.S_B = pyo.Set(model.B)
    model.S_H = pyo.Set(model.H)
    # index gathering the state of every basin and hydro powerplants
    model.BS = pyo.Set(dimen=2, initialize=lambda model: [(b, s_b) for b in model.B for s_b in model.S_B[b]])
    model.HS = pyo.Set(dimen=2, initialize=lambda model: [(h, s_h) for h in model.H for s_h in model.S_H[h]])
    # index (gathering h, b, s_h, s_b) to make the correspondence between the state of basin and hydro powerplants
    model.HBS = pyo.Set(dimen=3) 
    
    return model
