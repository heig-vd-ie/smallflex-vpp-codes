"""
:math:`S\_H` and :math:`S\_B` include subset to specify the corresponding hydro powerplants and basins that the 
state is associated with, respectively. The sets are constructed as demonstrated in the following example.

:math:`S\_H =\\begin{cases} 1: \left[1, 2\\right] \\\\ 2: \left[4, 5, 6\\right]  \\\\ 3: \left[6\\right] \\end{cases}`

To collect all states associated with a basin :math:`b` we can use the notation :math:`S\_B\{B\}`

In a Pyomo model, it is not possible to directly index variables and parameters using sets that contain subsets, 
such as :math:`S\_H`  To handle this limitation, we need to create new sets, and :math:`S\_B`, which will explicitly 
represent the deployment of these subsets. These new sets will be structured to map the relationships required for 
indexing variables and parameters in the model effectively.

:math:`HS \in \{s, s\_h\}=\{(1,~1),~(1,~2),~(2,~3),~(2,~4),~(2,~5),~(3,~6)\}` 


The set :math:`S\_BH` defines the connections between each basin, its corresponding state, and the hydro powerplants. 
This link is established solely between a hydro powerplant and its associated upstream basin. It is assumed that the 
water level in downstream basins does not affect the behavior of turbined or pumped energy.

"""

import pyomo.environ as pyo

def baseline_sets(model):
    model.T = pyo.Set()
    model.H = pyo.Set()
    model.B = pyo.Set()
    # index gathering the state per basin and the hydro powerplants
    model.S_b = pyo.Set(model.B)
    model.S_h = pyo.Set(model.H)
    # index gathering the state of every basin and hydro powerplants
    model.BS = pyo.Set(dimen=2, initialize=lambda model: [(b, s_b) for b in model.B for s_b in model.S_b[b]])
    model.HS = pyo.Set(dimen=2, initialize=lambda model: [(h, s_h) for h in model.H for s_h in model.S_h[h]])
    # index (gathering h, b, s_h, s_b) to make the correspondence between the state of basin and hydro powerplants
    model.S_BH = pyo.Set(dimen=4) 
    
    return model
