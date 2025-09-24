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

:math:`SB \in \{b, s\}=\{(1,~1),~(1,~2),~(2,~3),~(2,~4),~(2,~5),~(3,~6)\}` 


The set :math:`HBS` defines the connections between each basin, its corresponding state, and the hydro powerplants. 
This link is established solely between a hydro powerplant and its associated upstream basin. It is assumed that the 
water level in downstream basins does not affect the behavior of turbined or pumped energy.

:math:`HBS \in \{b, ~h, ~s\}=\{(1, ~1, ~1),~(1, ~1, ~2),~(1, ~1, ~3,)\}`

"""

from xml.parsers.expat import model
import pyomo.environ as pyo

def first_stage_sets(model):
    model.T = pyo.Set()
    model.Ω = pyo.Set()
    model.H = pyo.Set()
    model.B = pyo.Set()
    
    # subset of hydro powerplants with discrete and continuous control
    model.DH = pyo.Set(within=model.H)
    model.CH = pyo.Set(initialize=lambda m: [h for h in m.H if h not in m.DH])
    model.HB = pyo.Set(dimen=2)
    
    return model
