# %%
from general_function import download_from_switch
from config import settings

# %%
download_from_switch(
    local_folder_path=".cache/input", switch_folder_path= "", 
    switch_link=settings.switch_link, switch_pass=settings.switch_pass) # type: ignore