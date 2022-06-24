"""
Functions for widgets (forms)
"""

from ipywidgets import Dropdown, Layout, Output
from typing import List, Tuple


def dropdown_menu(label: str, default: str, option_list: List[Tuple[str, str]] = []):
    """Create a drop-down menu
    """
    # set label
    options = [(default, '')]

    # add options
    if option_list:
        options.extend(option_list)

    return Dropdown(description=f"{label}: ", options=options)


def menu_for_credentials(json_files: dict):
    options = [('選択して', '')]

    options.append(('OAuth', ''))
    for f, p in json_files['OAuth'].items():
        options.append((f'-   {f}', p))

    options.append(('Service Account', ''))
    for f, p in json_files['Service Account'].items():
        options.append((f'-   {f}', p))

    # return dropdown_menu('JSON', '選択して', options)
    return Dropdown(
        description="JSON: ",
        options=options,
        layout=Layout(width='80%')
    )


#######
def create_menu(options: dict, label=None):
    opt = [(d['name'], d['id']) for d in options]
    return dropdown_menu(label, 'Please select', opt)


def create_ga_account_property_menu(accounts):
    # opt = [(d['name'], d['id']) for d in accounts]
    # menu1 = dropdown_menu('Account', 'Please select', opt)
    menu1 = create_menu(accounts, label='Account')
    menu2 = dropdown_menu('Property', '---')
    menu3 = dropdown_menu('View', '---')
    return menu1, menu2, menu3
