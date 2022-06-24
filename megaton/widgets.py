"""
Functions for widgets (forms)
"""

from ipywidgets import Dropdown, HTML, Layout, Output, Tab, Text, VBox
from typing import List, Tuple


def dropdown_menu(label: str, default: str, option_list: List[Tuple[str, str]] = []):
    """Create a drop-down menu
    """
    # set label
    options = [(default, '')] if default else []

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


def html_text(value: str, placeholder: str, description: str):
    return HTML(
        value=value,
        placeholder=placeholder,
        description=description
    )


def input_text(value: str, placeholder: str, description: str, disabled: bool = False):
    return Text(
        value=value,
        placeholder=placeholder,
        description=description,
        disabled=disabled
    )


# def tag_for_ga(ga: dict):
#     tab1 = VBox([input_text("tab1", "please", "this")])
#     tab2 = VBox([input_text("tab2", "please", "this")])
#     tab_set = Tab([tab1, tab2])
#     tab_set.set_title(0, 'UA')
#     tab_set.set_title(1, 'GA4')
#     tab_set.selected_index = 1
#
#     return tab_set


def create_blank_menu(name: str = None, default: str = None):
    """disable化された空のセレクトメニューを作る"""
    return dropdown_menu(label=name, default=default)

#######
# def create_menu(options: dict, label=None):
#     opt = [(d['name'], d['id']) for d in options]
#     return dropdown_menu(label, 'Please select', opt)
#
#
# def create_ga_account_property_menu(accounts):
#     # opt = [(d['name'], d['id']) for d in accounts]
#     # menu1 = dropdown_menu('Account', 'Please select', opt)
#     menu1 = create_menu(accounts, label='Account')
#     menu2 = dropdown_menu('Property', '---')
#     menu3 = dropdown_menu('View', '---')
#     return menu1, menu2, menu3
