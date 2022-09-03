"""
Functions for widgets (forms)
"""

from typing import List, Tuple

from ipywidgets import Dropdown, HTML, Output, Layout, Tab, Text, VBox


def dropdown_menu(label: str, default: str, option_list: List[Tuple[str, str]] = [], width: Tuple[str, str] = None):
    """Create a drop-down menu
    """
    description_width, menu_width = width

    # set label
    options = [(default, '')] if default else []

    # add options
    if option_list:
        options.extend(option_list)

    style = {'description_width': description_width} if description_width else Non
    layout = {'width': menu_width}

    return Dropdown(description=f"{label}: ", options=options, style=style, layout=layout)


def create_blank_menu(name: str = None, default: str = None, width: Tuple[str, str] = None):
    """空のセレクトメニューを作る

    Args
        name: label
        default: default value for the menu
        width: Tuple of label_width and menu_width in str
            label_width
            menu_width: 'max-content', 'initial'
    """
    return dropdown_menu(label=name, default=default, width=width)


def menu_for_credentials(json_files: dict):
    options = [('GAにアクセスする認証情報を選択してください', '')]

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


def input_text(value: str, placeholder: str, description: str, disabled: bool = False, width: Tuple[str, str] = None):
    description_width, menu_width = width
    style = {'description_width': description_width} if description_width else None
    layout = {'width': menu_width}

    return Text(
        value=value,
        placeholder=placeholder,
        description=description,
        style=style,
        layout=layout,
        disabled=disabled,
    )


def tab(ga: list):
    return VBox(ga)


def tab_set(tabs: list, titles: list):
    return Tab(
        children=tabs,
        _titles={i: v for i, v in enumerate(titles)}
    )
