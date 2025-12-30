"""
Functions for widgets (forms)
"""

from typing import List, Tuple

_IMPORT_ERROR = None
try:
    import ipywidgets as _ipywidgets
except Exception as exc:
    _ipywidgets = None
    _IMPORT_ERROR = exc


class WidgetsUnavailableError(ImportError):
    """Raised when ipywidgets is required but unavailable."""


def require_widgets():
    global _ipywidgets, _IMPORT_ERROR
    if _ipywidgets is not None:
        return _ipywidgets
    try:
        import ipywidgets as _ipywidgets  # noqa: F811
    except Exception as exc:
        _IMPORT_ERROR = exc
        raise WidgetsUnavailableError(
            "ipywidgets is required for UI. Install ipywidgets or use headless=True."
        ) from exc
    return _ipywidgets


def dropdown_menu(label: str, default: str, option_list: List[Tuple[str, str]] = [], width: Tuple[str, str] = None):
    """Create a drop-down menu
    """
    widgets = require_widgets()
    Dropdown = widgets.Dropdown
    description_width, menu_width = (None, None)
    if width:
        description_width, menu_width = width

    # set label
    options = [(default, '')] if default else []

    # add options
    if option_list:
        options.extend(option_list)

    style = {'description_width': description_width} if description_width else {}
    layout = {'width': menu_width} if menu_width else {}

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
    widgets = require_widgets()
    Dropdown = widgets.Dropdown
    Layout = widgets.Layout
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
    widgets = require_widgets()
    HTML = widgets.HTML
    return HTML(
        value=value,
        placeholder=placeholder,
        description=description
    )


def input_text(value: str, placeholder: str, description: str, disabled: bool = False, width: Tuple[str, str] = None):
    widgets = require_widgets()
    Text = widgets.Text
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
    widgets = require_widgets()
    VBox = widgets.VBox
    return VBox(ga)


def tab_set(tabs: list, titles: list):
    widgets = require_widgets()
    Tab = widgets.Tab
    tab_widget = Tab(children=tabs)
    for i, title in enumerate(titles):
        tab_widget.set_title(i, title)
    return tab_widget


def Output(*args, **kwargs):
    widgets = require_widgets()
    return widgets.Output(*args, **kwargs)


__all__ = [
    "WidgetsUnavailableError",
    "require_widgets",
    "dropdown_menu",
    "create_blank_menu",
    "menu_for_credentials",
    "html_text",
    "input_text",
    "tab",
    "tab_set",
    "Output",
]
