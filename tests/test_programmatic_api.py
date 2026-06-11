"""Tests for the programmatic (script/CI) public API added in 1.4.0:

- Megaton.for_property / Megaton.for_site
- Megaton.properties / Megaton.sites / Megaton.use_property
- GA4 composite filter dict ({"and"/"or"/"not": ...})
"""
from types import SimpleNamespace

import pytest

from megaton import errors, ga4
from megaton.start import Megaton


class _FakeSelector:
    def __init__(self):
        self.selected = None

    def select(self, id):
        self.selected = id


def _fake_ga_client(accounts):
    return SimpleNamespace(
        accounts=accounts,
        account=_FakeSelector(),
        property=_FakeSelector(),
    )


_ACCOUNTS = [
    {
        'id': 'acc-1',
        'name': 'Account One',
        'properties': [
            {'id': '111', 'name': 'Prop A'},
            {'id': '222', 'name': 'Prop B'},
        ],
    },
    {
        'id': 'acc-2',
        'name': 'Account Two',
        'properties': [
            {'id': '333', 'name': 'Prop C'},
        ],
    },
]


@pytest.fixture
def app(monkeypatch):
    def _fake_auth(self, credential=None, cache_key=None):
        self.ga['4'] = _fake_ga_client(_ACCOUNTS)

    monkeypatch.setattr(Megaton, 'auth', _fake_auth)
    return Megaton(None, headless=True)


def test_properties_flattens_accounts(app):
    props = app.properties()
    assert [p['id'] for p in props] == ['111', '222', '333']
    assert props[0]['account_id'] == 'acc-1'
    assert props[2]['account_name'] == 'Account Two'


def test_properties_empty_without_clients():
    app = Megaton(None, headless=True)
    assert app.properties() == []


def test_use_property_selects_account_and_property(app):
    result = app.use_property('333')
    assert result is app
    assert app.ga['4'].account.selected == 'acc-2'
    assert app.ga['4'].property.selected == '333'


def test_use_property_accepts_int_like_input(app):
    app.use_property(222)
    assert app.ga['4'].account.selected == 'acc-1'
    assert app.ga['4'].property.selected == '222'


def test_use_property_fast_switch_skips_metadata_refresh(app):
    app.use_property('333', refresh_metadata=False)
    # select() was NOT called; ids were assigned directly
    assert app.ga['4'].account.selected is None
    assert app.ga['4'].property.selected is None
    assert app.ga['4'].account.id == 'acc-2'
    assert app.ga['4'].property.id == '333'


def test_use_property_unknown_id_raises_with_available_list(app):
    with pytest.raises(ValueError) as exc:
        app.use_property('999')
    assert '999' in str(exc.value)
    assert '111' in str(exc.value)


def test_use_property_without_clients_raises_runtime_error():
    app = Megaton(None, headless=True)
    with pytest.raises(RuntimeError):
        app.use_property('111')


def test_for_property_returns_preselected_instance(monkeypatch):
    def _fake_auth(self, credential=None, cache_key=None):
        self.ga['4'] = _fake_ga_client(_ACCOUNTS)

    monkeypatch.setattr(Megaton, 'auth', _fake_auth)
    mg = Megaton.for_property('222')
    assert mg.headless is True
    assert mg.ga['4'].property.selected == '222'


def test_for_site_preselects_site(monkeypatch):
    mg = Megaton.for_site('https://example.com/')
    assert mg.headless is True
    assert mg.search.site == 'https://example.com/'


def test_sites_delegates_to_gsc_service(app):
    app._gsc_service = SimpleNamespace(
        list_sites=lambda: ['https://a.example/', 'sc-domain:b.example'])
    assert app.sites() == ['https://a.example/', 'sc-domain:b.example']


# --- composite filter dict ---

def _report():
    parent = SimpleNamespace(
        property=SimpleNamespace(
            id='123',
            api_metadata={
                'dimensions': [
                    {'api_name': 'date', 'display_name': 'Date'},
                    {'api_name': 'country', 'display_name': 'Country'},
                ],
                'metrics': [
                    {'api_name': 'eventCount', 'display_name': 'Event count'},
                ],
            },
        ),
        data_client=SimpleNamespace(),
    )
    return ga4.MegatonGA4.Report(parent)


def test_filter_dict_or_group():
    expr = _report()._format_filter({'or': ['country==Japan', 'country==Taiwan']})
    assert len(expr.or_group.expressions) == 2
    assert expr.or_group.expressions[0].filter.field_name == 'country'


def test_filter_dict_nested_and_or_not():
    expr = _report()._format_filter({
        'and': [
            'date==2024-01-01',
            {'or': ['country==Japan', 'country==Taiwan']},
            {'not': 'eventCount>100'},
        ],
    })
    children = expr.and_group.expressions
    assert len(children) == 3
    assert children[0].filter.field_name == 'date'
    assert len(children[1].or_group.expressions) == 2
    assert children[2].not_expression.filter.field_name == 'eventCount'


def test_filter_dict_single_item_collapses():
    expr = _report()._format_filter({'or': ['country==Japan']})
    assert expr.filter.field_name == 'country'


def test_filter_dict_leaf_string_with_and_semicolon():
    expr = _report()._format_filter({'or': ['date==2024-01-01;country==Japan',
                                            'eventCount>5']})
    children = expr.or_group.expressions
    assert len(children) == 2
    assert len(children[0].and_group.expressions) == 2


def test_filter_dict_invalid_key_raises():
    with pytest.raises(errors.BadRequest):
        _report()._format_filter({'xor': ['country==Japan']})


def test_filter_dict_multiple_keys_raises():
    with pytest.raises(errors.BadRequest):
        _report()._format_filter({'and': ['date==2024-01-01'],
                                  'or': ['country==Japan']})


def test_filter_dict_empty_list_raises():
    with pytest.raises(errors.BadRequest):
        _report()._format_filter({'and': []})


def test_filter_string_path_unchanged():
    expr = _report()._format_filter('date==2024-01-01;eventCount>10')
    assert len(expr.and_group.expressions) == 2
