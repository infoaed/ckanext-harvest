'''
Tests for ckanext/harvest/harvesters/base.py
'''
from nose.tools import assert_equal
from ckanext.harvest.harvesters.base import munge_tags, HarvesterBase


class TestMungeTags:

    def test_basic(self):
        pkg = {'tags': [{'name': 'river quality'},
                        {'name': 'Geo'}]}
        munge_tags(pkg)
        assert_equal(pkg['tags'], [{'name': 'river-quality'},
                                   {'name': 'geo'}])

    def test_blank(self):
        pkg = {'tags': [{'name': ''},
                        {'name': 'Geo'}]}
        munge_tags(pkg)
        assert_equal(pkg['tags'], [{'name': 'geo'}])

    def test_replaced(self):
        pkg = {'tags': [{'name': '*'},
                        {'name': 'Geo'}]}
        munge_tags(pkg)
        assert_equal(pkg['tags'], [{'name': 'geo'}])


def test_extras_from_dict():
    res = HarvesterBase.extras_from_dict({'theme': 'environment', 'freq': 'daily'})
    assert_equal(res, [{'key': 'theme', 'value': 'environment'},
                       {'key': 'freq', 'value': 'daily'}])
