import pytest
from readers import base
from pathlib import Path
from copy import deepcopy
from typing import List


class TestCheckSingle:
    paths = (Path('/test_dir/test_file_1.foo'), Path('/test_dir/test_file_2.bar'))

    def test_single(self):
        assert base.check_single(self.paths[0]) == self.paths[0]

    def test_fails_on_multi(self):
        with pytest.raises(ValueError):
            base.check_single(self.paths[0], self.paths[1])


class TestIsArray:
    array_name = 'abc[]'
    not_array_name = 'abc'
    invalid_name = ''

    def test_valid_array_name(self):
        assert base.is_array_key(self.array_name)

    def test_not_array_name(self):
        assert not base.is_array_key(self.not_array_name)

    def test_invalid_array_name(self):
        assert not base.is_array_key(self.invalid_name)


class TestChangeArrayKey:
    array_name = 'abc[]'
    not_array_name = 'abc'
    invalid_name = ''

    def test_change_valid_array_name(self):
        assert base.change_array_key(self.array_name) == 'abc'

    def test_change_none_array_name(self):
        with pytest.raises(ValueError):
            base.change_array_key(self.not_array_name)

    def test_change_invalid_name(self):
        with pytest.raises(ValueError):
            base.change_array_key(self.invalid_name)


class TestMergeDict:
    merge_dict = {'lvl_1': {'k1': 'v1',
                            'lvl_2': {'k2': 'v2'}}}
    lvl_1_merge = {'lvl_1': {'k3': 'v3'}}
    lvl_2_merge = {'lvl_1': {'lvl_2': {'k4': 'v4'}}}
    lvl_1_merge_multi_key = {'lvl_1': {'k5': 'v5', 'k6': 'v6'}}

    # currently, merge_dict doesn't recieve data of the following shape type
    lvl_1_and_2_merge = {'lvl_1': {'k7': 'v7', 'lvl_2': {'k8': 'v8'}}}

    def test_lvl_1_merge(self):
        merge_dict = deepcopy(self.merge_dict)
        assert base.merge_dict(self.lvl_1_merge, merge_dict) == {'lvl_1': {'k3': 'v3', 'k1': 'v1',
                                                                           'lvl_2': {'k2': 'v2'}}}

    def test_lvl_2_merge(self):
        merge_dict = deepcopy(self.merge_dict)
        assert base.merge_dict(self.lvl_2_merge, merge_dict) == {'lvl_1': {'k1': 'v1',
                                                                           'lvl_2': {'k2': 'v2', 'k4': 'v4'}}}

    def test_multi_key_same_lvl(self):
        merge_dict = deepcopy(self.merge_dict)
        assert base.merge_dict(self.lvl_1_merge_multi_key, merge_dict) == {'lvl_1': {'k1': 'v1', 'k5': 'v5', 'k6': 'v6',
                                                                                     'lvl_2': {'k2': 'v2'}}}

    # This feature is currently not used
    def test_multi_key_multi_lvl(self):
        merge_dict = deepcopy(self.merge_dict)
        assert base.merge_dict(self.lvl_1_and_2_merge, merge_dict) == {'lvl_1': {'k1': 'v1', 'k7': 'v7',
                                                                                 'lvl_2': {'k2': 'v2', 'k8': 'v8'}}}


class TestMergeDictList:
    one_element_merge = [{'a[]': 1},
                         {'a[]': 2}]

    nested_element_merge = [{'b': {'a[]': 1}},
                            {'b': {'a[]': 2}}]

    nested_list_element_merge = [{'a': {'b[]': {'c': 1}}},
                                 {'a': {'b[]': {'c': 2}}}]

    not_dictionary = 'foo'

    @staticmethod
    def merged(test_list: List[dict], merged_dict: dict):
        for test_dict in test_list:
            merged_dict = base.merge_dict_list(test_dict, merged_dict)
        return merged_dict

    def test_one_element_merge(self):
        assert self.merged(self.one_element_merge, {}) == {'a': [1, 2]}

    def test_nested_element_merge(self):
        assert self.merged(self.nested_element_merge, {}) == {'b': {'a': [1, 2]}}

    def test_nested_list_element_merge(self):
        assert self.merged(self.nested_list_element_merge, {}) == {'a': {'b': [{'c': 1}, {'c': 2}]}}

    def test_not_dictionary(self):
        assert self.merged(self.not_dictionary, {}) is None


class TestInsertValue:
    insert_into_single = {'a': {'b': '#_insert'}}
    insert_into_many = {'a': {'k1': 'v1',
                              'k2': '#_insert'}}

    # needs to handle multiple types
    values = ('μfoo', 123, [1, 2, 3])

    def test_insert_single(self):
        results = ({'a': {'b': 'μfoo'}},
                   {'a': {'b': 123}},
                   {'a': {'b': [1, 2, 3]}})

        for value, result in zip(self.values, results):
            test_dict = deepcopy(self.insert_into_single)
            base.insert_value(test_dict, value)
            assert test_dict == result

    def test_insert_into_many(self):
        results = ({'a': {'k1': 'v1', 'k2': 'μfoo'}},
                   {'a': {'k1': 'v1', 'k2': 123}},
                   {'a': {'k1': 'v1', 'k2': [1, 2, 3]}})

        for value, result in zip(self.values, results):
            test_dict = deepcopy(self.insert_into_many)
            base.insert_value(test_dict, value)
            assert test_dict == result


class TestRemoveArrayID:
    single_array_key = {'a[]': 1}
    mixed_keys_array_key = {'a[]': 1, 'b': 2}
    multi_array_key = {'a[]': 1, 'b[]': 2}

    def test_single_key(self):
        base.remove_array_id(self.single_array_key)
        assert self.single_array_key == {'a': [1]}

    def test_mixed_key(self):
        base.remove_array_id(self.mixed_keys_array_key)
        assert self.mixed_keys_array_key == {'a': [1], 'b': 2}

    def test_multi_array_key(self):
        base.remove_array_id(self.multi_array_key)
        assert self.multi_array_key == {'a': [1],  'b': [2]}
