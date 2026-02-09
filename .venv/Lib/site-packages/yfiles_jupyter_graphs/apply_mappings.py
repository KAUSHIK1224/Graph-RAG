from typing import Dict as TDict, List as TList, Union
import inspect
from .utils import NEW_GROUP_ID_PREFIX, get_attribute_by_name

"""
The Mapping class contains all mapping related logic as well as some of the group node related code. 

"""
class MappingClass:

    def __init__(self, widget):
        self._widget = widget

    def _get_wrapped_mapping_function(
            self,
            function: Union[callable, str, None],
            key: str):
        """wrap mapping function so that return value is used for inplace update
        and for compatibility with arguments index and element

        mapping function return only one value
        but the underlying element (dict) should save this value


        Parameters
        ----------
        function: callable | str
            function to be wrapped or the property key whose value should be returned
        key: str
            element (dict) key that should be used
            to save the return value of the function

        Returns
        -------
        wrapped: callable
            wrapped function

        """

        # helper function
        widget = self._widget

        def _get_function_value(fn: Union[callable, str, None], index: int, element: TDict):
            if fn is None:
                return None

            if isinstance(fn, str):
                res = element.get('properties', {}).get(fn, None)
                if res is None:
                    res = element.get(fn, None)
                    if res is None:
                        return None
                return res

            parameters = inspect.signature(fn).parameters
            if len(parameters) == 0:
                return fn()
            elif len(parameters) == 1 and parameters[list(parameters)[0]].annotation == int:
                return fn(index)
            elif len(parameters) == 1:
                return fn(element)
            else:
                return fn(index, element)

        def wrapped(index: int, element: TDict):
            """wrap mapping function"""
            try:
                value = _get_function_value(function, index, element)
            except (NameError, TypeError, KeyError, ValueError) as error:
                widget._errorMessage = ''.join(f"Could not resolve the mapping function for the given data object \n "
                                              f"{{ \n \t {element} \n }} \n ")
                widget._error = error
                return element

            if key == 'label':
                if isinstance(value, dict):
                    element['label'] = value.pop('text', '')
                    if 'styles' in element:
                        # should always be the case due to the previous running mappings
                        styles = element['styles']
                        styles['label_styles'] = value
                        element['styles'] = styles
                    else:
                        # just to be extra safe
                        element['styles'] = {'label_styles': value}
                elif value is not None:
                    element[key] = str(value)

            elif key == 'layout':
                if value is not None:
                    # layout mapping overwrites previous position and size results
                    element['position'] = value[:2]
                    element['size'] = value[2:]

            elif key == 'parent_group':
                if isinstance(value, (str, int, float)):
                    label = str(value)
                    parent_id = NEW_GROUP_ID_PREFIX + label
                    # store new group node
                    group_nodes = widget._group_nodes
                    group_nodes.append({'id': parent_id, 'properties': {'label': label}})
                    widget._group_nodes = group_nodes

                    # assign as this node's parent
                    element['parentId'] = parent_id

                elif isinstance(value, dict):
                    # for dicts, the user is required to provide a "label" property
                    if 'label' not in value or not isinstance(value.get('label'), (str, int, float)):
                        widget._errorMessage = ''.join(
                            f"The provided group node dict has no property 'label': \n "
                            f"{{ \n \t {value} \n }} \n ")
                        return element  # do nothing

                    label = str(value.get('label'))
                    parent_id = NEW_GROUP_ID_PREFIX + label
                    # store new group node
                    group_nodes = widget._group_nodes
                    group_nodes.append({'id': parent_id, 'properties': dict(value.items())})
                    widget._group_nodes = group_nodes

                    # assign as this node's parent
                    element['parentId'] = parent_id

            elif value is not None:
                element[key] = value

            return element

        return wrapped

    def _get_wrapped_mapping_function_by_name(self, function_name: str, *args, **kwargs):
        function = get_attribute_by_name(self._widget, function_name, 'default')
        return self._get_wrapped_mapping_function(function, *args, **kwargs)

    def get_mapping_functions_by_name(self, function_dict: TDict):
        return [
            self._get_wrapped_mapping_function_by_name(fn, **kwargs)
            for fn, kwargs in function_dict.items()
        ]

    def _apply_mapping_and_change_value(
            self,
            key: str,
            mapping: callable,
            *args,
            **kwargs
    ):
        """handle traitlet value change

        this is one possible solution to the problem that traitlets lists/dicts
        can not be changed inplace

        no checking is done if self really has traitlet attribute of name key

        https://stackoverflow.com/q/51482598

        related
        https://ipywidgets.readthedocs.io/en/latest/examples/Widget%20Events.html
        https://ipywidgets.readthedocs.io/en/latest/examples/Widget%20Custom.html
        https://github.com/jupyter-widgets/ipywidgets/issues/2916
        https://github.com/jupyter-widgets/ipywidgets/issues/2922
        https://github.com/ipython/traitlets/issues/443
        https://github.com/ipython/traitlets/pull/466
        https://github.com/ipython/traitlets/issues/495
        https://github.com/ipython/traitlets/issues/496
        https://github.com/ipython/traitlets/issues/557

        Parameters
        ----------
        key: str
            traitlet attribute to change
        mapping: callable
            function that calculates new traitlet value
            takes old value, args and kwargs as inputs
            return value is new traitlet value
        args: typing.List
        temp_value: typing.Optional[typing.Any]
            value for traitlet during mapping calculation
        kwargs: typing.Dict

        Returns
        -------

        """
        widget = self._widget
        value = getattr(widget, key)
        # assign a temp array during mapping to avoid traitlet synchronization
        setattr(widget, key, [])
        value = mapping(value, *args, **kwargs)
        # re-assign the mapped values
        setattr(widget, key, value)

    def _apply_node_mappings(self):
        self._apply_mapping_and_change_value(
            '_nodes',
            self._apply_elements_mappings,
            self._get_node_mapping_functions()
        )

    def _apply_edge_mappings(self):
        self._apply_mapping_and_change_value(
            '_edges',
            self._apply_elements_mappings,
            self._get_edge_mapping_functions()
        )

    # create new parent group nodes before applying other mappings to include them

    def _apply_parent_group_mapping(self):
        widget = self._widget
        # avoid node duplication
        self._reset_group_nodes()

        # resolve the group node mapping that creates new group node dicts
        self._apply_mapping_and_change_value(
            '_nodes',
            self._apply_elements_mappings,
            self._get_parent_group_mapping_function()
        )

        # assign the new group nodes to the dataset
        widget._nodes = widget._nodes + widget._group_nodes

    ''' 
        Public function called by the widget to apply all mappings.   
    '''

    def apply_mappings(self):
        # create artificial group nodes by resolving the node_parent_group mapping *before* applying node mappings
        self._apply_parent_group_mapping()
        # process node/edge mappings
        self._apply_node_mappings()
        self._apply_edge_mappings()

    def _get_mapping_functions_by_name(self, function_dict: TDict):
        return [
            self._get_wrapped_mapping_function_by_name(fn, **kwargs)
            for fn, kwargs in function_dict.items()
        ]

    def _get_node_mapping_functions(self):
        return self._get_mapping_functions_by_name({
            '_node_property_mapping': {'key': 'properties'},
            '_node_color_mapping': {'key': 'color'},
            '_node_styles_mapping': {'key': 'styles'},
            '_node_label_mapping': {'key': 'label'},
            '_node_scale_factor_mapping': {'key': 'scale_factor'},
            '_node_type_mapping': {'key': 'type'},
            '_node_size_mapping': {'key': 'size'},
            '_node_position_mapping': {'key': 'position'},
            '_node_layout_mapping': {'key': 'layout'},
            '_node_cell_mapping': {'key': 'node_cell'},
            '_heat_mapping': {'key': 'heat'},
            '_node_coordinate_mapping': {'key': 'coordinates'},
            '_node_parent_mapping': {'key': 'parentId'}
        })

    def _get_edge_mapping_functions(self):
        return self._get_mapping_functions_by_name({
            '_edge_property_mapping': {'key': 'properties'},
            '_edge_color_mapping': {'key': 'color'},
            '_edge_thickness_factor_mapping': {'key': 'thickness_factor'},
            '_directed_mapping': {'key': 'directed'},
            '_edge_styles_mapping': {'key': 'styles'},
            '_edge_label_mapping': {'key': 'label'},
            '_heat_mapping': {'key': 'heat'}
        })

    def _get_parent_group_mapping_function(self):
        return self._get_mapping_functions_by_name({
            '_node_parent_group_mapping': {'key': 'parent_group'}
        })

    @staticmethod
    def _apply_elements_mappings(elements: TList[TDict], mappings: TList[callable]):
        """for each element apply all mappings inorder and inplace

        Parameters
        ----------
        elements: typing.List[typing.Dict]
        mappings: typing.List[callable]

        Returns
        -------
        elements: typing.List[typing.Dict]

        """
        for index, element in enumerate(elements):
            for mapping in mappings:
                element = mapping(index, element)
            elements[index] = element
        return elements

    def _reset_group_nodes(self):
        widget = self._widget
        if len(widget._group_nodes) > 0:
            group_node_ids = set(map(lambda node_dict: node_dict.get('id'), widget._group_nodes))
            widget._nodes = [node for node in widget._nodes if node['id'] not in group_node_ids]

        # clear artificial group nodes
        widget._group_nodes = []
