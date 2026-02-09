from typing import Dict as TDict, Optional


def safe_delete_mapping(obj, mapping):
    """Deleter for mapping properties.

    Ensures safe deletion.

    Returns
    -------

    """
    if hasattr(obj, mapping):
        delattr(obj, mapping)

def get_neo4j_item_text(element: TDict):
    lowercase_element_props = {key.lower(): value for key, value in element.get('properties', {}).items()}
    for key in NEO4J_LABEL_KEYS:
        if key in lowercase_element_props:
            return str(lowercase_element_props[key])
    return None

def get_attribute_by_name(
        obj,
        attribute_name: str,
        fallback_to: Optional[str] = None
):
    """get the specified attribute by name

    if fallback_to is set to a string
    then get attribute with name {fallback_to}{attribute_name}

    Parameters
    ----------
    obj: object
    attribute_name: str
    fallback_to: typing.Optional[str]

    Returns
    -------
    attribute: typing.Any

    """

    if hasattr(obj, attribute_name):
        return getattr(obj, attribute_name)
    elif fallback_to is not None and hasattr(obj, fallback_to + attribute_name):
        return getattr(obj, fallback_to + attribute_name)
    else:
        return None

'''
NEIGHBORHOOD_TAB_ID: str
    Specify neighborhood tab id.
DATA_TAB_ID: str
    Specify data tab id.
SEARCH_TAB_ID: str
    Specify search tab id.
ABOUT_TAB_ID: str
    Specify about tab id.
CONTEXT_PANE_MAPPING: list
    Define the order and mapping to id and title of the context pane tabs.

    Each element is a dictionary with keys ("id", "title").
    By defining it on the python side of the widget
    it makes it possible (and easier) to check user input for correctness.
'''
NEIGHBORHOOD_TAB_ID = 'Neighborhood'
DATA_TAB_ID = 'Data'
SEARCH_TAB_ID = 'Search'
ABOUT_TAB_ID = 'About'
CONTEXT_PANE_MAPPING = [ #TODO remove...
    {'id': NEIGHBORHOOD_TAB_ID, 'title': NEIGHBORHOOD_TAB_ID},
    {'id': DATA_TAB_ID, 'title': DATA_TAB_ID},
    {'id': SEARCH_TAB_ID, 'title': SEARCH_TAB_ID},
    {'id': ABOUT_TAB_ID, 'title': ABOUT_TAB_ID}
]
SCALING_PER_NODE = 10
COLOR_PALETTE = ['#2196F3', '#4CAF50', '#F44336', '#607D8B', '#673AB7', '#CDDC39', '#9E9E9E', '#9C27B0']
NEO4J_LABEL_KEYS = ['name', 'title', 'text', 'description', 'caption', 'label']
NEW_GROUP_ID_PREFIX = 'group#'
