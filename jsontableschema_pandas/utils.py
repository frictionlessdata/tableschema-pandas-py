import six


def force_list(value, allowed_types):
    if isinstance(value, allowed_types):
        return [value]
    if isinstance(value, list):
        return value
    allowed_types = (list,) + tuple(allowed_types)
    allowed_types = ', '.join(map(six.text_type, allowed_types))
    raise ValueError('%r is not one of %s' % (value, allowed_types))
