def force_list(value, allowed_types):
    if isinstance(value, allowed_types):
        return [value]
    if isinstance(value, list):
        return value
    if not isinstance(allowed_types, tuple):
        allowed_types = (allowed_types,)
    allowed_types = ', '.join(x.__name__ for x in ((list,) + allowed_types))
    raise TypeError('%r is not one of %s' % (value, allowed_types))
