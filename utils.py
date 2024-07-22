def clean(value):
    if type(value) is list:
        return [element.replace('\n', '').replace(' ', '') for element in value]
    return value.replace('\n', '').replace('-', ' ')

