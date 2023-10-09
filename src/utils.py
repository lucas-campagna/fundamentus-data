from  urllib.parse import quote as parse_url

def build_rawdata(keys : set, vars : dict):
    data = dict()
    for key in keys:
        data[key] = parse_url(vars[key]) if type(vars[key]) == str else vars[key]
    return '&'.join([f'{k}={v}' for k,v in data.items()])