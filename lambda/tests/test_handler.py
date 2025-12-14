import json
from lambda import utils
from lambda.handler import parse_json, parse_csv

def test_normalize():
    assert utils.normalize_indicator(" EXAMPLE.COM ") == "example.com"

def test_parse_json_list():
    body = json.dumps([{"type":"ip","value":"1.2.3.4"}])
    out = parse_json(body)
    assert isinstance(out, list)
    assert out[0]["value"] == "1.2.3.4"

def test_parse_json_obj():
    body = json.dumps({"indicators":[{"type":"ip","value":"1.2.3.4"}]})
    out = parse_json(body)
    assert isinstance(out, list)
    assert out[0]["value"] == "1.2.3.4"

def test_parse_csv():
    csvdata = "type,value\nip,1.2.3.4\n"
    out = parse_csv(csvdata)
    assert isinstance(out, list)
    assert out[0]["value"] == "1.2.3.4"
