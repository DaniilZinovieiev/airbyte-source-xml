#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import json
import logging
from copy import deepcopy

import jsonschema
import pytest
from airbyte_cdk.models import (
    AirbyteConnectionStatus,
    AirbyteMessage,
    AirbyteStream,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    ConnectorSpecification,
    DestinationSyncMode,
    Status,
    SyncMode,
    Type,
)
from airbyte_cdk.utils import AirbyteTracedException
from airbyte_protocol.models.airbyte_protocol import Type as MessageType
from source_file_custom.source import SourceFileCustom
from source_file_custom.client import Client

logger = logging.getLogger("airbyte")


@pytest.fixture
def source():
    return SourceFileCustom()

@pytest.fixture
def client():
    return Client(
        dataset_name="test_dataset",
        url="scp://test_dataset",
        provider={"provider": {"storage": "HTTPS", "reader_impl": "gcsfs", "user_agent": True}},
    )

@pytest.fixture
def config():
    config_path: str = "/home/zeppier/PycharmProjects/airbyte-source-xml/integration_tests/config.json"
    with open(config_path, "r") as f:
        return json.loads(f.read())

@pytest.fixture
def config_xml():
    return {
        "dataset_name": "test_xml",
        "format": "xml",
        "url": "https://www.troliunamas.lt/media/kaina24/kaina24_default_feed.xml",
        "provider": {
            "storage": "HTTPS",
        }
    }


def test_read_xml(source, config_xml):

    raw_catalog = source.discover(logger=logging.getLogger("airbyte"), config=deepcopy(config_xml))

    catalog = get_catalog_xml(raw_catalog.streams[0].json_schema)

    records = source.read(logger=logging.getLogger("airbyte"), config=deepcopy(config_xml), catalog=catalog)

    records = [r.record.data for r in records if r.type == MessageType.RECORD]
    for record in records:
        print(record)
    #     assert isinstance(record, dict), f"Record is not a dictionary: {record}"
    #
    #     expected_keys = ['id', 'title', 'description', 'condition', 'ean_code', 'manufacturer_code', 'manufacturer', 'model', 'stock', 'price', 'delivery_price', 'delivery_time', 'image_url', 'product_url', 'category_id', 'category_link']
    #     for key in expected_keys:
    #         assert key in record, f"Record missing expected key '{key}': {record}"


def test_load_xml_schema_with_nested(client):
    file_path = '../source_file_custom/custom_xml_mock.xml'
    with open(file_path, 'r') as file:
        json_schema = client.load_xml_schema(file)

    print(json_schema)

def test_load_xml_with_nested(client):
    file_path = '../source_file_custom/custom_xml_mock.xml'
    with open(file_path, 'r') as file:
        json_schema = client.load_xml(file)

    print(json_schema)


def test_csv_with_utf16_encoding(absolute_path, test_files):
    config_local_csv_utf16 = {
        "dataset_name": "AAA",
        "format": "csv",
        "reader_options": '{"encoding":"utf_16", "parse_dates": ["header5"]}',
        "url": f"{absolute_path}/{test_files}/test_utf16.csv",
        "provider": {"storage": "local"},
    }
    expected_schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "properties": {
            "header1": {"type": ["string", "null"]},
            "header2": {"type": ["number", "null"]},
            "header3": {"type": ["number", "null"]},
            "header4": {"type": ["boolean", "null"]},
            "header5": {"type": ["string", "null"], "format": "date-time"},
        },
        "type": "object",
    }

    catalog = SourceFileCustom().discover(logger=logger, config=config_local_csv_utf16)
    stream = next(iter(catalog.streams))
    assert stream.json_schema == expected_schema


def get_catalog(properties):
    return ConfiguredAirbyteCatalog(
        streams=[
            ConfiguredAirbyteStream(
                stream=AirbyteStream(
                    name="test",
                    json_schema={"$schema": "http://json-schema.org/draft-07/schema#", "type": "object", "properties": properties},
                    supported_sync_modes=[SyncMode.full_refresh],
                ),
                sync_mode=SyncMode.full_refresh,
                destination_sync_mode=DestinationSyncMode.overwrite,
            )
        ]
    )

def get_catalog_xml(schema):
    return ConfiguredAirbyteCatalog(
        streams=[
            ConfiguredAirbyteStream(
                stream=AirbyteStream(
                    name="test",
                    json_schema=schema,
                    supported_sync_modes=[SyncMode.full_refresh],
                ),
                sync_mode=SyncMode.full_refresh,
                destination_sync_mode=DestinationSyncMode.overwrite,
            )
        ]
    )

def test_nan_to_null(absolute_path, test_files):
    """make sure numpy.nan converted to None"""
    config = {
        "dataset_name": "test",
        "format": "csv",
        "reader_options": json.dumps({"sep": ";"}),
        "url": f"{absolute_path}/{test_files}/test_nan.csv",
        "provider": {"storage": "local"},
    }

    catalog = get_catalog(
        {"col1": {"type": ["string", "null"]}, "col2": {"type": ["number", "null"]}, "col3": {"type": ["number", "null"]}}
    )

    source = SourceFileCustom()
    records = source.read(logger=logger, config=deepcopy(config), catalog=catalog)

    records = [r.record.data for r in records if r.type == MessageType.RECORD]
    assert records == [
        {"col1": "key1", "col2": 1.11, "col3": None},
        {"col1": "key2", "col2": None, "col3": 2.22},
        {"col1": "key3", "col2": None, "col3": None},
        {"col1": "key4", "col2": 3.33, "col3": None},
    ]

    config.update({"format": "yaml", "url": f"{absolute_path}/{test_files}/formats/yaml/demo.yaml"})
    records = source.read(logger=logger, config=deepcopy(config), catalog=catalog)
    records = [r.record.data for r in records if r.type == MessageType.RECORD]
    assert records == []

    config.update({"provider": {"storage": "SSH", "user": "user", "host": "host"}})

    with pytest.raises(Exception):
        for record in source.read(logger=logger, config=config, catalog=catalog):
            pass


def test_spec(source):
    spec = source.spec(None)
    assert isinstance(spec, ConnectorSpecification)


def test_check(source, config):
    expected = AirbyteConnectionStatus(status=Status.SUCCEEDED)
    actual = source.check(logger=logger, config=config)
    assert actual == expected


def test_check_invalid_config(source, invalid_config):
    expected = AirbyteConnectionStatus(status=Status.FAILED)
    actual = source.check(logger=logger, config=invalid_config)
    assert actual.status == expected.status


def test_check_invalid_reader_options(source, invalid_reader_options_config):
    with pytest.raises(AirbyteTracedException, match="Field 'reader_options' is not a valid JSON object. Please provide key-value pairs"):
        source.check(logger=logger, config=invalid_reader_options_config)


def test_discover_dropbox_link(source, config_dropbox_link):
    source.discover(logger=logger, config=config_dropbox_link)


def test_discover(source, config, client):
    catalog = source.discover(logger=logger, config=config)
    catalog = AirbyteMessage(type=Type.CATALOG, catalog=catalog).dict(exclude_unset=True)
    schemas = [stream["json_schema"] for stream in catalog["catalog"]["streams"]]
    for schema in schemas:
        jsonschema.Draft7Validator.check_schema(schema)


def test_check_wrong_reader_options(source, config):
    config["reader_options"] = '{encoding":"utf_16"}'
    with pytest.raises(AirbyteTracedException, match="Field 'reader_options' is not valid JSON object. https://www.json.org/"):
        source.check(logger=logger, config=config)


def test_check_google_spreadsheets_url(source, config):
    config["url"] = "https://docs.google.com/spreadsheets/d/"
    with pytest.raises(
        AirbyteTracedException,
        match="Failed to load https://docs.google.com/spreadsheets/d/: please use the Official Google Sheets Source connector",
    ):
        source.check(logger=logger, config=config)


def test_pandas_header_not_none(absolute_path, test_files):
    config = {
        "dataset_name": "test",
        "format": "csv",
        "reader_options": json.dumps({}),
        "url": f"{absolute_path}/{test_files}/test_no_header.csv",
        "provider": {"storage": "local"},
    }

    catalog = get_catalog({"text11": {"type": ["string", "null"]}, "text12": {"type": ["string", "null"]}})

    source = SourceFileCustom()
    records = source.read(logger=logger, config=deepcopy(config), catalog=catalog)
    records = [r.record.data for r in records if r.type == MessageType.RECORD]
    assert records == [
        {"text11": "text21", "text12": "text22"},
    ]


def test_pandas_header_none(absolute_path, test_files):
    config = {
        "dataset_name": "test",
        "format": "csv",
        "reader_options": json.dumps({"header": None}),
        "url": f"{absolute_path}/{test_files}/test_no_header.csv",
        "provider": {"storage": "local"},
    }

    catalog = get_catalog({"0": {"type": ["string", "null"]}, "1": {"type": ["string", "null"]}})

    source = SourceFileCustom()
    records = source.read(logger=logger, config=deepcopy(config), catalog=catalog)
    records = [r.record.data for r in records if r.type == MessageType.RECORD]
    assert records == [
        {"0": "text11", "1": "text12"},
        {"0": "text21", "1": "text22"},
    ]


def test_incorrect_reader_options(absolute_path, test_files):
    config = {
        "dataset_name": "test",
        "format": "csv",
        "reader_options": json.dumps({"sep": "4", "nrows": 20}),
        "url": f"{absolute_path}/{test_files}/test_parser_error.csv",
        "provider": {"storage": "local"},
    }

    source = SourceFileCustom()
    with pytest.raises(
        AirbyteTracedException,
        match="can not be parsed. Please check your reader_options. https://pandas.pydata.org/pandas-docs/stable/user_guide/io.html",
    ):
        _ = source.discover(logger=logger, config=deepcopy(config))

    with pytest.raises(
        AirbyteTracedException,
        match="can not be parsed. Please check your reader_options. https://pandas.pydata.org/pandas-docs/stable/user_guide/io.html",
    ):
        catalog = get_catalog({"0": {"type": ["string", "null"]}, "1": {"type": ["string", "null"]}})
        records = source.read(logger=logger, config=deepcopy(config), catalog=catalog)
        records = [r.record.data for r in records if r.type == MessageType.RECORD]
