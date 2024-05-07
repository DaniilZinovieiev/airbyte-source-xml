#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_file_custom import SourceFileCustom


def run():
    source = SourceFileCustom()
    launch(source, sys.argv[1:])
