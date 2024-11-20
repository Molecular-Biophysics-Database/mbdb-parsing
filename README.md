# mbdb-parsing

## Description

Set of processors for InvenioRDM/OArepo style repository that allows for
extraction and conversion of metadata from raw measurements files. See below
for the list of supported file types.

### MST
 - .moc
 - .moc2
 - .xlsx

## Requirements

 * Python >=3.12

## Installation

```bash
pip install mbdb-parsing
```
Or add mbdb-parsing to the dependencies inside the app's pyproject.toml:

```toml
[project]
dependencies = [
    "mbdb-parsing",
]
```

## Configuration

The processors should be placed inside FileServiceConfig(s) objects:

```python
from invenio_records_resources.services import FileServiceConfig
from oarepo_runtime.services.config.service import PermissionsPresetsConfigMixin

from mbdb_parsing.mst import MocProcessor, XlxsProcessor


class MstFilesServiceConfig(PermissionsPresetsConfigMixin, FileServiceConfig):
    file_processors = [
        MocProcessor(),
        XlxsProcessor(),
    ]
    components = [
        *PermissionsPresetsConfigMixin.components,
        *FileServiceConfig.components,
    ]
```
