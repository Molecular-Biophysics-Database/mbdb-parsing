# Extraction, conversion and validation of measured data and metadata

**WARNING**
The tools are in an early state of development, please be careful when using
them as they might overwrite changes you have made elsewhere without asking you.

## readers

Collection of the tools for extracting the measured data and metadata from
instrument manufacturer derived formats and converting them to MBDB type json
files. Depending on the specific technique, one or more files may be provided.

Each method should implement the following classes from the
**abstract base class**:

  * A **single** `ReaderPicker` prefixed with method (*e.g.* `MSTPicker`)
  * A number of `MeasurementReader` prefixed the method and suffixed with an
    identifier of type of file it can read (*e.g.* `MSTReaderMOC`)

Furthermore, a number of conversion dictionaries that

### ReaderPicker

The `ReaderPicker` is responsible for choosing the correct `MeasurementReader`
based on the supplied file path(s) and store it in the class attribute
`picked_reader`. It should raise an `ValueError` if the no `MeasurementReader`
capable of reading the supplied file(s) could be found.

### MeasurementReaders

Each `MeasurementReader` should be specific to single output (*e.g.* filetype)
and implement the abstract method:

  * `read`, which parses the content of the file(s) and places the result in the
    `measurement_dict` attribute.
  * `to_storage_json`, which converts the know items in `measurement_dict` to
     MBDB compatible json using the conversion dictionary using the
     `conversion_dict` attribute.

`MeasurementReader`s are allowed to share a single conversion dictionary
(so be careful when changing it). An empty conversion dictionary signifies that
a conversion has not yet been defined and that no information is extracted.

### Rules for writing conversion dictionaries

  * The keys should be keys from the extracted data
  * The values should be complete MBDB format paths
  * Array type items should be suffixed with `[]`
  * `'#_insert'` is the place holder, which is replaced by the extracted values

## TODO
  * Writing tool for loading the extracted and converted data into the GUI
  * Writing a general conversion interface
  * Converting from MBDB JSON to:
    * Simple tabulated form of the raw data
    * mmCIF
  * Writing validators to check the measured data (method specific)
