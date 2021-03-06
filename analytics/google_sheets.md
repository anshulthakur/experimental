## Google Sheets Primer

This document is meant to be a short primer on how to use Google spreadsheets using python APIs. 
Some part is devoted to the nomenclature of Google sheets itself as that is a prerequisite for operating
on the sheets software (Python or otherwise).

More info may be obtained from [Google Sheets API Tutorials](https://developers.google.com/sheets/api/guides/concepts)

For Python APIs, see [link](https://developers.google.com/api-client-library/python/start/get_started)

A typical Google Spreadsheets URL looks like:

```
https://docs.google.com/spreadsheets/d/1qpyC0XzvTcKT6EISywvqESX3A0MwQoFDE8p-Bll4hps/edit#gid=1646970894
```

* Regex to extract Spreadsheet ID: 

```
/spreadsheets/d/([a-zA-Z0-9-_]+)
```

Yields `1qpyC0XzvTcKT6EISywvqESX3A0MwQoFDE8p-Bll4hps`

* Regex to extract Sheet ID:

```
[#&]gid=([0-9]+)
```

Yields `1646970894`

* Get sheet properties like __Title__ etc. for all sheets:

[ref](https://developers.google.com/sheets/api/samples/sheet#determine_sheet_id_and_other_properties)

```
GET https://sheets.googleapis.com/v4/spreadsheets/spreadsheetId?&fields=sheets.properties
```

### Making GET requests using Python API

While the above is just an HTTP GET request, how do we make a GET request using Python client?
For a service object `service`, we can invoke a general GET method as
```
service.<resource>.get(<param>)
```

[Resource](https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/get)
