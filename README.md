# Summary

A very efficient library for diffing two sorted streams of tabular data, such as CSV files.

Keywords:
- table
- tabular data
- CSV
- TSV
- diff
- difference
- delta
- changes
- comparison

# Why another lib?

Most of the diffing libraries either load all the data in memory for comparison or would at least load the keys and store some hash on the data.
This is fine for a lot of scenarios but it doesn't scale with huge files and puts a risk that the data would'nt fit in memory.
Also, those strategies require a two-pass approach for diffing which is more expensive.

This library requires that the submitted files are already sorted by some primary key to compare the two streams in a single pass, 
while loading at most two rows of data in memory.

This allows us to diff two 600MB files containing 4 millions of rows in 10 seconds on my MacBook Pro.

# Features

- very fast
- memory efficient
- multiple input formats
- multiple output formats
- input files can have different column sets, in different order
- input files can have different formats
- compact JSON output format (field names are not repeated)
- highly configurable and customizable
- change stats
- new and old values available

# Points of interest

- single pass algorithm offering O(n) performance
- 100% code coverage
- one single dependency on n-readlines
- small composable objects
- iterator for enumerating the changes

# Algorithm complexity

Assuming that n is the number of rows in the old source and m the number of rows in the new source:
- Min complexity is O(max(n, m))
- Max complexity is O(n+m) (when old source contains only deleted rows and new source only new rows)

The average complexity, assuming a low rate of additions or deletions, should be linear and based on the input files.

# Usage

## Install the library

`npm i tabular-data-differ`

## Examples 

### Diff 2 CSV files on the console

```Typescript
import { diff } from 'tabular-data-differ';
const stats = diff({
    oldSource: './tests/a.csv',
    newSource: './tests/b.csv',
    keyFields: ['id'],
}).to('console');
console.log(stats);
```

### Diff 2 CSV files and only get the stats

```Typescript
import { diff } from 'tabular-data-differ';
const stats = diff({
    oldSource: './tests/a.csv',
    newSource: './tests/b.csv',
    output: 'null',
    keyFields: ['id'],
}).to('null');
console.log(stats);
```

### Diff 2 CSV files and produce a CSV file

```Typescript
import { diff } from 'tabular-data-differ';
const stats = diff({
    oldSource: './tests/a.csv',
    newSource: './tests/b.csv',
    keyFields: ['id'],
}).to('./temp/delta.csv');
console.log(stats);
```

### Diff 2 CSV files and produce a JSON file

```Typescript
import { diff } from 'tabular-data-differ';
const stats = diff({
    oldSource: './tests/a.csv',
    newSource: './tests/b.csv',
    keyFields: ['id'],
}).to({
    stream: './temp/delta.json',
    format: 'json',
});
console.log(stats);
```

### Diff 2 CSV files and produce a TSV file

```Typescript
import { diff } from 'tabular-data-differ';
const stats = diff({
    oldSource: './tests/a.csv',
    newSource: './tests/b.csv',
    keyFields: ['id'],
}).to({
    stream: './temp/delta.tsv',
    delimiter: '\t',
});
console.log(stats);
```

### Diff one CSV and one TSV and produce a JSON file

```Typescript
import { diff } from 'tabular-data-differ';
const stats = diff({
    oldSource: './tests/a.csv',
    newSource: {
        stream: './tests/b.tsv',
        delimiter: '\t',
    },
    keyFields: ['id'],
}).to({
    stream: './temp/delta.tsv',
    format: 'json',
});
console.log(stats);
```

### Diff two string arrays and enumerate the changes

```Typescript
import { diff, ArrayInputStream } from 'tabular-data-differ';
const differ = diff({
    oldSource: {
        stream: new ArrayInputStream([
            'id,name',
            '1,john',
            '2,mary',
        ]),
    },
    newSource: {
        stream: new ArrayInputStream([
            'id,name',
            '1,john',
            '3,sarah',
        ]),
    },
    keyFields: ['id'],
});
console.log('columns:', differ.getColumns());
for (const rowDiff of differ) {
    console.log(rowDiff);
}
console.log('stats:', differ.getStats());
```

### Diff 2 CSV files on the console and ignore deleted rows

```Typescript
import { diff } from 'tabular-data-differ';
const stats = diff({
    oldSource: './tests/a.csv',
    newSource: './tests/b.csv',
    keyFields: ['id'],
}).to({
    filter: (rowDiff) => rowDiff.status !== 'deleted',
});
console.log(stats);
```

### Diff 2 CSV files on the console but select only some categories of rows

```Typescript
import { diff } from 'tabular-data-differ';
const differ = diff({
    oldSource: './tests/a.csv',
    newSource: './tests/b.csv',
    keyFields: ['id'],
});
const catIdx = differ.getColumns().indexOf('CATEGORY');
const stats = differ.to({
    stream: 'console',
    filter: (rowDiff) => rowDiff.newRow && ['cat1', 'cat2', 'cat3'].includes(rowDiff.newRow[catIdx]),
});
console.log(stats);
```

## Documentation

### Source options

Name     |Required|Default value|Description
---------|--------|-------------|-----------
stream   | yes    |             |either a string filename or an instance of an InputStream (like FileInputStream).
format   | no     | csv         | either an existing format (csv or json) or a factory function to create your own format.
delimiter| no     | ,           | the char used to delimit fields within a row. This is only used by the CSV format.
filter   | no     |             | a filter to allow or reject the input rows.

### OutputOptions

Name         |Required|Default value|Description
-------------|--------|-------------|-----------
stream       | no     | console     | either a standard output (console, null), a string filename or an instance of an InputStream (like FileInputStream). 
format       | no     | csv         | either an existing format (csv or json) or a factory function to create your own format.
delimiter    | no     | ,           | the char used to delimit fields within a row. This is only used by the CSV format.
filter       | no     |             | a filter to select which changes should be sent to the output stream.
keepOldValues| no     | false       | specifies if the output should contain both the old and new values for each row.
keepSameRows | no     | false       | specifies if the output should also contain the rows that haven't changed.
changeLimit  | no     |             | specifies a maximum number of differences that should be outputted.
labels       | no     |             | a dictionary of key/value that allows to add custom metadata to the generated file.

### Differ options

Name            |Required|Default value|Description
----------------|--------|-------------|-----------
oldSource       | yes    |             | either a string filename or a SourceOptions
newSource       | yes    |             | either a string filename or a SourceOptions
keyFields       | yes    |             | the list of columns that form the primary key. This is required for comparing the rows.
includedFields  | no     |             | the list of columns to keep from the input files. If not specified, all columns are selected.
excludedFields  | no     |             | the list of columns to exclude from the input files.
descendingOrder | no     | false       | specifies if the input files are in descending order.

### diff function

Creates a Differ object from the submitted DifferOptions.

### Differ methods

#### open

Initiates the opening of the input streams (old, new) and reads the headers.

#### close

Closes all open streams.

#### getColumns

Returns the current column names. This will open the streams if it wasn't already done.

#### getStats

Returns the currents stats. There is no side effect.

#### to

Initiates the comparison between the old and new sources and sends the diffs to the specified output.

This returns the change stats once completed.

The options parameter can be either a standard output (console, null), a string filename or an OutputOptions.

Note that it can throw the UnorderedStreamsError exception if it detects that the streams are not properly ordered.

#### iterator

Enumerates the changes between the old and new sources.

Note that it can throw the UnorderedStreamsError exception if it detects that the streams are not properly ordered.

### CSV output format

This is a standard CSV format, using the specified character for delimiting fields or the default one (comma).

Note that there is an additional column named DIFF_STATUS that will tell if the row was added, deleted, modified.

```csv
DIFF_STATUS,id,a,b,c
deleted,01,a1,b1,c1
modified,04,aa4,bb4,cc4
deleted,05,a5,b5,c5
deleted,06,a6,b6,c6
added,10,a10,b10,c10
added,11,a11,b11,c11
```

Note that if you set the "OutputOptions.keepOldValues" property to true, you'll get additional columns prefixed by 'OLD_':
```csv
DIFF_STATUS,id,a,b,c,OLD_id,OLD_a,OLD_b,OLD_c
deleted,,,,,01,a1,b1,c1
modified,04,aa4,bb4,cc4,04,a4,b4,c4
deleted,,,,,05,a5,b5,c5
deleted,,,,,06,a6,b6,c6
added,10,a10,b10,c10,,,,
added,11,a11,b11,c11,,,,
```

### JSON output format

The schema is made of 3 parts:
- the header
- the items
- the footer

```json
{
    "header": {},
    "items": [...],
    "footer": {}
}
```

#### Header

The header contains a mandatory list of columns and an optional dictionary of key/value pairs named labels.

```json
{
    "columns": ["col1", "col2", "col3"]
}
```

or

```json
{
    "columns": ["col1", "col2", "col3"],
    "labels": {
        "key1": "val1",
        "key2": "val2",
    }
}
```

#### Items

A list of DiffRow objects, which can have two distinct layouts based on the "OutputOptions.keepOldValues" property.

##### keepOldValues is false or undefined
```json
{"status":"deleted","data":["01","a1","b1","c1"]},
{"status":"same","data":["02","a2","b2","c2"]},
{"status":"modified","data":["04","aa4","bb4","cc4"]},
{"status":"added","data":["10","a10","b10","c10"]},
```
##### keepOldValues is true

```json
{"status":"deleted","old":["01","a1","b1","c1"]},
{"status":"same","new":["02","a2","b2","c2"],"old":["02","a2","b2","c2"]},
{"status":"modified","new":["04","aa4","bb4","cc4"],"old":["04","a4","b4","c4"]},
{"status":"added","new":["10","a10","b10","c10"]},
```

#### Footer

The footer will simply contain a stats section summarizing the types of changes in the file.

```json
{
    "stats" : {
        "totalComparisons": 11,
        "totalChanges": 6,
        "changePercent": 54.55,
        "added": 2,
        "deleted": 3,
        "modified": 1,
        "same": 5
    }
}
```

# Development

## Install

```shell
git clone git@github.com:livetocode/tabular-data-differ.git
cd tabular-data-differ
npm i
```

## Tests

Tests are implemented with Jest and can be run with:
`npm t`

You can also look at the coverage with:
`npm run show-coverage`
