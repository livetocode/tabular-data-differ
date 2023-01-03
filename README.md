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
console.log('headers:', differ.getHeaders());
for (const change of differ) {
    console.log(change);
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

### Diff 2 CSV files on the console but select only some categories of input rows

If we assume that the 4th column (row[3]) contains such a category:

```Typescript
import { diff } from 'tabular-data-differ';
const stats = diff({
    oldSource: {
        stream: './tests/a.csv',
        filter: row => ['cat1', 'cat2', 'cat3'].includes(row[3]),
    },
    newSource: {
        stream: './tests/b.csv',
        filter: row => ['cat1', 'cat2', 'cat3'].includes(row[3]),
    },
    keyFields: ['id'],
}).to('console');
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

Creates a Differ object from the specified DifferOptions.

### Differ methods

#### open

Initiates the opening of the input streams (old, new) and reads the headers.

#### close

Closes all open streams.

#### getHeaders

Returns the current column names. This will open the streams if it wasn't already done.

#### getStats

Returns the currents stats. There is no side effect.

#### to

Initiates the comparison between the old and new sources and sends the diffs to the specified output.

This returns the change stats once completed.

The options parameter can be either a standard output (console, null), a string filename or an OutputOptions.

#### OutputOptions

Name         |Required|Default value|Description
-------------|--------|-------------|-----------
stream       | no     | console     | either a standard output (console, null), a string filename or an instance of an InputStream (like FileInputStream). 
format       | no     | csv         | either an existing format (csv or json) or a factory function to create your own format.
delimiter    | no     | ,           | the char used to delimit fields within a row. This is only used by the CSV format.
filter       | no     |             | a filter to allow or reject the input rows.
keepOldValues| no     | false       | specifies if the output should contain both the old and new values for each row.
keepSameRows | no     | false       | specifies if the output should alsol contain the rows that haven't changed.
changeLimit  | no     |             | specifies a maximum number of differences that should be outputted.
labels       | no     |             | a dictionary of key/value that allows to add custom metadata to the generated file.


#### iterator

Enumerates the changes between the old and new sources.

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
