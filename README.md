# Summary

A very efficient library for diffing two **sorted** streams of tabular data, such as CSV files.

### Keywords
- table
- tabular data
- CSV
- TSV
- diff
- difference
- delta
- changes
- comparison

### Table of content

- [**Why another lib?**](#why-another-lib)
- [**Features**](#features)
- [**Points of interest**](#points-of-interest)
- [**Algorithm complexity**](#algorithm-complexity)
- [**Usage**](#usage)
- [**Documentation**](#documentation)
- [**Development**](#development)
- [**Roadmap**](#roadmap)

# Why another lib?

Most of the diffing libraries either load all the data in memory for comparison or would at least load the keys and store some hash on the data.
This is fine for a lot of scenarios but it doesn't scale with huge files and puts a risk that the data would'nt fit in memory.
Also, those strategies require a two-pass approach for diffing which is more expensive.

This library requires that the submitted files are already sorted by some primary key to compare the two streams in a single pass, 
while loading at most two rows of data in memory.

If your data is not already sorted, you can use my other lib https://github.com/livetocode/huge-csv-sorter, which can sort a huge file very efficiently thanks to SQLite.

This allows us to diff two 600MB files containing 2.6 millions of rows and 37 columns in 18 seconds on my MacBook Pro.
Or two 250 MB files containing 4 millions of rows and 7 columns in 10 seconds.

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
- no external dependency
- small composable objects
- async streams
- async iterator for enumerating the changes
- generic input format using an async generator function

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
const stats = await diff({
    oldSource: './tests/a.csv',
    newSource: './tests/b.csv',
    keys: ['id'],
}).to('console');
console.log(stats);
```

### Diff 2 CSV files on the console when the key column is in descending order

```Typescript
import { diff } from 'tabular-data-differ';
const stats = await diff({
    oldSource: './tests/a.csv',
    newSource: './tests/b.csv',
    keys: [{
        name: 'id',
        sortDirection: 'DESC',
    }],
}).to('console');
console.log(stats);
```

### Diff 2 CSV files on the console with a multi-column primary key, including a number

```Typescript
import { diff } from 'tabular-data-differ';
const stats = await diff({
    oldSource: './tests/a.csv',
    newSource: './tests/b.csv',
    keys: [
        'code',
        {
            name: 'version',
            comparer: 'number',
        }
    ],
}).to('console');
console.log(stats);
```

### Diff 2 CSV files on the console with a single case insensitive primary key (using a custom comparer)

```Typescript
import { diff, CellValue, cellComparer, stringComparer } from 'tabular-data-differ';

function caseInsensitiveCompare((a: CellValue, b: CellValue): number {
    if (typeof a === 'string' && typeof b === 'string') {
        return stringComparer(a.toLowerCase(), b.toLowerCase());
    }
    return cellComparer(a, b);
}

const stats = await diff({
    oldSource: './tests/a.csv',
    newSource: './tests/b.csv',
    keys: [
        {
            name: 'id',
            comparer: caseInsensitiveCompare,
        }
    ],
}).to('console');
console.log(stats);
```

### Diff 2 CSV files and only get the stats

```Typescript
import { diff } from 'tabular-data-differ';
const stats = await diff({
    oldSource: './tests/a.csv',
    newSource: './tests/b.csv',
    keys: ['id'],
}).to('null');
console.log(stats);
```

### Diff 2 CSV files and produce a CSV file

```Typescript
import { diff } from 'tabular-data-differ';
const stats = await diff({
    oldSource: './tests/a.csv',
    newSource: './tests/b.csv',
    keys: ['id'],
}).to('./temp/delta.csv');
console.log(stats);
```

### Diff 2 CSV files and produce a CSV file using HTTP transport

```Typescript
import { diff } from 'tabular-data-differ';
const stats = await diff({
    oldSource: new URL('https://some.server.org/tests/a.csv'),
    newSource: new URL('https://some.server.org/tests/b.csv'),
    keys: ['id'],
}).to(new URL('https://some.server.org/temp/delta.csv'));
console.log(stats);
```

Note that you can provide the username/password in the URL object if you need basic authentication.

### Diff 2 CSV files and produce a JSON file

```Typescript
import { diff } from 'tabular-data-differ';
const stats = await diff({
    oldSource: './tests/a.csv',
    newSource: './tests/b.csv',
    keys: ['id'],
}).to({
    destination: {
        format: 'json',
        stream: './temp/delta.json',
    },
});
console.log(stats);
```

### Diff 2 CSV files and produce a TSV file

```Typescript
import { diff } from 'tabular-data-differ';
const stats = await diff({
    oldSource: './tests/a.csv',
    newSource: './tests/b.csv',
    keys: ['id'],
}).to({
    destination: {
        format: 'tsv',
        stream: './temp/delta.tsv',
    }
});
console.log(stats);
```

### Diff one CSV and one TSV and produce a JSON file

```Typescript
import { diff } from 'tabular-data-differ';
const stats = await diff({
    oldSource: './tests/a.csv',
    newSource: {
        format: 'tsv',
        stream: './tests/b.tsv',
    },
    keys: ['id'],
}).to({
    destination: {
        format: 'json',
        stream: './temp/delta.json',
    },
});
console.log(stats);
```

### Diff two string arrays and enumerate the changes

```Typescript
import { diff, ArrayInputStream } from 'tabular-data-differ';
const ctx = await diff({
    oldSource: {
        format: 'csv',
        stream: new ArrayInputStream([
            'id,name',
            '1,john',
            '2,mary',
        ]),
    },
    newSource: {
        format: 'csv',
        stream: new ArrayInputStream([
            'id,name',
            '1,johnny',
            '3,sarah',
        ]),
    },
    keys: ['id'],
}).start();
console.log('columns:', ctx.columns);
const idIdx = ctx.columns.indexOf('id);
assert(idIdx >= 0, 'could not find id column');
const nameIdx = ctx.columns.indexOf('name);
assert(nameIdx >= 0, 'could not find name column');
for await (const rowDiff of ctx.diffs()) {
    if (rowDiff.status === 'modified') {
        const id = rowDiff.newRow[idIdx];
        const oldName = rowDiff.oldRow[nameIdx];
        const newName = rowDiff.newRow[nameIdx];
        if (oldName !== newName) {
            console.log('In record ', id, ', name changed from', oldName, 'to', newName);
        }
    }
}
console.log('stats:', ctx.stats);
```

### Diff 2 CSV files on the console and ignore deleted rows

```Typescript
import { diff } from 'tabular-data-differ';
const stats = await diff({
    oldSource: './tests/a.csv',
    newSource: './tests/b.csv',
    keys: ['id'],
}).to({
    destination: 'console',
    filter: (rowDiff) => rowDiff.status !== 'deleted',
});
console.log(stats);
```

### Diff 2 CSV files on the console but select only some categories of rows

```Typescript
import { diff } from 'tabular-data-differ';
const ctx = await diff({
    oldSource: './tests/c.csv',
    newSource: './tests/d.csv',
    keys: [
        'code',
        {
            name: 'version',
            comparer: 'number',
        }
    ],
}).start();
const catIdx = ctx.columns.indexOf('CATEGORY');
assert(catIdx >= 0, 'could not find CATEGORY column');
const stats = await ctx.to({
    destination: 'console',
    filter: (rowDiff) => ['Fruit', 'Meat'].includes(rowDiff.newRow?.[catIdx]?.toString() ?? rowDiff.oldRow?.[catIdx]?.toString() ?? ''),
});
console.log(stats);
```

### Duplicate key handling

If your data sources contain duplicate keys, then the diffing will fail by default, but you can configure this behavior using the duplicateKeyHandling option.

You can resolve the conflict by keeping the first or last row of the duplicates:
```Typescript
import { diff } from 'tabular-data-differ';
const stats = await diff({
    oldSource: './tests/a.csv',
    newSource: './tests/b.csv',
    keys: ['id'],
    duplicateKeyHandling: 'keepFirstRow', // or 'keepLastRow'
    duplicateRowBufferSize: 2000,
}).to('console');
console.log(stats);
```

Or, if you need more control in the row selection, then you can provide your own handler:
```Typescript
import { diff } from 'tabular-data-differ';
const stats = await diff({
    oldSource: './tests/a.csv',
    newSource: './tests/b.csv',
    keys: ['id'],
    duplicateKeyHandling: (rows) => rows[0], // same as 'keepFirstRow'
    duplicateRowBufferSize: 2000,
}).to('console');
console.log(stats);
```


### Order 2 CSV files and diff them on the console

Don't forget to install first my other lib: `npm i huge-csv-sorter`.

```Typescript
import { diff } from 'tabular-data-differ';
import { sort } from 'huge-csv-sorter';

await sort({
    source: './tests/a.csv',
    destination: './tests/a.sorted.csv',
    orderBy: ['id'],
});

await sort({
    source: './tests/b.csv',
    destination: './tests/b.sorted.csv',
    orderBy: ['id'],
});

const stats = await diff({
    oldSource: './tests/a.sorted.csv',
    newSource: './tests/b.sorted.csv',
    keys: ['id'],
}).to('console');
console.log(stats);
```

### Auto-correct unordered CSV files and retry diff

Don't forget to install first my other lib: `npm i huge-csv-sorter`.

```Typescript
import { diff } from 'tabular-data-differ';
import { sort } from 'huge-csv-sorter';

try {
    // try diff
    const stats = await diff({
        oldSource: './tests/a.csv',
        newSource: './tests/b.csv',
        keys: ['id'],
    }).to('./tests/diff.csv');
    console.log(stats);
} catch(err) {
    // catch unordered exception
    if (err instanceof UnorderedStreamsError) {
        // sort files
        await sort({
            source: './tests/a.csv',
            destination: './tests/a.sorted.csv',
            orderBy: ['id'],
        });

        await sort({
            source: './tests/b.csv',
            destination: './tests/b.sorted.csv',
            orderBy: ['id'],
        });
        // retry diff
        const stats = await diff({
            oldSource: './tests/a.sorted.csv',
            newSource: './tests/b.sorted.csv',
            keys: ['id'],
        }).to('./tests/diff.csv');
        console.log(stats);
    } else {
        throw err;
    }
} finally {
    // delete sorted files...
}
```

### async iterable source

You can easily plug any kind of data source by leveraging nodejs async generator functions, 
which would allow you to fetch the data from a database or from a REST API endpoint!

Here's a simplistic example:

```typescript
import { diff } from 'tabular-data-differ';

const stats = await diff({
    oldSource: {
        format: 'iterable',
        provider: someAsyncSource,
    },
    newSource: {
        format: 'iterable',
        provider: () => someAsyncSource(2),
    },
    keys: ['id'],
}).to('./output/files/output.csv');
console.log(stats);


async function *someAsyncSource(limit?: number) {
    let items = [
        {
            id: 1,
            name: 'John',
            age: 33,
        },
        {
            id: 2,
            name: 'Mary',
            age: 22,
        },
        {
            id: 3,
            name: 'Cindy',
            age: 44,
        },
    ];  
    if (limit !== undefined){
        items = items.slice(0, limit);
    }
    for (const item of items) {
        yield item;
    }
}

```


# Documentation

- [**API**](#api)
- [**File formats**](#file-formats)

## API

### Source options

#### CSV

Name     |Required|Default value|Description
---------|--------|-------------|-----------
format   | yes    |             | You must specify 'csv' to select the CSV format
stream   | yes    |             | either a string filename, a URL or an instance of an InputStream (like FileInputStream).
delimiter| no     | ,           | the char used to delimit fields within a row.

#### TSV

Name     |Required|Default value|Description
---------|--------|-------------|-----------
format   | yes    |             | You must specify 'tsv' to select the TSV format
stream   | yes    |             | either a string filename, a URL or an instance of an InputStream (like FileInputStream).
delimiter| no     | \t          | the char used to delimit fields within a row.

#### JSON

Name     |Required|Default value|Description
---------|--------|-------------|-----------
format   | yes    |             | You must specify 'json' to select the JSON format
stream   | yes    |             | either a string filename, a URL or an instance of an InputStream (like FileInputStream).

#### Iterable

Name     |Required|Default value|Description
---------|--------|-------------|-----------
format   | yes    |             | You must specify 'iterable' to select the Iterable format
provider | yes    |             | a function that must return an instance of an async iterable object (see [Async generator functions](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/AsyncGenerator))

See [Example](#async-iterable-source)

#### Custom

Name     |Required|Default value|Description
---------|--------|-------------|-----------
format   | yes    |             | You must specify 'custom' to select the custom format
reader   | yes    |             | an instance of a FormatReader

### Destination options

#### CSV

Name         |Required|Default value|Description
-------------|--------|-------------|-----------
format       | yes    |             | You must specify 'csv' to select the CSV format
stream       | yes    |             | either a string filename, a URL or an instance of an OutputStream (like FileOutputStream).
delimiter    | no     | ,           | the char used to delimit fields within a row.
keepOldValues| no     | false       | specifies if the destination should contain both the old and new values for each row.

#### TSV

Name         |Required|Default value|Description
-------------|--------|-------------|-----------
format       | yes    |             | You must specify 'tsv' to select the TSV format
stream       | yes    |             | either a string filename, a URL or an instance of an OutputStream (like FileOutputStream).
delimiter    | no     | \t          | the char used to delimit fields within a row.
keepOldValues| no     | false       | specifies if the destination should contain both the old and new values for each row.

#### JSON

Name         |Required|Default value|Description
-------------|--------|-------------|-----------
format       | yes    |             | You must specify 'json' to select the JSON format
stream       | yes    |             | either a string filename, a URL or an instance of an OutputStream (like FileOutputStream).
keepOldValues| no     | false       | specifies if the destination should contain both the old and new values for each row.

#### Custom

Name     |Required|Default value|Description
---------|--------|-------------|-----------
format   | yes    |             | You must specify 'custom' to select the custom format
writer   | yes    |             | an instance of a FormatWriter

### OutputOptions

Name            |Required|Default value|Description
----------------|--------|-------------|-----------
destination     | yes    |             | either a standard output (console, null), a string filename, a URL or an instance of an InputStream (like FileInputStream). 
filter          | no     |             | a filter to select which changes should be sent to the output stream.
keepSameRows    | no     | false       | specifies if the output should also contain the rows that haven't changed.
changeLimit     | no     |             | specifies a maximum number of differences that should be outputted.
labels          | no     |             | a dictionary of key/value pairs that can provide custom metadata to the generated file.

### Key options (ColumnDefinition)

Name         |Required|Default value|Description
-------------|--------|-------------|-----------
name         | yes    |             | the name of the column.
comparer     | no     | string      | either a standard comparer ('string' or 'number') or a custom comparer.
sortDirection| no     | ASC         | specifies if the column is sorted in ascending (ASC) or descending (DESC) order.

### Differ options

Name                  |Required|Default value|Description
----------------------|--------|-------------|-----------
oldSource             | yes    |             | either a string filename, a URL or a SourceOptions
newSource             | yes    |             | either a string filename, a URL or a SourceOptions
keys                  | yes    |             | the list of columns that form the primary key. This is required for comparing the rows. A key can be a string name or a {ColumnDefinition}
includedColumns       | no     |             | the list of columns to keep from the input sources. If not specified, all columns are selected.
excludedColumns       | no     |             | the list of columns to exclude from the input sources.
rowComparer           | no     |             | specifies a custom row comparer.
duplicateKeyHandling  |no      | fail        | specifies how to handle duplicate rows in a source. It will fail by default and throw a UniqueKeyViolationError exception. But you can ignore, keep the first or last row, or even provide your own function that will receive the duplicates and select the best candidate. 
duplicateRowBufferSize|no      | 1000        | specifies the maximum size of the buffer used to accumulate duplicate rows.

### diff function

Creates a Differ object from the submitted DifferOptions.

### Differ methods

#### start

returns a new DifferContext object with the input streams open and columns initialized.

You must call start to get an iterator (DifferContext.diffs) or if you need the columns prior to sending the diffs to the output with the "to" method.

#### to

Initiates the comparison between the old and new sources and sends the diffs to the specified output.

This returns the change stats once completed.

The options parameter can be either a standard output (console, null), a string filename, a URL or an OutputOptions.

Note that it can throw the UnorderedStreamsError exception if it detects that the streams are not properly ordered by the specified keys.
Note that it can throw the UniqueKeyViolationError exception if it detects that a stream has duplicate keys wich violates the primary keys specified in the options.

### DifferContext methods

#### close

Closes all open streams.

Note that the methods "to" or "diffs" will automatically close the streams.

#### columns

Returns the current column names.

#### stats

Returns the currents stats.

#### to

Initiates the comparison between the old and new sources and sends the diffs to the specified output.

This returns the change stats once completed.

The options parameter can be either a standard output (console, null), a string filename, a URL or an OutputOptions.

Note that it can throw the UnorderedStreamsError exception if it detects that the streams are not properly ordered by the specified keys.
Note that it can throw the UniqueKeyViolationError exception if it detects that a stream has duplicate keys wich violates the primary keys specified in the options.

#### diffs

Enumerates the differences between the old and new sources.

Note that it can throw the UnorderedStreamsError exception if it detects that the streams are not properly ordered by the specified keys.
Note that it can throw the UniqueKeyViolationError exception if it detects that a stream has duplicate keys wich violates the primary keys specified in the options.

### JSON input format

This library implements a simplistic JSON parser with a couple of assumptions:
- each JSON object should be saved on a distinct line
- the JSON file should only contain an array of objects
- each object should be flat (no nested JSON objects)
- all objects should share the same properties
- the lines can be indented
- each object can have either a preceding or a trailing comma
- the array start ([) and end (]) can be inlined with the first/last object or their own separate line

#### Examples

```json
[
    {"id": "01","a":"a1","b":"b1","c":"c1"},
    {"id": "02","a":"a2","b":"b2","c":"c2"},
    {"id": "03","a":"a3","b":"b3","c":"c3"}
]
```

```json
[{"id": "01","a":"a1","b":"b1","c":"c1"},
{"id": "02","a":"a2","b":"b2","c":"c2"},
{"id": "03","a":"a3","b":"b3","c":"c3"}]
```

```json
[
    {"id": "01","a":"a1","b":"b1","c":"c1"}
    ,{"id": "02","a":"a2","b":"b2","c":"c2"}
    ,{"id": "03","a":"a3","b":"b3","c":"c3"}
]
```

## File formats

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

A list of RowDiff objects, which can have two distinct layouts based on the "OutputOptions.keepOldValues" property.

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

# Roadmap

If you manifest some interest in this project, we could add new streams:
- S3, allowing you to use an external storage capacity such as AWS S3
- HTTP, allowing you to provide custom headers for authentication

And we could add more formats:
- XML
- protobuff
- SQL, allowing you to diff two database tables between two separate databases

But with the [async iterable sources](#iterable) feature, you should be able to easily plug any kind of data source you need!