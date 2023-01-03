import fs from 'fs';
import lineByLine from 'n-readlines';

//TODO: make stream operations async
//TODO: allow to specify a custom comparer for each specified key
// keyFields: ["id", {name: "version", comparer: NumberComparer, descendingOrder: true}]

export type Row = string[];

export type RowFilter = (row: Row) => boolean;

export interface RowPair {
    oldRow?: Row;
    newRow?: Row;
}

export type RowComparer = (keys: RowHeader[], a? : Row, b?: Row) => number;

export type RowNormalizer = (row?: Row) => Row | undefined;

export type RowHeader = {
    name: string;
    oldIndex: number;
    newIndex: number;
}

export type RowPairProvider = () => RowPair;

export type RowDiffStatus = 'same' | 'added' | 'modified' | 'deleted';

export interface RowDiff {
    delta: number;
    status: RowDiffStatus;
    oldRow?: Row;
    newRow?: Row;
}

export type RowDiffFilter = (rowDiff: RowDiff) => boolean;

export class DiffStats {
    totalComparisons = 0;
    totalChanges = 0;
    changePercent = 0;
    added = 0;
    deleted = 0;
    modified = 0;
    same = 0;

    add(rowDiff: RowDiff): void {
        this.totalComparisons++;

        if (rowDiff.status === 'added') {
            this.added++;
            this.totalChanges++;
        } else if (rowDiff.status === 'deleted') {
            this.deleted++;
            this.totalChanges++;
        } else if (rowDiff.status === 'modified') {
            this.modified++;
            this.totalChanges++;
        } else if (rowDiff.status === 'same') {
            this.same++;
        }        
        this.changePercent = roundDecimals((this.totalChanges / this.totalComparisons) * 100, 2);
    }
}

export interface InputStream {
    open(): void;
    readLine(): string | undefined;
    close(): void;
}

export interface StreamReaderOptions {
    stream: InputStream;
    delimiter?: string;
    filter?: RowFilter;
}

export interface StreamReaderHeader {
    columns: string[];
    labels?: Record<string, string>;
}

export interface StreamReaderFooter {
}

export interface StreamReader {
    open(): void;
    readHeader(): StreamReaderHeader;
    readRow(): Row | undefined;
    readFooter(): StreamReaderFooter | undefined;
    close(): void;
}

export type StreamReaderFactory = (options: StreamReaderOptions) => StreamReader;

export interface OutputStream {
    open(): void;
    writeLine(line: string): void;
    close(): void;
}

export interface StreamWriterOptions {
    stream: OutputStream;
    delimiter?: string;
    keepOldValues?: boolean;
}

export interface StreamWriterHeader {
    columns: string[];
    labels?: Record<string, string>;
}

export interface StreamWriterFooter {
    stats: DiffStats;
}

export interface StreamWriter {
    open(): void;
    writeHeader(header: StreamWriterHeader): void;
    writeDiff(rowDiff: RowDiff): void;
    writeFooter(footer: StreamWriterFooter): void;
    close(): void;
}

export type StreamWriterFactory = (options: StreamWriterOptions) => StreamWriter;


export class FileInputStream implements InputStream {
    private liner?: lineByLine;

    constructor(private readonly filename: string) {
    }

    open(): void {
        this.liner = new lineByLine(this.filename);
    }

    readLine(): string | undefined {
        if (!this.liner) {
            throw new Error(`FileInputStream "${this.filename}" is not open`);
        }
        while(true) {
            const result = this.liner.next();
            if(result === false) {
                return undefined;
            }
            const line = result.toString().trim();
            // ignore empty lines
            if (line.length > 0) {
                return line;
            }
        }
    }

    close(): void {
        if (this.liner) {
            if ((this.liner as any)['fd'] !== null) {
                this.liner.close();
            }
            this.liner = undefined;
        }
    }
}

export class ArrayInputStream implements InputStream {
    private currentIndex = 0;

    constructor(private lines: string[]) {}

    open(): void {
    }

    readLine(): string | undefined {
        if (this.currentIndex < this.lines.length) {
            const result = this.lines[this.currentIndex];
            this.currentIndex++;
            return result;    
        }
    }

    close(): void {
    }
}

export class CsvStreamReader implements StreamReader {
    private readonly stream: InputStream;
    private readonly delimiter: string;

    constructor(options: StreamReaderOptions) {
        this.stream = options.stream;
        this.delimiter = options.delimiter ?? ',';
    }

    open(): void {
        this.stream.open();
    }

    readHeader(): StreamReaderHeader {
        return {
            columns: parseCsvLine(this.delimiter, this.stream.readLine()) ?? [],
        };
    }

    readRow(): Row | undefined {
        return parseCsvLine(this.delimiter, this.stream.readLine());
    }

    readFooter(): StreamReaderFooter | undefined {
        return undefined;
    }

    close(): void {
        this.stream.close();        
    }
}

const defaultStatusColumnName = 'DIFF_STATUS';

export class CsvStreamWriter implements StreamWriter{
    private readonly stream: OutputStream;
    private readonly delimiter: string;
    private readonly keepOldValues: boolean;

    constructor(options: StreamWriterOptions) {
        this.stream = options.stream;
        this.delimiter = options.delimiter ?? ',';
        this.keepOldValues = options.keepOldValues ?? false;
    }

    open(): void {
        this.stream.open();        
    }

    writeHeader(header: StreamWriterHeader): void {
        const columns = [defaultStatusColumnName, ...header.columns];
        if (this.keepOldValues) {
            columns.push(...header.columns.map(h => 'OLD_' + h));
        }
        this.stream.writeLine(serializeRowAsCsvLine(columns, this.delimiter));
    }

    writeDiff(rowDiff: RowDiff): void {
        if (rowDiff.oldRow && rowDiff.newRow) {
            const row = [rowDiff.status, ...rowDiff.newRow];
            if (this.keepOldValues) {
                row.push(...rowDiff.oldRow);
            }
            this.stream.writeLine(serializeRowAsCsvLine(row, this.delimiter));
        } else if (rowDiff.oldRow) {
            if (this.keepOldValues) {
                const emptyRow = rowDiff.oldRow.map(_ => '');
                this.stream.writeLine(serializeRowAsCsvLine([rowDiff.status, ...emptyRow, ...rowDiff.oldRow], this.delimiter));
            } else {
                this.stream.writeLine(serializeRowAsCsvLine([rowDiff.status, ...rowDiff.oldRow], this.delimiter));

            }
        } else if (rowDiff.newRow) {
            const row = [rowDiff.status, ...rowDiff.newRow];
            if (this.keepOldValues) {
                const emptyRow = rowDiff.newRow.map(_ => '');
                row.push(...emptyRow);
            }
            this.stream.writeLine(serializeRowAsCsvLine(row, this.delimiter));
        }
    }

    writeFooter(footer: StreamWriterFooter): void {
    }

    close(): void {
        this.stream.close();
    }
}

export class JsonStreamWriter implements StreamWriter{
    private readonly stream: OutputStream;
    private readonly keepOldValues: boolean;
    private rowCount: number = 0;

    constructor(options: StreamWriterOptions) {
        this.stream = options.stream;
        this.keepOldValues = options.keepOldValues ?? false;
    }

    open(): void {
        this.stream.open();    
    }

    writeHeader(header: StreamWriterHeader): void {
        this.rowCount = 0;
        const h = JSON.stringify(header);
        this.stream.writeLine(`{ "header": ${h}, "items": [`);
    }

    writeDiff(rowDiff: RowDiff): void {
        const record: any = {
            status: rowDiff.status,
        };
        if (this.keepOldValues) {
            if (rowDiff.newRow) {
                record.new = rowDiff.newRow;
            }
            if (rowDiff.oldRow) {
                record.old = rowDiff.oldRow;
            }    
        } else {
            record.data = rowDiff.newRow ?? rowDiff.oldRow;
        }
        const separator = this.rowCount === 0 ? '' : ',';
        this.rowCount++;
        this.stream.writeLine(separator + JSON.stringify(record));
    }

    writeFooter(footer: StreamWriterFooter): void {
        this.stream.writeLine(`], "footer": ${JSON.stringify(footer)}}`);
    }

    close(): void {
        this.stream.close();   
    }
}

export class NullStreamWriter implements StreamWriter {
    open(): void {
        
    }

    writeHeader(header: StreamWriterHeader): void {
    }

    writeDiff(rowDiff: RowDiff): void {
    }

    writeFooter(footer: StreamWriterFooter): void {
    }

    close(): void {
        
    }
}

export class NullOutputStream implements OutputStream {
    open(): void {}

    writeLine(line: string): void {}

    close(): void {}
}

export class ConsoleOutputStream implements OutputStream {
    open(): void {}

    writeLine(line: string): void {
        console.log(line);
    }

    close(): void {}
}

export class FileOutputStream implements OutputStream {
    private fd: number = 0;

    constructor(public readonly path: string) {
    }
    
    open(): void {
        if (this.fd !== 0) {
            throw new Error(`file "${this.path}" is already open`);
        }
        this.fd = fs.openSync(this.path, 'w');
    }

    writeLine(line: string): void {
        fs.writeSync(this.fd, line + '\n');
    }

    close(): void {
        if (this.fd !== 0) {
            fs.closeSync(this.fd);
            this.fd = 0;
        }
    }
}

export type Filename = string;

export interface SourceOptions {
    stream: Filename | InputStream;
    format?: 'csv' | 'json' | StreamReaderFactory; // Defaults to CSV
    delimiter?: string;
    filter?: RowFilter;
}

export interface OutputOptions {
    stream?:  'console' | 'null' | Filename | OutputStream; // Defaults to console
    format?: 'csv' | 'json' | StreamWriterFactory; // Defaults to CSV
    delimiter?: string;
    keepOldValues?: boolean;
    keepSameRows?: boolean;
    changeLimit?: number;
    filter?: RowDiffFilter;
    labels?: Record<string, string>;
}

export interface DifferOptions {
    oldSource: Filename | SourceOptions; 
    newSource: Filename | SourceOptions; 
    keyFields: string[];
    includedFields?: string[];
    excludedFields?: string[];
    descendingOrder?: boolean;
}

export function diff(options: DifferOptions): Differ {
    return new Differ(options);
}


function createInputStream(options: SourceOptions): InputStream {
    if (typeof options.stream === 'string') {
        return new FileInputStream(options.stream);
    }
    return options.stream;
}

function createStreamReader(options: SourceOptions): StreamReader {
    const stream = createInputStream(options);
    const readerOptions: StreamReaderOptions = { 
        stream, 
        delimiter: options.delimiter,
        filter: options.filter,
    };
    if (options.format === 'csv') {
        return new CsvStreamReader(readerOptions);
    } 
    if (options.format === 'json') {
        throw new Error('not implemented');
    }
    if (options.format !== undefined) {
        return options.format(readerOptions);
    }
    return new CsvStreamReader(readerOptions);    
}

function createSource(value: Filename | SourceOptions): { reader: StreamReader, filter?: RowFilter} {
    if (typeof value === 'string') {
        return { reader: new CsvStreamReader({ stream: new FileInputStream(value) }) };
    }
    return { reader: createStreamReader(value), filter: value.filter };
}

function createStreamWriter(options: OutputOptions): StreamWriter {
    const stream = createOutputStream(options);
    const writerOptions: StreamWriterOptions = { 
        stream, 
        delimiter: options.delimiter,
        keepOldValues: options.keepOldValues,
    };
    if (options.format === 'csv') {
        return new CsvStreamWriter(writerOptions);
    }
    if (options.format === 'json') {
        return new JsonStreamWriter(writerOptions);
    }
    if (options.format !== undefined) {
        return options.format(writerOptions);
    }
    if (options.stream === 'null') {
        return new NullStreamWriter();
    }
    return new CsvStreamWriter(writerOptions);
}

function createOutputStream(options: OutputOptions): OutputStream {
    if (options.stream === 'console') {
        return new ConsoleOutputStream();
    } 
    if (options.stream === 'null') {
        return new NullOutputStream();
    }
    if (typeof options.stream === 'string') {
        return new FileOutputStream(options.stream);
    }
    if (options.stream) {
        return options.stream;
    }
    return new ConsoleOutputStream();    
}

function createOutput(value: 'console' | 'null' | Filename | OutputOptions): { 
    writer: StreamWriter, 
    filter?: RowDiffFilter,
    keepSameRows?: boolean, 
    changeLimit?: number,
    labels?: Record<string, string>;
} {
    if (value === 'console') {
        return { writer: new CsvStreamWriter({ stream: new ConsoleOutputStream() }) };
    }
    if (value === 'null') {
        return { writer: new NullStreamWriter() };
    }
    if (typeof value === 'string') {
        return { writer: new CsvStreamWriter({ stream: new FileOutputStream(value) }) };
    }
    return { 
        writer: createStreamWriter(value), 
        filter: value.filter, 
        keepSameRows: value.keepSameRows, 
        changeLimit: value.changeLimit,
        labels: value.labels,
    };
}

export class Differ {
    private stats = new DiffStats();
    private isOpen = false;
    private oldReader: StreamReader;
    private oldRowFilter?: RowFilter;
    private newReader: StreamReader;
    private newRowFilter?: RowFilter;
    private comparer: RowComparer = defaultRowComparer;
    private oldHeaders: Row = [];
    private newHeaders: Row = [];
    private headers: RowHeader[] = [];
    private keys: RowHeader[] = [];
    private headersWithoutKeys: RowHeader[] = [];
    private normalizeOldRow: RowNormalizer = row => row;
    private normalizeNewRow: RowNormalizer = row => row;

    constructor(private options: DifferOptions) {
        const oldSource = createSource(options.oldSource);
        this.oldReader = oldSource.reader;
        this.oldRowFilter = oldSource.filter;
        const newSource = createSource(options.newSource);
        this.newReader = newSource.reader;
        this.newRowFilter = newSource.filter;
        this.comparer = options.descendingOrder === true ? 
                            invertRowComparer(defaultRowComparer) : 
                            defaultRowComparer;
    }

    open(): void {
        if (!this.isOpen) {
            this.isOpen = true;
            this.stats = new DiffStats();
            this.oldReader.open();
            this.newReader.open();
            this.extractHeaders();
        }
    }
    
    close(): void {
        if (this.isOpen) {
            this.newReader.close();
            this.oldReader.close();
            this.isOpen = false;
        }
    }

    getHeaders(): string[] {
        if (this.headers.length === 0) {
            this.open();
        }
        return this.headers.map(h => h.name);
    }

    getStats(): DiffStats {
        return this.stats;
    }

    to(options: 'console' | 'null' | Filename | OutputOptions): DiffStats {
        const output = createOutput(options);
        const columns = this.getHeaders();
        output.writer.open();
        try {
            output.writer.writeHeader({
                columns,
                labels: output.labels,
            });
            for (const rowDiff of this) {
                let canWriteDiff = output.keepSameRows === true || rowDiff.status !== 'same';
                if (canWriteDiff && output.filter) { 
                    canWriteDiff = output.filter(rowDiff);
                }
                if (canWriteDiff) { 
                    output.writer.writeDiff(rowDiff);
                }
                if (typeof output.changeLimit === 'number' && this.stats.totalChanges >= output.changeLimit) {
                    break;
                }
            }    
            output.writer.writeFooter({ stats: this.stats });
        } finally {
            output.writer.close();
        }
        return this.stats;
    }
    
    *[Symbol.iterator]() {
        this.open();
        try {
            let pairProvider: RowPairProvider = () => this.getNextPair();
            let previousPair: RowPair = {}
            while (true) {
                const pair = pairProvider();

                if (pair.oldRow === undefined && pair.newRow === undefined) {
                    break;
                }
                this.ensurePairsAreInAscendingOrder(previousPair, pair);
                previousPair = pair;

                const res = this.evalPair(pair);
                this.stats.add(res);
                yield res;    

                if (res.delta === 0) {
                    pairProvider = () => this.getNextPair();
                } else if (res.delta > 0) {
                    pairProvider = () => ({ oldRow: pair.oldRow, newRow: this.getNextNewRow() });
                } else {
                    pairProvider = () => ({ oldRow: this.getNextOldRow(), newRow: pair.newRow });
                }
            }
        } finally {
            this.oldReader.readFooter();
            this.newReader.readFooter();
            this.close();
        }
    }

    private extractHeaders() {
        this.oldHeaders = this.oldReader.readHeader().columns;
        this.newHeaders = this.newReader.readHeader().columns;
        if (this.oldHeaders.length === 0) {
            throw new Error('Expected to find headers in old source');
        }
        if (this.newHeaders.length === 0) {
            throw new Error('Expected to find headers in new source');
        }
        this.keys = this.normalizeKeys(this.oldHeaders, this.newHeaders, this.options.keyFields);
        this.headers = this.normalizeHeaders(this.oldHeaders, this.newHeaders);
        this.headersWithoutKeys = this.headers.filter(x => !this.keys.some(y => y.name === x.name));
        if (!sameArrays(this.oldHeaders, this.headers.map(h => h.name))) {
            this.normalizeOldRow = row => row ? this.headers.map(h => row[h.oldIndex]) : undefined;
        }
        if (!sameArrays(this.newHeaders, this.headers.map(h => h.name))) {
            this.normalizeNewRow = row => row ? this.headers.map(h => row[h.newIndex]) : undefined;
        }
    }

    private normalizeHeaders(oldHeaders: Row, newHeaders: Row) {
        const includedFields = new Set<string>(this.options.includedFields);
        const excludedFields = new Set<string>(this.options.excludedFields);
        const headers: RowHeader[] = [];
        for (let i = 0; i < newHeaders.length; i++) {
            const h = newHeaders[i];
            const isIncluded = includedFields.size === 0 || includedFields.has(h);
            if (isIncluded) {
                const isExcluded = excludedFields.has(h);
                if (!isExcluded) {
                    const oldIdx = oldHeaders.indexOf(h);
                    if (oldIdx < 0) {
                        throw new Error(`Could not find new header '${h}' in old headers:\nold=${oldHeaders}\nnew=${newHeaders}`);
                    }
                    headers.push({
                        name: h,
                        newIndex: i,
                        oldIndex: oldIdx,
                    });
                }
            }
        }
        return headers;
    }

    private normalizeKeys(oldHeaders: Row, newHeaders: Row, keyFields: string[]) {
        const headers: RowHeader[] = [];
        for (const keyField of keyFields) {
            const oldIndex = oldHeaders.indexOf(keyField);
            if (oldIndex < 0) {
                throw new Error(`Could not find key '${keyField}' in old headers: ${oldHeaders}`);
            }
            const newIndex = newHeaders.indexOf(keyField);
            if (newIndex < 0) {
                throw new Error(`Could not find key '${keyField}' in new headers: ${newHeaders}`);
            }
            headers.push({
                name: keyField,
                newIndex,
                oldIndex,
            });

        }
        return headers;
    }

    private getNextOldRow(): Row | undefined {
        return nextFilteredRow(this.oldReader, this.oldRowFilter);
    }

    private getNextNewRow(): Row | undefined {
        return nextFilteredRow(this.newReader, this.newRowFilter);
    }

    private getNextPair(): RowPair {
        const oldRow = this.getNextOldRow();
        const newRow = this.getNextNewRow();
        return { oldRow, newRow };
    }

    private evalPair(pair: RowPair): RowDiff {
        const delta = this.comparer(this.keys, pair.oldRow, pair.newRow);
        if (delta === 0) {
            const areSame = this.comparer(this.headersWithoutKeys, pair.oldRow, pair.newRow) === 0;
            const newRow = this.normalizeNewRow(pair.newRow);
            const oldRow = this.normalizeOldRow(pair.oldRow);
            return { delta, status: areSame ? 'same' : 'modified', oldRow, newRow };
        } else if (delta < 0) {
            const oldRow = this.normalizeOldRow(pair.oldRow);
            return { delta, status: 'deleted', oldRow };
        } else {
            const newRow = this.normalizeNewRow(pair.newRow);
            return { delta, status: 'added', newRow };
        }
    }

    private ensureRowsAreInAscendingOrder(source: string, previous?: Row, current?: Row) {
        if (previous && current) {
            const oldDelta = this.comparer(this.keys, previous, current);
            if (this.options.descendingOrder === true) {
                if (oldDelta > 0) {
                    // console.log(`Expected rows to be in descending order in ${source}  source but received: previous=${previous}, current=${current}`);
                    throw new Error(`Expected rows to be in descending order in ${source} source but received: previous=${previous}, current=${current}`);
                }        
            } else {
                if (oldDelta > 0) {
                    // console.log(`Expected rows to be in ascending order in ${source}  source but received: previous=${previous}, current=${current}`);
                    throw new Error(`Expected rows to be in ascending order in ${source} source but received: previous=${previous}, current=${current}`);
                }        
            }
        }
    }

    private ensurePairsAreInAscendingOrder(previous: RowPair, current: RowPair) {
        this.ensureRowsAreInAscendingOrder('old', previous.oldRow, current.oldRow);
        this.ensureRowsAreInAscendingOrder('new', previous.newRow, current.newRow);
    }
}

export function parseCsvLine(delimiter: string, line?: string): Row | undefined {
    if (line) {
        const fields: Row = [];
        let idx = 0;
        let prevIdx = 0;
        let c = '';
        while (idx < line.length) {
            c = line[idx];
            if (c === '"') {
                idx++;
                let hasEscapedDoubleQuote = false;
                const startIdx = idx;
                while (idx < line.length) {
                    if (line[idx] === '"' && idx < line.length - 1 && line[idx+1] === '"') {
                        // skip escaped double quotes
                        idx++;    
                        hasEscapedDoubleQuote = true;
                    } else if (line[idx] === '"') {
                        break;
                    }
                    idx++;
                }
                let value = line.substring(startIdx, idx)
                if (hasEscapedDoubleQuote) {
                    value = value.replaceAll('""', '"');
                }
                fields.push(value);
                idx++;
                if (line[idx] === delimiter) {
                    idx++;
                }
                prevIdx = idx;
            } else if (c === delimiter) {
                const value = line.substring(prevIdx, idx);
                fields.push(value);
                idx++;
                prevIdx = idx;
            } else {
                idx++;
            }
        }
        if (prevIdx < idx) {
            const value = line.substring(prevIdx, idx);
            fields.push(value);    
        } else if (c === delimiter) {
            fields.push('');
        }
        return fields;
    }
}

function nextFilteredRow(reader: StreamReader, filter?: RowFilter): Row | undefined {
    while (true) {
        const row = reader.readRow();
        if (row && filter) {
            if (!filter(row)) {
                continue;
            }
        }
        return row;
    }
}

const charsToEncodeRegEx = /,|"/;
export function serializeCsvField(value : string): string {
    if (charsToEncodeRegEx.test(value)) {
        return `"${value.replaceAll('"', '""')}"`;
    }
    return value;
}

export function serializeRowAsCsvLine(row : Row, delimiter?: string) {
    return row.map(serializeCsvField).join(delimiter ?? ',');
}

export function defaultRowComparer(keys: RowHeader[], a? : Row, b?: Row): number {
    if (keys.length === 0) {
        throw new Error('Expected to have at least one key in keys parameter');
    }
    if (a === undefined && b === undefined) {
        return 0;
    }
    if (a === undefined && b !== undefined) {
        return 1;
    }
    if (a !== undefined && b === undefined) {
        return -1;
    }
    for (const key of keys) {
        const aa = a![key.oldIndex] ?? '';
        const bb = b![key.newIndex] ?? '';
        // We can't use localeCompare since the ordered csv file produced by SQLite won't use the same locale
        // const delta = aa.localeCompare(bb);
        const delta = aa === bb ? 0 : aa < bb ? -1 : 1;
        if (delta !== 0) {
            return delta;
        }
    }
    return 0;
}

export function invertRowComparer(comparer: RowComparer): RowComparer {
    return (keys, a, b) => {
        let delta = comparer(keys, a, b);
        if (delta !== 0) {
            delta = - delta;
        }
        return delta;
    };
}

export function roundDecimals(value: number, decimals: number) {
    const pow = Math.pow(10, decimals)
    return Math.round(value * pow) / pow
}
  
export function sameArrays(a: string[], b: string[]) {
    if (a.length !== b.length) {
        return false;
    }
    for (let i = 0; i < a.length; i++) {
        if (a[i] !== b[i]) {
            return false;
        }
    }
    return true;
}
