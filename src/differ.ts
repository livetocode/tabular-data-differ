import fs from 'fs';
import lineByLine from 'n-readlines';

//TODO: make stream operations async

export class UnorderedStreamsError extends Error {
}

export type ColumnComparer = (a: string, b: string) => number;

export type Row = string[];

export type RowFilter = (row: Row) => boolean;

export interface RowPair {
    oldRow?: Row;
    newRow?: Row;
}

export type RowComparer = (keys: Column[], a? : Row, b?: Row) => number;

export type RowNormalizer = (row?: Row) => Row | undefined;

export type Column = {
    name: string;
    oldIndex: number;
    newIndex: number;
    comparer?: ColumnComparer;
    order?: ColumnOrdering;
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
}

export interface StreamReaderHeader {
    columns: string[];
    labels?: Record<string, string>;
}

export interface StreamReader {
    open(): void;
    readHeader(): StreamReaderHeader;
    readRow(): Row | undefined;
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

    close(): void {
        this.stream.close();        
    }
}

export function parseJsonObj(line?: string) {
    if (line === undefined) {
        return undefined;
    }
    let text = line.trim();
    if (text.startsWith('[')) {
        text = text.substring(1);
    }
    if (text.endsWith(']')) {
        text = text.substring(0, text.length - 1);
    }
    if (text.startsWith(',')) {
        text = text.substring(1);
    }
    if (text.endsWith(',')) {
        text = text.substring(0, text.length - 1);
    }
    if (text === '') {
        return undefined;
    }
    if (text.startsWith('{') && text.endsWith('}')) {
        const obj = JSON.parse(text);
        return obj;
    }
    throw new Error('Expected to find a JSON object');
}

export function convertJsonObjToRow(obj: any, columns: string[]): Row | undefined {
    if (obj === null || obj === undefined) {
        return undefined;
    }
    const row = columns.map(col => `${obj[col]}`);
    return row;
}

export class JsonStreamReader implements StreamReader {
    private readonly stream: InputStream;
    private headerObj: any;
    private columns: string[] = [];

    constructor(options: StreamReaderOptions) {
        this.stream = options.stream;
    }

    open(): void {
        this.stream.open();
        this.headerObj = null;
        this.columns = [];
    }

    readHeader(): StreamReaderHeader {
        let line = this.stream.readLine();
        this.headerObj = parseJsonObj(line);
        if (!this.headerObj) {
            // if the obj is undefined, it might mean that we just started an array with a single line containing '['
            // so, process the next line
            line = this.stream.readLine();
            this.headerObj = parseJsonObj(line);
        }
        if (!this.headerObj) {
            throw new Error('Expected to find at least one object');
        }
        this.columns = Object.keys(this.headerObj);
        return {
            columns: this.columns,
        };
    }

    readRow(): Row | undefined {
        if (this.headerObj) {
            const row = convertJsonObjToRow(this.headerObj, this.columns);
            this.headerObj = null;
            return row;
        }
        const line = this.stream.readLine();
        const obj = parseJsonObj(line);
        const row = convertJsonObjToRow(obj, this.columns);

        return row;
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
            columns.push(...header.columns.map(col => 'OLD_' + col));
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

/** 
 * A string containing a filename 
 */
export type Filename = string;

/**
 * Options for configuring an input stream that will be compared to another similar stream
 * in order to obtain the changes between those two sources.
 */
export interface SourceOptions {
    /**
     * Specifies the input stream, either providing a string filename or a custom instance of an InputStream
     */
    stream: Filename | InputStream;
    /**
     * Specifies the format of the input stream, either providing a standard format (csv or json) or your factory function for producing a custom StreamReader instance. 
     * Defaults to 'csv'.
     */
    format?: 'csv' | 'json' | StreamReaderFactory; // Defaults to CSV
    /**
     * Specifies the char delimiting the fields in a row.
     * Defaults to ','. 
     * Used only by the CSV format.
     */
    delimiter?: string;
    /**
     * Specifies a filter to allow or reject the input rows
     */
    filter?: RowFilter;
}

/**
 * Options for configuring the output destination of the changes emitted by the Differ object
 */
export interface OutputOptions {
    /**
     * Specifies a standard output (console, null), a string filename or an instance of an InputStream (like FileInputStream). 
     * Defaults to 'console'.
     */
    stream?:  'console' | 'null' | Filename | OutputStream;
    /**
     * Specifies an existing format (csv or json) or a factory function to create your own format.
     * Defaults to 'csv'.
     */
    format?: 'csv' | 'json' | StreamWriterFactory;
    /**
     * Specifies the char delimiting the fields in a row.
     * Defaults to ','. 
     * Used only by the CSV format.
     */
     delimiter?: string;
    /**
     * Specifies if the output should contain both the old and new values for each row.
     */
    keepOldValues?: boolean;
    /**
     * Specifies if the output should also contain the rows that haven't changed.
     */
    keepSameRows?: boolean;
    /**
     * Specifies a maximum number of differences that should be outputted.
     */
    changeLimit?: number;
    /**
     * Specifies a filter to select which changes should be sent to the output stream.
     */
     filter?: RowDiffFilter;
     /**
      * Specifies a dictionary of key/value that allows to add custom metadata to the generated file.
      */
    labels?: Record<string, string>;
}

export type ColumnOrdering = 'ASC' | 'DESC';

export interface ColumnDefinition {
    /**
     * the name of the column.
     */
    name: string;
    /**
     * either a standard comparer ('string' or 'number') or a custom comparer.
     */
    comparer?: 'string' | 'number' | ColumnComparer;
    /**
     * specifies if the column is in ascending (ASC) or descending (DESC) order.
     */
    order?: ColumnOrdering;
}

/**
 * Options for configuring the Differ object that will traverse two input streams in parallel in order to compare their rows
 * and produce a change set.
 */
export interface DifferOptions {
    /**
     * Configures the old source
     */
    oldSource: Filename | SourceOptions; 
    /**
     * Configures the new source
     */
     newSource: Filename | SourceOptions; 
     /**
      * Configures the primary keys used to compare the rows between the old and new sources
      */
    keys: (string | ColumnDefinition)[];
    /**
     * the list of columns to keep from the input sources. If not specified, all columns are selected.
     */
    includedColumns?: string[];
    /**
     * the list of columns to exclude from the input sources.
     */
    excludedColumns?: string[];
    /**
     * Specifies a custom row comparer
     */
    rowComparer?: RowComparer;
}

/**
 * Creates a new differ object allowing you to compare two input streams and eventually send the changes to a specific output.
 * @param options the options required to compare two streams
 * @returns a Differ instance
 * @example
 * import { diff } from 'tabular-data-differ';
 * const stats = diff({
 *   oldSource: './tests/a.csv',
 *   newSource: './tests/b.csv',
 *   keyFields: ['id'],
 * }).to('console');
 * console.log(stats);
 */
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
    };
    if (options.format === 'csv') {
        return new CsvStreamReader(readerOptions);
    } 
    if (options.format === 'json') {
        return new JsonStreamReader(readerOptions);
    }
    if (options.format !== undefined) {
        return options.format(readerOptions);
    }
    return new CsvStreamReader(readerOptions);    
}

interface Source {
    reader: StreamReader;
}

function createSource(value: Filename | SourceOptions): Source {
    if (typeof value === 'string') {
        return { 
            reader: new CsvStreamReader({ stream: new FileInputStream(value) }) 
        };
    }
    return { 
        reader: createStreamReader(value), 
    };
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
    
    constructor(private options: DifferOptions) {
    }

    start(): DifferContext {
        const ctx = new DifferContext(this.options);
        ctx[OpenSymbol]();
        return ctx;
    }

    /**
     * Iterates over the changes and sends them to the submitted output.
     * @param options a standard ouput such as console or null, a string filename or a custom OutputOptions.
     * @returns the change stats once all the changes have been processed. 
     * Note that the stats might be different from getStats() when there is a filter in the output options, 
     * as the differ stats are updated by the iterator which doesn't have any filter.
     * @throws {UnorderedStreamsError}
     * @example
     * import { diff } from 'tabular-data-differ';
     * const stats = diff({
     *   oldSource: './tests/a.csv',
     *   newSource: './tests/b.csv',
     *   keyFields: ['id'],
     * }).to('console');
     * console.log(stats);
     */
    to(options: 'console' | 'null' | Filename | OutputOptions): DiffStats {
        const ctx = this.start();
        return ctx.to(options);
    }    
}

const OpenSymbol = Symbol('open');

export class DifferContext {
    private _stats = new DiffStats();
    private _columnNames: string[] = [];
    private _isOpen = false;
    private _isClosed = false;
    private oldSource: Source;
    private newSource: Source;
    private comparer: RowComparer = defaultRowComparer;
    private keys: Column[] = [];
    private _columns: Column[] = [];
    private columnsWithoutKeys: Column[] = [];
    private normalizeOldRow: RowNormalizer = row => row;
    private normalizeNewRow: RowNormalizer = row => row;

    constructor(private options: DifferOptions) {
        this.oldSource = createSource(options.oldSource);
        this.newSource = createSource(options.newSource);
        this.comparer = options.rowComparer ?? defaultRowComparer;
    }

    /**
     * Opens the input streams (old and new) and reads the nam.
     * This is an internal method that will be automatically called by "Differ.start" method.
     */
    [OpenSymbol](): void {
        if (!this._isOpen) {
            this._isOpen = true;
            this.oldSource.reader.open();
            this.newSource.reader.open();
            this.extractHeaders();
        }
    }

    /**
     * Closes the input streams.
     * This will be automatically called by the "diffs" or "to" methods.
     * This does nothing if the streams are not open.
     */
    close(): void {
        if (this._isOpen) {
            this.newSource.reader.close();
            this.oldSource.reader.close();
            this._isOpen = false;
        }
        this._isClosed = true;
    }

    /**
     * tells if the input streams are open or not
     */
    get isOpen() {
        return this._isOpen;
    }

    /**
     * gets the normalized column names from the old and new streams, according to the includedFields/excludedFields constraints.
     * @returns a list of column names
     */
    get columns(): string[] {
        return this._columnNames;
    }

    /**
     * gets the diff stats
     * @returns the diff stats
     */
    get stats(): DiffStats {
        return this._stats;
    }
    
    /**
     * Iterates over the changes and sends them to the submitted output.
     * @param options a standard ouput such as console or null, a string filename or a custom OutputOptions.
     * @returns the change stats once all the changes have been processed. 
     * Note that the stats might be different from "DiffContext.stats" when there is a filter in the output options, 
     * as the context stats are updated by the iterator which doesn't have any filter.
     * @throws {UnorderedStreamsError}
     * @example
     * import { diff } from 'tabular-data-differ';
     * const stats = diff({
     *   oldSource: './tests/a.csv',
     *   newSource: './tests/b.csv',
     *   keyFields: ['id'],
     * }).to('console');
     * console.log(stats);
     */
     to(options: 'console' | 'null' | Filename | OutputOptions): DiffStats {
        const stats = new DiffStats();
        const output = createOutput(options);
        output.writer.open();
        try {
            output.writer.writeHeader({
                columns: this.columns,
                labels: output.labels,
            });
            for (const rowDiff of this.diffs()) {
                let isValidDiff = output.filter?.(rowDiff) ?? true;
                if (isValidDiff) {
                    stats.add(rowDiff);
                }
                let canWriteDiff = output.keepSameRows === true || rowDiff.status !== 'same';
                if (isValidDiff && canWriteDiff) { 
                    output.writer.writeDiff(rowDiff);
                }
                if (typeof output.changeLimit === 'number' && stats.totalChanges >= output.changeLimit) {
                    break;
                }
            }    
            output.writer.writeFooter({ stats: stats });
        } finally {
            output.writer.close();
        }
        return stats;
    }

    /**
     * Enumerates the differences between two input streams (old and new).
     * @yields {RowDiff}
     * @throws {UnorderedStreamsError}
     * @example
     * import { diff, ArrayInputStream } from 'tabular-data-differ';
     * const ctx = diff({
     *     oldSource: {
     *         stream: new ArrayInputStream([
     *             'id,name',
     *             '1,john',
     *             '2,mary',
     *         ]),
     *     },
     *     newSource: {
     *         stream: new ArrayInputStream([
     *             'id,name',
     *             '1,john',
     *             '3,sarah',
     *         ]),
     *     },
     *     keyFields: ['id'],
     * }).start();
     * console.log('columns:', ctx.getColumns());
     * for (const rowDiff of ctx.diffs()) {
     *     console.log(rowDiff);
     * }
     * console.log('stats:', ctx.getStats());
     */
    *diffs() {
        if (this._isClosed) {
            throw new Error('Cannot get diffs on closed streams. You should call "Differ.start()" again.');
        }
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

                const rowDiff = this.evalPair(pair);
                this.stats.add(rowDiff);
                yield rowDiff;    

                if (rowDiff.delta === 0) {
                    pairProvider = () => this.getNextPair();
                } else if (rowDiff.delta > 0) {
                    pairProvider = () => ({ oldRow: pair.oldRow, newRow: this.getNextNewRow() });
                } else {
                    pairProvider = () => ({ oldRow: this.getNextOldRow(), newRow: pair.newRow });
                }
            }
        } finally {
            this.close();
        }
    }

    private extractHeaders() {
        const oldHeader = this.oldSource.reader.readHeader();
        const newHeader = this.newSource.reader.readHeader();
        if (oldHeader.columns.length === 0) {
            throw new Error('Expected to find columns in old source');
        }
        if (newHeader.columns.length === 0) {
            throw new Error('Expected to find columns in new source');
        }
        this._columns = this.normalizeColumns(oldHeader.columns, newHeader.columns);
        this.keys = this.extractKeys(this._columns, this.options.keys.map(asColumnDefinition));
        this.columnsWithoutKeys = this._columns.filter(col => !this.keys.some(key => key.name === col.name));
        this._columnNames = this._columns.map(col => col.name);
        if (!sameArrays(oldHeader.columns, this._columns.map(col => col.name))) {
            this.normalizeOldRow = row => row ? this._columns.map(col => row[col.oldIndex] ?? '') : undefined;
        }
        if (!sameArrays(newHeader.columns, this._columns.map(col => col.name))) {
            this.normalizeNewRow = row => row ? this._columns.map(col => row[col.newIndex]) : undefined;
        }
    }

    private normalizeColumns(oldColumns: Row, newColumns: Row) {
        const includedColumns = new Set<string>(this.options.includedColumns);
        const excludedColumns = new Set<string>(this.options.excludedColumns);
        const columns: Column[] = [];
        for (let newIndex = 0; newIndex < newColumns.length; newIndex++) {
            const name = newColumns[newIndex];
            const isIncluded = includedColumns.size === 0 || includedColumns.has(name);
            if (isIncluded) {
                const isExcluded = excludedColumns.has(name);
                if (!isExcluded) {
                    const oldIndex = oldColumns.indexOf(name);
                    columns.push({
                        name,
                        newIndex,
                        oldIndex,
                    });
                }
            }
        }
        return columns;
    }

    private extractKeys(columns: Column[], keys: ColumnDefinition[]) {
        const result: Column[] = [];
        for (const key of keys) {
            const column = columns.find(col => col.name === key.name);
            if (column) {
                if (column.oldIndex < 0) {
                    throw new Error(`Could not find key '${key.name}' in old stream`);
                }
                result.push({
                    ...column,
                    comparer: asColumnComparer(key.comparer),
                    order: key.order,
                });
            } else {
                throw new Error(`Could not find key '${key.name}' in new stream`);
            }
        }
        return result;
    }

    private getNextOldRow(): Row | undefined {
        return this.oldSource.reader.readRow();
    }

    private getNextNewRow(): Row | undefined {
        return this.newSource.reader.readRow();
    }

    private getNextPair(): RowPair {
        const oldRow = this.getNextOldRow();
        const newRow = this.getNextNewRow();
        return { oldRow, newRow };
    }

    private evalPair(pair: RowPair): RowDiff {
        const delta = this.comparer(this.keys, pair.oldRow, pair.newRow);
        const newRow = this.normalizeNewRow(pair.newRow);
        const oldRow = this.normalizeOldRow(pair.oldRow);
        if (delta === 0) {
            const areSame = this.comparer(this.columnsWithoutKeys, pair.oldRow, pair.newRow) === 0;
            return { delta, status: areSame ? 'same' : 'modified', oldRow, newRow };
        } else if (delta < 0) {
            return { delta, status: 'deleted', oldRow };
        }
        return { delta, status: 'added', newRow };        
    }

    private ensureRowsAreInAscendingOrder(source: string, previous?: Row, current?: Row) {
        if (previous && current) {
            const oldDelta = this.comparer(this.keys, previous, current);
            if (oldDelta > 0) {
                const colOrder = this.keys.map(key => `${key.name} ${key.order ?? 'ASC'}`);
                throw new UnorderedStreamsError(`Expected rows to be ordered by "${colOrder}" in ${source} source but received:\n  previous=${previous}\n  current=${current}`);
            }        
        }
    }

    private ensurePairsAreInAscendingOrder(previous: RowPair, current: RowPair) {
        this.ensureRowsAreInAscendingOrder('old', previous.oldRow, current.oldRow);
        this.ensureRowsAreInAscendingOrder('new', previous.newRow, current.newRow);
    }
}

function asColumnDefinition(value: string | ColumnDefinition): ColumnDefinition {
    if (typeof value === 'string') {
        return { name: value };
    }
    return value;
}

function asColumnComparer(comparer?: 'string' | 'number' | ColumnComparer) : ColumnComparer | undefined {
    if (comparer === 'string') {
        return stringComparer;
    }
    if (comparer === 'number') {
        return numberComparer;
    }
    return comparer;
}

export function parseCsvLine(delimiter: string, line?: string): Row | undefined {
    if (line) {
        const row: Row = [];
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
                row.push(value);
                idx++;
                if (line[idx] === delimiter) {
                    idx++;
                }
                prevIdx = idx;
            } else if (c === delimiter) {
                const value = line.substring(prevIdx, idx);
                row.push(value);
                idx++;
                prevIdx = idx;
            } else {
                idx++;
            }
        }
        if (prevIdx < idx) {
            const value = line.substring(prevIdx, idx);
            row.push(value);    
        } else if (c === delimiter) {
            row.push('');
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

export function stringComparer(a: string, b: string): number {
    // We can't use localeCompare since the ordered csv file produced by SQLite won't use the same locale
    // return a.localeCompare(b);
    if (a === b) {
        return 0;
    } else if (a < b) {
        return -1;
    } 
    return 1;
}

export function numberComparer(a: string, b: string): number {
    // keep numbers as strings when comparing for equality
    // since it avoids doing the number conversion and will also avoid floating point errors
    if (a === b) {
        return 0;
    }
    const aa = parseFloat(a);
    const bb = parseFloat(b);
    if (aa < bb) {
        return -1;
    }
    return 1;
}

export function defaultRowComparer(columns: Column[], a? : Row, b?: Row): number {
    if (columns.length === 0) {
        throw new Error('Expected to have at least one entry in the columns parameter');
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
    for (const col of columns) {
        const aa = a![col.oldIndex] ?? '';
        const bb = b![col.newIndex] ?? '';
        const comparer = col.comparer ?? stringComparer;
        let delta = comparer(aa, bb);
        if (delta !== 0 && col.order === 'DESC') {
            delta = - delta;
        }
        if (delta !== 0) {
            return delta;
        }
    }
    return 0;
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
