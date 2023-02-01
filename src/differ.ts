import { 
    ConsoleOutputStream, 
    FileOutputStream,
    Filename,
} from "./streams";
import { 
    Row, 
    RowDiffFilter, 
    ColumnComparer, 
    ColumnOrdering, 
    RowComparer, 
    FormatReader, 
    CsvFormatReader, 
    JsonFormatReader, 
    FormatWriter, 
    CsvFormatWriter, 
    JsonFormatWriter, 
    NullFormatWriter, 
    DiffStats, 
    defaultRowComparer, 
    Column, 
    RowNormalizer, 
    RowDiff, 
    stringComparer, 
    numberComparer,
    CsvFormatReaderOptions,
    CsvFormatWriterOptions,
    JsonFormatReaderOptions,
    JsonFormatWriterOptions,
    IterableFormatReaderOptions,
    IterableFormatReader,
} from "./formats";

export class UnorderedStreamsError extends Error {
}

export class UniqueKeyViolationError extends Error {
}

export interface RowPair {
    oldRow?: Row;
    newRow?: Row;
}

export type RowPairProvider = () => Promise<RowPair>;

/**
 * Options for configuring a source stream as a CSV stream
 */
export type CsvSource = {
    format: 'csv';
} & CsvFormatReaderOptions;

/**
 * Options for configuring a destination stream as a CSV stream
 */
export type CsvDestination = {
    format: 'csv';
} & CsvFormatWriterOptions;

/**
 * Options for configuring a source stream as a TSV stream
 */
export type TsvSource = {
    format: 'tsv';
} & CsvFormatReaderOptions;

/**
 * Options for configuring a destination stream as a TSV stream
 */
export type TsvDestination = {
    format: 'tsv';
} & CsvFormatWriterOptions;

/**
 * Options for configuring a source stream as a JSON stream
 */
export type JsonSource = {
    format: 'json';
} & JsonFormatReaderOptions;

/**
 * Options for configuring a destination stream as a JSON stream
 */
export type JsonDestination = {
    format: 'json';
} & JsonFormatWriterOptions;

/**
 * Options for configuring a source as an iterable generator
 */
export type IterableSource = {
    format: 'iterable';
} & IterableFormatReaderOptions;

/**
 * Options for configuring a source as a custom format
 */
export type CustomSource = {
    format: 'custom';
    reader: FormatReader;
}

/**
 * Options for configuring a destination as a custom format
 */
export type CustomDestination = {
    format: 'custom';
    writer: FormatWriter;
}

/**
 * Options for configuring a source of data
 */
export type SourceOptions = 
    | CsvSource 
    | TsvSource 
    | JsonSource 
    | IterableSource
    | CustomSource;

/**
 * Options for configuring a destination of data
 */
export type DestinationOptions = 
    | CsvDestination 
    | TsvDestination 
    | JsonDestination 
    | CustomDestination;

/**
 * Options for configuring an input stream that will be compared to another similar stream
 * in order to obtain the changes between those two sources.
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
 * Options for configuring the output destination of the changes emitted by the Differ object
 */
 export interface OutputOptions {
    destination:  'console' | 'null' | Filename | DestinationOptions;
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
      * Specifies a dictionary of key/value pairs that can provide custom metadata to the generated file.
      */
    labels?: Record<string, string>;
}

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

function createFormatReader(options: SourceOptions): FormatReader {
    const unknownFormat: any = options.format;
    if (options.format === 'csv') {
        return new CsvFormatReader(options);
    } 
    if (options.format === 'tsv') {
        return new CsvFormatReader({
            ...options,
            delimiter: '\t',
        });
    } 
    if (options.format === 'json') {
        return new JsonFormatReader(options);
    }
    if (options.format === 'iterable') {
        return new IterableFormatReader(options);
    }
    if (options.format === 'custom') {
        return options.reader;
    }
    throw new Error(`Unknown source format '${unknownFormat}'`);
}

function createSource(value: Filename | SourceOptions): FormatReader {
    if (typeof value === 'string' || value instanceof URL) {
        return createFormatReader({ format: 'csv', stream: value });
    }
    return createFormatReader(value);
}

function createFormatWriter(options:  'console' | 'null' | Filename | DestinationOptions): FormatWriter {
    if (options === 'console') {
        return new CsvFormatWriter({ stream: 'console' });
    }
    if (options === 'null') {
        return new NullFormatWriter();
    }
    if (typeof options === 'string' || options instanceof URL) {
        return new CsvFormatWriter({ stream: options });
    }
    const unknownFormat: any = options.format;
    if (options.format === 'csv') {
        return new CsvFormatWriter(options);
    }
    if (options.format === 'tsv') {
        return new CsvFormatWriter({
            ...options,
            delimiter: '\t',
        });
    }
    if (options.format === 'json') {
        return new JsonFormatWriter(options);
    }
    if (options.format === 'custom') {
        return options.writer;
    }
    throw new Error(`Unknown destination format '${unknownFormat}'`);
}

function createOutput(value: 'console' | 'null' | Filename | OutputOptions): { 
    format: FormatWriter, 
    filter?: RowDiffFilter,
    keepSameRows?: boolean, 
    changeLimit?: number,
    labels?: Record<string, string>;
} {
    if (value === 'console') {
        return { format: new CsvFormatWriter({ stream: new ConsoleOutputStream() }) };
    }
    if (value === 'null') {
        return { format: new NullFormatWriter() };
    }
    if (typeof value === 'string' || value instanceof URL) {
        return { format: new CsvFormatWriter({ stream: new FileOutputStream(value) }) };
    }
    return { 
        format: createFormatWriter(value.destination), 
        filter: value.filter, 
        keepSameRows: value.keepSameRows, 
        changeLimit: value.changeLimit,
        labels: value.labels,
    };
}

export class Differ {
    
    constructor(private options: DifferOptions) {
    }

    async start(): Promise<DifferContext> {
        const ctx = new DifferContext(this.options);
        await ctx[OpenSymbol]();
        return ctx;
    }

    /**
     * Iterates over the changes and sends them to the submitted output.
     * @param options a standard ouput such as console or null, a string filename, a URL or a custom OutputOptions.
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
    async to(options: 'console' | 'null' | Filename | OutputOptions): Promise<DiffStats> {
        const ctx = await this.start();
        return ctx.to(options);
    }    
}

const OpenSymbol = Symbol('open');

export class DifferContext {
    private _stats = new DiffStats();
    private _columnNames: string[] = [];
    private _isOpen = false;
    private _isClosed = false;
    private oldSource: FormatReader;
    private newSource: FormatReader;
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
     * Opens the input streams (old and new) and reads the headers.
     * This is an internal method that will be automatically called by "Differ.start" method.
     */
    async [OpenSymbol](): Promise<void> {
        if (!this._isOpen) {
            this._isOpen = true;
            await this.oldSource.open();
            await this.newSource.open();
            await this.extractHeaders();
        }
    }

    /**
     * Closes the input streams.
     * This will be automatically called by the "diffs" or "to" methods.
     * This does nothing if the streams are not open.
     */
    close(): void {
        if (this._isOpen) {
            this.newSource.close();
            this.oldSource.close();
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
     * @param options a standard ouput such as console or null, a string filename, A URL or a custom OutputOptions.
     * @returns the change stats once all the changes have been processed. 
     * Note that the stats might be different from "DiffContext.stats" when there is a filter in the output options, 
     * as the context stats are updated by the iterator which doesn't have any filter.
     * @throws {UnorderedStreamsError}
     * @throws {UniqueKeyViolationError}
     * @example
     * import { diff } from 'tabular-data-differ';
     * const stats = diff({
     *   oldSource: './tests/a.csv',
     *   newSource: './tests/b.csv',
     *   keyFields: ['id'],
     * }).to('console');
     * console.log(stats);
     */
     async to(options: 'console' | 'null' | Filename | OutputOptions): Promise<DiffStats> {
        const stats = new DiffStats();
        const output = createOutput(options);
        await output.format.open();
        try {
            await output.format.writeHeader({
                columns: this.columns,
                labels: output.labels,
            });
            for await (const rowDiff of this.diffs()) {
                let isValidDiff = output.filter?.(rowDiff) ?? true;
                if (isValidDiff) {
                    stats.add(rowDiff);
                }
                let canWriteDiff = output.keepSameRows === true || rowDiff.status !== 'same';
                if (isValidDiff && canWriteDiff) { 
                    await output.format.writeDiff(rowDiff);
                }
                if (typeof output.changeLimit === 'number' && stats.totalChanges >= output.changeLimit) {
                    break;
                }
            }    
            await output.format.writeFooter({ stats: stats });
        } finally {
            await output.format.close();
        }
        return stats;
    }

    /**
     * Enumerates the differences between two input streams (old and new).
     * @yields {RowDiff}
     * @throws {UnorderedStreamsError}
     * @throws {UniqueKeyViolationError}
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
    async *diffs() {
        if (this._isClosed) {
            throw new Error('Cannot get diffs on closed streams. You should call "Differ.start()" again.');
        }
        try {
            let pairProvider: RowPairProvider = () => this.getNextPair();
            let previousPair: RowPair = {}
            let previousRowDiff: RowDiff | undefined;
            while (true) {
                const pair = await pairProvider();

                if (pair.oldRow === undefined && pair.newRow === undefined) {
                    break;
                }

                const rowDiff = this.evalPair(pair);
                if (!sameRowDiff(this.comparer, this._columns, rowDiff, previousRowDiff)) {
                    this.ensurePairsAreInAscendingOrder(previousPair, pair);
                    this.stats.add(rowDiff);
                    yield rowDiff;        
                }

                if (rowDiff.delta === 0) {
                    pairProvider = () => this.getNextPair();
                } else if (rowDiff.delta > 0) {
                    pairProvider = async () => ({ oldRow: pair.oldRow, newRow: await this.getNextNewRow() });
                } else {
                    pairProvider = async () => ({ oldRow: await this.getNextOldRow(), newRow: pair.newRow });
                }
                previousPair = pair;
                previousRowDiff = rowDiff;
            }
        } finally {
            this.close();
        }
    }

    private async extractHeaders(): Promise<void> {
        const oldHeader = await this.oldSource.readHeader();
        const newHeader = await this.newSource.readHeader();
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

    private normalizeColumns(oldColumns: string[], newColumns: string[]) {
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

    private getNextOldRow(): Promise<Row | undefined> {
        return this.oldSource.readRow();
    }

    private getNextNewRow(): Promise<Row | undefined> {
        return this.newSource.readRow();
    }

    private async getNextPair():Promise<RowPair> {
        const oldRow = await this.getNextOldRow();
        const newRow = await this.getNextNewRow();
        return { oldRow, newRow };
    }

    private evalPair(pair: RowPair): RowDiff {
        const delta = this.comparer(this.keys, pair.oldRow, pair.newRow);
        const newRow = this.normalizeNewRow(pair.newRow);
        const oldRow = this.normalizeOldRow(pair.oldRow);
        if (delta === 0) {
            const areSame =  this.columnsWithoutKeys.length === 0 || 
                             this.comparer(this.columnsWithoutKeys, pair.oldRow, pair.newRow) === 0;
            return { delta, status: areSame ? 'same' : 'modified', oldRow, newRow };
        } else if (delta < 0) {
            return { delta, status: 'deleted', oldRow };
        }
        return { delta, status: 'added', newRow };        
    }

    private ensureRowsAreInAscendingOrder(source: string, previous?: Row, current?: Row) {
        if (previous && current && previous !== current) {
            const oldDelta = this.comparer(this.keys, previous, current);
            if (oldDelta === 0) {
                const cols = this.keys.map(key => key.name);
                throw new UniqueKeyViolationError(`Expected rows to be unique by "${cols}" in ${source} source but received:\n  previous=${previous}\n  current=${current}`);
            }
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

export function sameRowDiff(comparer: RowComparer, columns: Column[], current?: RowDiff, previous?: RowDiff) {
    if (current === previous) {
        return true;
    }
    if (current === undefined || previous === undefined) {
        return false;
    }
    if (current.status === 'added') {
        return comparer(columns, current.newRow, previous.newRow) === 0; 
    }
    if (current.status === 'deleted') {
        return comparer(columns, current.oldRow, previous.oldRow) === 0; 
    }
    return comparer(columns, current.newRow, previous.newRow) === 0 && 
           comparer(columns, current.oldRow, previous.oldRow) === 0;
}
