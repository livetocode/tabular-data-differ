import { getOrCreateInputStream, getOrCreateOutputStream, InputStream, InputStreamOptions, OutputStream, OutputStreamOptions, TextReader, TextWriter } from "./streams";

export const defaultStatusColumnName = 'DIFF_STATUS';

export type CellValue = string | number | boolean | null;

export type SortDirection = 'ASC' | 'DESC';

export type ColumnComparer = (a: CellValue, b: CellValue) => number;

export type Column = {
    name: string;
    oldIndex: number;
    newIndex: number;
    comparer?: ColumnComparer;
    sortDirection?: SortDirection;
}

export type Row = CellValue[];

export type RowComparer = (keys: Column[], a? : Row, b?: Row) => number;

export type RowNormalizer = (row?: Row) => Row | undefined;

export type RowFilter = (row: Row) => boolean;

export type RowDiffStatus = 'same' | 'added' | 'modified' | 'deleted';

export interface RowDiff {
    delta: number;
    status: RowDiffStatus;
    oldRow?: Row;
    newRow?: Row;
}

export type RowDiffFilter = (rowDiff: RowDiff) => boolean;

/**
 * Shared option for some destination formats such as CSV or JSON.
 */
export type KeepOldValuesOptions = {
    /**
     * Specifies if the output should contain both the old and new values for each row.
     */
     keepOldValues?: boolean;
};

export type CsvFormatReaderOptions = {
    delimiter?: string;
} & InputStreamOptions;

export type CsvFormatWriterOptions = {
    delimiter?: string;
    statusColumnName?: string;
} & OutputStreamOptions & KeepOldValuesOptions;

export type JsonFormatReaderOptions = {
} & InputStreamOptions;

export type JsonFormatWriterOptions = {
} & OutputStreamOptions & KeepOldValuesOptions;

export type IterableFormatReaderOptions = {
    provider: () => AsyncIterable<any>;
};

export interface FormatHeader {
    columns: string[];
    labels?: Record<string, string>;
}

export interface FormatReader {
    open(): Promise<void>;
    readHeader(): Promise<FormatHeader>;
    readRow(): Promise<Row | undefined>;
    close(): Promise<void>;
}

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

export interface FormatWriterOptions {
    stream: OutputStream;
    delimiter?: string;
    keepOldValues?: boolean;
    statusColumnName?: string;
}

export interface FormatFooter {
    stats: DiffStats;
}

export interface FormatWriter {
    open(): Promise<void>;
    writeHeader(header: FormatHeader): Promise<void>;
    writeDiff(rowDiff: RowDiff): Promise<void>;
    writeFooter(footer: FormatFooter): Promise<void>;
    close(): Promise<void>;
}

export type FormatWriterFactory = (options: FormatWriterOptions) => FormatWriter;

export class BufferedFormatReader implements FormatReader {
    private peekedRow: Row | undefined;
    private hasPeekedRow = false;

    constructor(private reader: FormatReader) {}

    open(): Promise<void> {
        this.hasPeekedRow = false;
        this.peekedRow = undefined;
        return this.reader.open();
    }

    readHeader(): Promise<FormatHeader> {
        return this.reader.readHeader();
    }
    
    async peekRow(): Promise<Row | undefined> {
        if (this.hasPeekedRow) { 
            return this.peekedRow;
        }
        this.peekedRow = await this.reader.readRow();
        this.hasPeekedRow = true;
        return this.peekedRow;
    }

    async readRow(): Promise<Row | undefined> {
        if (this.hasPeekedRow) {
            const result = this.peekedRow;
            this.peekedRow = undefined;
            this.hasPeekedRow = false;
            return result;
        }
        return await this.reader.readRow();
    }

    close(): Promise<void> {
        return this.reader.close();
    }
}

export abstract class StreamFormatReader implements FormatReader {
    protected readonly stream: InputStream;
    protected readonly encoding?: BufferEncoding;

    constructor(options: InputStreamOptions) {
        this.stream =  getOrCreateInputStream(options.stream);
        this.encoding = options.encoding;
    }

    async open(): Promise<void> {
        await this.stream.open();
    }

    abstract readHeader(): Promise<FormatHeader>;

    abstract readRow(): Promise<Row | undefined>;

    async close(): Promise<void> {
        await this.stream.close();        
    }

}

export abstract class TextFormatReader extends StreamFormatReader {
    private _textReader?: TextReader;

    protected get textReader() {
        if (!this._textReader) {
            throw new Error('Cannot access textReader because stream is not open');
        }
        return this._textReader;
    }
    
    async open(): Promise<void> {
        await super.open();
        this._textReader = this.stream.createTextReader({ encoding: this.encoding });
    }

    async close(): Promise<void> {
        if (this.textReader) {
            await this.textReader.close();
            this._textReader = undefined;
        }
        await super.close();
    }
}

export abstract class StreamFormatWriter implements FormatWriter {
    protected readonly stream: OutputStream;
    protected readonly encoding?: BufferEncoding;

    constructor(options: OutputStreamOptions) {
        this.stream =  getOrCreateOutputStream(options.stream);
        this.encoding = options.encoding;
    }

    async open(): Promise<void> {
        await this.stream.open();
    }


    async close(): Promise<void> {
        await this.stream.close();        
    }

    abstract writeHeader(header: FormatHeader): Promise<void>;

    abstract writeDiff(rowDiff: RowDiff): Promise<void>;

    abstract writeFooter(footer: FormatFooter): Promise<void>;
}

export abstract class TextFormatWriter extends StreamFormatWriter {
    private _textWriter?: TextWriter;

    protected get textWriter() {
        if (!this._textWriter) {
            throw new Error('Cannot access textWriter because stream is not open');
        }
        return this._textWriter;
    }
    
    async open(): Promise<void> {
        await super.open();
        this._textWriter = this.stream.createTextWriter({ encoding: this.encoding });
    }

    async close(): Promise<void> {
        if (this.textWriter) {
            await this.textWriter.close();
            this._textWriter = undefined;
        }
        await super.close();
    }
}

export class CsvFormatReader extends TextFormatReader {
    private readonly delimiter: string;

    constructor(options: CsvFormatReaderOptions) {
        super(options);
        this.delimiter = options.delimiter ?? ',';
    }

    async readHeader(): Promise<FormatHeader> {
        return {
            columns: parseCsvLine(this.delimiter, await this.textReader.readLine()) ?? [],
        };
    }

    async readRow(): Promise<Row | undefined> {
        return parseCsvLine(this.delimiter, await this.textReader.readLine());
    }
}

export class JsonFormatReader extends TextFormatReader {
    private headerObj: any;
    private columns: string[] = [];

    constructor(options: JsonFormatReaderOptions) {
        super(options);
    }

    async open(): Promise<void> {
        this.headerObj = null;
        this.columns = [];
        await super.open();
    }

    async readHeader(): Promise<FormatHeader> {
        let line = await this.textReader.readLine();
        this.headerObj = parseJsonObj(line);
        if (!this.headerObj) {
            // if the obj is undefined, it might mean that we just started an array with a single line containing '['
            // so, process the next line
            line = await this.textReader.readLine();
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

    async readRow(): Promise<Row | undefined> {
        if (this.headerObj) {
            const row = convertJsonObjToRow(this.headerObj, this.columns);
            this.headerObj = null;
            return row;
        }
        const line = await this.textReader.readLine();
        const obj = parseJsonObj(line);
        const row = convertJsonObjToRow(obj, this.columns);

        return row;
    }
}

export class IterableFormatReader implements FormatReader {
    private headerObj: any;
    private columns: string[] = [];
    private iterable: () => AsyncIterable<any>;
    private iterator?: AsyncIterator<any>;

    constructor(options: IterableFormatReaderOptions) {
        this.iterable = options.provider;
    }

    open(): Promise<void> {
        if (this.iterator) {
            throw new Error('Reader is already open!');
        }
        this.headerObj = null;
        this.columns = [];
        this.iterator = this.iterable()[Symbol.asyncIterator]();
        return Promise.resolve();
    }

    async readHeader(): Promise<FormatHeader> {
        if (!this.headerObj) {
            this.headerObj = await this.nextItem();
        }
        if (!this.headerObj) {
            throw new Error('Expected to find at least one object');
        }
        this.columns = Object.keys(this.headerObj);
        return {
            columns: this.columns,
        };
    }

    async readRow(): Promise<Row | undefined> {
        if (this.headerObj) {
            const row = convertJsonObjToRow(this.headerObj, this.columns);
            this.headerObj = null;
            return row;
        }
        const obj = await this.nextItem();
        const row = convertJsonObjToRow(obj, this.columns);
        return row;
    }

    close(): Promise<void> {
        if (this.iterator) {
            if (this.iterator.return) {
                this.iterator.return();
            }
            this.iterator = undefined;
        }
        return Promise.resolve();
    }

    private async nextItem(): Promise<any> {
        if (!this.iterator) {
            throw new Error('You must call open before reading content!');
        }
        const res = await this.iterator.next();
        if (res.done) {
            this.iterator = undefined;
            return undefined;
        }
        return res.value;
    }
}

export class CsvFormatWriter extends TextFormatWriter {
    private readonly delimiter: string;
    private readonly keepOldValues: boolean;
    private readonly statusColumnName: string;

    constructor(options: CsvFormatWriterOptions) {
        super(options);
        this.delimiter = options.delimiter ?? ',';
        this.keepOldValues = options.keepOldValues ?? false;
        this.statusColumnName = options.statusColumnName ?? defaultStatusColumnName;
    }

    writeHeader(header: FormatHeader): Promise<void> {
        const columns = [this.statusColumnName, ...header.columns];
        if (this.keepOldValues) {
            columns.push(...header.columns.map(col => 'OLD_' + col));
        }
        return this.textWriter.writeLine(serializeRowAsCsvLine(columns, this.delimiter));
    }

    async writeDiff(rowDiff: RowDiff): Promise<void> {
        if (rowDiff.oldRow && rowDiff.newRow) {
            const row = [rowDiff.status, ...rowDiff.newRow];
            if (this.keepOldValues) {
                row.push(...rowDiff.oldRow);
            }
            await this.textWriter.writeLine(serializeRowAsCsvLine(row, this.delimiter));
        } else if (rowDiff.oldRow) {
            if (this.keepOldValues) {
                const emptyRow = rowDiff.oldRow.map(_ => '');
                await this.textWriter.writeLine(serializeRowAsCsvLine([rowDiff.status, ...emptyRow, ...rowDiff.oldRow], this.delimiter));
            } else {
                await this.textWriter.writeLine(serializeRowAsCsvLine([rowDiff.status, ...rowDiff.oldRow], this.delimiter));

            }
        } else if (rowDiff.newRow) {
            const row = [rowDiff.status, ...rowDiff.newRow];
            if (this.keepOldValues) {
                const emptyRow = rowDiff.newRow.map(_ => '');
                row.push(...emptyRow);
            }
            await this.textWriter.writeLine(serializeRowAsCsvLine(row, this.delimiter));
        }
    }

    writeFooter(footer: FormatFooter): Promise<void> {
        return Promise.resolve();
    }
}

export class JsonFormatWriter extends TextFormatWriter {
    private readonly keepOldValues: boolean;
    private rowCount: number = 0;

    constructor(options: JsonFormatWriterOptions) {
        super(options);
        this.keepOldValues = options.keepOldValues ?? false;
    }

    writeHeader(header: FormatHeader): Promise<void> {
        this.rowCount = 0;
        const h = JSON.stringify(header);
        return this.textWriter.writeLine(`{ "header": ${h}, "items": [`);
    }

    writeDiff(rowDiff: RowDiff): Promise<void> {
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
        return this.textWriter.writeLine(separator + JSON.stringify(record));
    }

    writeFooter(footer: FormatFooter): Promise<void> {
        return this.textWriter.writeLine(`], "footer": ${JSON.stringify(footer)}}`);
    }
}

export class NullFormatWriter implements FormatWriter {
    open(): Promise<void> {
        return Promise.resolve();
    }

    writeHeader(header: FormatHeader): Promise<void> {
        return Promise.resolve();
    }

    writeDiff(rowDiff: RowDiff): Promise<void> {
        return Promise.resolve();
    }

    writeFooter(footer: FormatFooter): Promise<void> {
        return Promise.resolve();
    }

    close(): Promise<void> {
        return Promise.resolve();
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
    const row = columns.map(col => {
        const val = obj[col];
        if (val === null || typeof val === 'number' || typeof val === 'boolean') {
            return val;
        }
        return `${val}`;
    });
    return row;
}

export function parseCsvLine(delimiter: string, line?: string): string[] | undefined {
    if (line) {
        const row: string[] = [];
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
export function serializeCsvField(value : CellValue): string {
    if (value === null) {
        return '';
    }
    if (typeof value === 'string' && charsToEncodeRegEx.test(value)) {
        return `"${value.replaceAll('"', '""')}"`;
    }
    return value.toString();
}

export function serializeRowAsCsvLine(row : Row, delimiter?: string) {
    return row.map(serializeCsvField).join(delimiter ?? ',');
}

export function stringComparer(a: CellValue, b: CellValue): number {
    // We can't use localeCompare since the ordered csv file produced by SQLite won't use the same locale
    // return a.localeCompare(b)
    const aa = a === null ? '' : a.toString();
    const bb = b === null ? '' : b.toString();
    if (aa === bb) {
        return 0;
    } else if (aa < bb) {
        return -1;
    } 
    return 1;
}

export function numberComparer(a: CellValue, b: CellValue): number {
    if (a === b) {
        return 0;
    }
    if (a === null && b !== null) {
        return -1;
    }
    if (a !== null && b === null) {
        return 1;
    }
    if (typeof a === 'number' && typeof b === 'number') {
        return a < b ? -1 : 1;
    }
    if (typeof a === 'boolean' && typeof b === 'boolean') {
        return a < b ? -1 : 1;
    }
    const strA = a!.toString();
    const strB = b!.toString();
    if (strA === strB) {
        return 0;
    }
    if (strA === '' && strB !== '') {
        return -1;
    }
    if (strA !== '' && strB === '') {
        return 1;
    }
    const aa = parseFloat(strA);
    const bb = parseFloat(strB);
    if (Number.isNaN(aa) && !Number.isNaN(bb)) {
        return -1;
    }
    if (!Number.isNaN(aa) && Number.isNaN(bb)) {
        return 1;
    }
    if (aa < bb) {
        return -1;
    }
    return 1;
}

export function cellComparer(a: CellValue, b: CellValue): number {
    if (typeof a === 'number' && typeof b === 'number') {
        return numberComparer(a, b);
    }
    return stringComparer(a, b);
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
        const aa = a![col.oldIndex] ?? null;
        const bb = b![col.newIndex] ?? null;
        const comparer = col.comparer ?? cellComparer;
        let delta = comparer(aa, bb);
        if (delta !== 0 && col.sortDirection === 'DESC') {
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


