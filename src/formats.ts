import { InputStream, OutputStream } from "./streams";

export type ColumnOrdering = 'ASC' | 'DESC';

export type ColumnComparer = (a: string, b: string) => number;

export type Column = {
    name: string;
    oldIndex: number;
    newIndex: number;
    comparer?: ColumnComparer;
    order?: ColumnOrdering;
}

export type Row = string[];

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

export interface FormatReaderOptions {
    stream: InputStream;
    delimiter?: string;
}

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

export type FormatReaderFactory = (options: FormatReaderOptions) => FormatReader;

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



export class CsvFormatReader implements FormatReader {
    private readonly stream: InputStream;
    private readonly delimiter: string;

    constructor(options: FormatReaderOptions) {
        this.stream = options.stream;
        this.delimiter = options.delimiter ?? ',';
    }

    open(): Promise<void> {
        return this.stream.open();
    }

    async readHeader(): Promise<FormatHeader> {
        return {
            columns: parseCsvLine(this.delimiter, await this.stream.readLine()) ?? [],
        };
    }

    async readRow(): Promise<Row | undefined> {
        return parseCsvLine(this.delimiter, await this.stream.readLine());
    }

    close(): Promise<void> {
        return this.stream.close();        
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

export class JsonFormatReader implements FormatReader {
    private readonly stream: InputStream;
    private headerObj: any;
    private columns: string[] = [];

    constructor(options: FormatReaderOptions) {
        this.stream = options.stream;
    }

    open(): Promise<void> {
        this.headerObj = null;
        this.columns = [];
        return this.stream.open();
    }

    async readHeader(): Promise<FormatHeader> {
        let line = await this.stream.readLine();
        this.headerObj = parseJsonObj(line);
        if (!this.headerObj) {
            // if the obj is undefined, it might mean that we just started an array with a single line containing '['
            // so, process the next line
            line = await this.stream.readLine();
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
        const line = await this.stream.readLine();
        const obj = parseJsonObj(line);
        const row = convertJsonObjToRow(obj, this.columns);

        return row;
    }

    close(): Promise<void> {
        return this.stream.close();        
    }
}

const defaultStatusColumnName = 'DIFF_STATUS';

export class CsvFormatWriter implements FormatWriter{
    private readonly stream: OutputStream;
    private readonly delimiter: string;
    private readonly keepOldValues: boolean;
    private readonly statusColumnName: string;

    constructor(options: FormatWriterOptions) {
        this.stream = options.stream;
        this.delimiter = options.delimiter ?? ',';
        this.keepOldValues = options.keepOldValues ?? false;
        this.statusColumnName = options.statusColumnName ?? defaultStatusColumnName;
    }

    open(): Promise<void> {
        return this.stream.open();        
    }

    writeHeader(header: FormatHeader): Promise<void> {
        const columns = [this.statusColumnName, ...header.columns];
        if (this.keepOldValues) {
            columns.push(...header.columns.map(col => 'OLD_' + col));
        }
        return this.stream.writeLine(serializeRowAsCsvLine(columns, this.delimiter));
    }

    async writeDiff(rowDiff: RowDiff): Promise<void> {
        if (rowDiff.oldRow && rowDiff.newRow) {
            const row = [rowDiff.status, ...rowDiff.newRow];
            if (this.keepOldValues) {
                row.push(...rowDiff.oldRow);
            }
            await this.stream.writeLine(serializeRowAsCsvLine(row, this.delimiter));
        } else if (rowDiff.oldRow) {
            if (this.keepOldValues) {
                const emptyRow = rowDiff.oldRow.map(_ => '');
                await this.stream.writeLine(serializeRowAsCsvLine([rowDiff.status, ...emptyRow, ...rowDiff.oldRow], this.delimiter));
            } else {
                await this.stream.writeLine(serializeRowAsCsvLine([rowDiff.status, ...rowDiff.oldRow], this.delimiter));

            }
        } else if (rowDiff.newRow) {
            const row = [rowDiff.status, ...rowDiff.newRow];
            if (this.keepOldValues) {
                const emptyRow = rowDiff.newRow.map(_ => '');
                row.push(...emptyRow);
            }
            await this.stream.writeLine(serializeRowAsCsvLine(row, this.delimiter));
        }
    }

    writeFooter(footer: FormatFooter): Promise<void> {
        return Promise.resolve();
    }

    close(): Promise<void> {
        return this.stream.close();
    }
}

export class JsonFormatWriter implements FormatWriter{
    private readonly stream: OutputStream;
    private readonly keepOldValues: boolean;
    private rowCount: number = 0;

    constructor(options: FormatWriterOptions) {
        this.stream = options.stream;
        this.keepOldValues = options.keepOldValues ?? false;
    }

    open(): Promise<void> {
        return this.stream.open();    
    }

    writeHeader(header: FormatHeader): Promise<void> {
        this.rowCount = 0;
        const h = JSON.stringify(header);
        return this.stream.writeLine(`{ "header": ${h}, "items": [`);
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
        return this.stream.writeLine(separator + JSON.stringify(record));
    }

    writeFooter(footer: FormatFooter): Promise<void> {
        return this.stream.writeLine(`], "footer": ${JSON.stringify(footer)}}`);
    }

    close(): Promise<void> {
        return this.stream.close();   
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
