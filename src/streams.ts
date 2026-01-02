import { PathLike } from 'fs';
import fs from 'fs/promises';

/** 
 * Either a string containing a filename or a URL
 */
export type Filename = string | URL;

export interface StreamReadResult {
    bytesRead: number;
    buffer: Buffer;
}
export interface StreamReadOptions {
    /**
     * @default `Buffer.alloc(0xffff)`
     */
    buffer?: Buffer;
    /**
     * @default 0
     */
    offset?: number | undefined;
    /**
     * @default `length of buffer`
     */
    length?: number | undefined;
}

export interface StreamWriteResult {
    bytesWritten: number;
    buffer: Buffer;
}

export interface StreamWriteOptions {
    buffer: Buffer;
    /**
     * @default 0
     */
    offset?: number | undefined;
    /**
     * @default `buffer.byteLength`
     */
    length?: number | undefined;
}

interface TextReaderOptions {
    encoding?: BufferEncoding | null | undefined;
    highWaterMark?: number | undefined;
}

export interface TextReader {
    readLine(): Promise<string | undefined>;
    close(): Promise<void>;
}

export interface InputStream {
    open(): Promise<void>;
    read(options: StreamReadOptions): Promise<StreamReadResult>;
    createTextReader(options? : TextReaderOptions): TextReader;
    close(): Promise<void>;
}

interface TextWriterOptions {
    encoding?: BufferEncoding | null | undefined;
}

export interface TextWriter {
    writeLine(line: string): Promise<void>;
    close(): Promise<void>;
}

export interface OutputStream {
    open(): Promise<void>;
    write(options: StreamWriteOptions): Promise<StreamWriteResult>;
    createTextWriter(options? : TextWriterOptions): TextWriter;
    close(): Promise<void>;
}

/**
 * Options for configuring an input stream
 */
export type InputStreamOptions = {
    /**
     * Specifies a stream either by its filename or by an instance of an `InputStream`
     */
    stream: Filename | InputStream;
    /**
     * Specifies the encoding to use for text operations such as readLine
     */
    encoding?: BufferEncoding;
};

/**
 * Options for configuring an output stream
 */
export type OutputStreamOptions = {
    /**
     * Specifies a stream either by its filename or by an instance of an `OutputStream`
     */
    stream: 'console' | 'null' | Filename | OutputStream;
    /**
     * Specifies the encoding to use for text operations such as writeLine
     */
    encoding?: BufferEncoding;
};

export class FileTextReader implements TextReader {
    constructor(private iterator?: AsyncIterableIterator<string>) {
    }

    async readLine(): Promise<string | undefined> {
        if (!this.iterator) {
            throw new Error(`FileTextReader is closed`);
        }
        while(true) {
            const result = await this.iterator.next();
            if(result.done) {
                this.iterator = undefined;
                return undefined;
            }
            const line = result.value.trim();
            // ignore empty lines
            if (line.length > 0) {
                return line;
            }
        }
    }
    
    async close(): Promise<void> {
        if (this.iterator) {
            if (this.iterator.return) {
                await this.iterator.return();
            }
            this.iterator = undefined;
        }
    }
}

export class FileInputStream implements InputStream {
    private file?: fs.FileHandle;

    constructor(private readonly path: PathLike) {
    }

    async open(): Promise<void> {
        if (this.file !== undefined) {
            throw new Error(`file "${this.path}" is already open!`);
        }
        this.file = await fs.open(this.path, 'r');
    }

    read(options: StreamReadOptions): Promise<StreamReadResult> {
        if (!this.file) {
            throw new Error(`file "${this.path}" is not open!`);
        }
        return this.file.read(options);
    }

    createTextReader(options?: TextReaderOptions): TextReader {
        if (!this.file) {
            throw new Error(`file "${this.path}" is not open!`);
        }
        return new FileTextReader(this.file.readLines(options)[Symbol.asyncIterator]());
    }

    async close(): Promise<void> {
        if (this.file !== undefined) {            
            await this.file.close();
            this.file = undefined;
        }
    }

}

export class ArrayInputStream implements InputStream {

    constructor(private lines: string[]) {}

    open(): Promise<void> {
        return Promise.resolve();
    }

    createTextReader(options?: TextReaderOptions | undefined): TextReader {
        return new ArrayTextReader(this.lines);
    }

    read(options: StreamReadOptions): Promise<StreamReadResult> {
        throw new Error('Method not implemented.');
    }

    close(): Promise<void> {
        return Promise.resolve();
    }
}

export class ArrayTextReader implements TextReader {
    private currentIndex = 0;

    constructor(private lines: string[]) {}

    readLine(): Promise<string | undefined> {
        if (this.currentIndex < this.lines.length) {
            const result = this.lines[this.currentIndex];
            this.currentIndex++;
            return Promise.resolve(result);
        }
        return Promise.resolve(undefined);
    }

    close(): Promise<void> {
        return Promise.resolve();
    }
}

export class NullTextWriter implements TextWriter {
    writeLine(line: string): Promise<void> {
        return Promise.resolve();
    }
    
    close(): Promise<void> {
        return Promise.resolve();
    }
}

export class NullOutputStream implements OutputStream {
    open(): Promise<void> {
        return Promise.resolve();
    }

    createTextWriter(options?: TextWriterOptions | undefined): TextWriter {
        return new NullTextWriter();
    }

    write(options: StreamWriteOptions): Promise<StreamWriteResult> {
        const buffer = options.buffer;
        const len = options.length ?? buffer.length;
        const start = options.offset ?? 0;
        const end = start + len;
        const bytesWritten = Math.min(buffer.length, end) - start;
        return Promise.resolve({ bytesWritten, buffer });
    }

    close(): Promise<void> {
        return Promise.resolve();
    }
}

export class ConsoleTextWriter implements TextWriter {
    writeLine(line: string): Promise<void> {
        console.log(line);
        return Promise.resolve();
    }
    
    close(): Promise<void> {
        return Promise.resolve();
    }
}

export class ConsoleOutputStream implements OutputStream {
    createTextWriter(options?: TextWriterOptions | undefined): TextWriter {
        return new ConsoleTextWriter();
    }

    open(): Promise<void> {
        return Promise.resolve();
    }

    write(options: StreamWriteOptions): Promise<StreamWriteResult> {
        throw new Error('Cannot write to the console. Use a TextWriter instead.');
    }

    close(): Promise<void> {        
        return Promise.resolve();
    }
}

export class FileTextWriter implements TextWriter {
    constructor(private file: fs.FileHandle) {}

    async writeLine(line: string): Promise<void> {
        await this.file.write(Buffer.from(line + '\n'));
    }

    close(): Promise<void> {
        return Promise.resolve();
    }
}

export class FileOutputStream implements OutputStream {
    private file?: fs.FileHandle;

    constructor(public readonly path: PathLike) {
    }

    async open(): Promise<void> {
        if (this.file !== undefined) {
            throw new Error(`file "${this.path}" is already open!`);
        }
        this.file = await fs.open(this.path, 'w');
    }

    createTextWriter(options?: TextWriterOptions | undefined): TextWriter {
        if (this.file === undefined) {
            throw new Error(`file "${this.path}" is not open!`);
        }
        return new FileTextWriter(this.file);
    }
    
    async write(options: StreamWriteOptions): Promise<StreamWriteResult> {
        if (this.file === undefined) {
            throw new Error(`file "${this.path}" is not open!`);
        }
        return await this.file.write(options.buffer, options.offset, options.length);
    }

    async close(): Promise<void> {
        if (this.file !== undefined) {
            await this.file.close();
            this.file = undefined;
        }
    }
}

export function getOrCreateInputStream(stream: Filename | InputStream): InputStream {
    if (typeof stream === 'string' || stream instanceof URL) {
        return new FileInputStream(stream);
    }
    return stream;
}

export function getOrCreateOutputStream(stream: 'console' | 'null' | Filename | OutputStream): OutputStream {
    if (stream === 'console') {
        return new ConsoleOutputStream();
    } 
    if (stream === 'null') {
        return new NullOutputStream();
    }
    if (typeof stream === 'string' || stream instanceof URL) {
        return new FileOutputStream(stream);
    }
    return stream;
}
