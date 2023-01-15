import { PathLike } from 'fs';
import fs from 'fs/promises';

export interface InputStream {
    open(): Promise<void>;
    readLine(): Promise<string | undefined>;
    close(): Promise<void>;
}

export interface OutputStream {
    open(): Promise<void>;
    writeLine(line: string): Promise<void>;
    close(): Promise<void>;
}

export class FileInputStream implements InputStream {
    private file?: fs.FileHandle;
    private iterator?: AsyncIterableIterator<string>;

    constructor(private readonly path: PathLike) {
    }

    async open(): Promise<void> {
        if (this.file !== undefined) {
            throw new Error(`file "${this.path}" is already open`);
        }
        this.file = await fs.open(this.path, 'r');
        this.iterator = this.file.readLines()[Symbol.asyncIterator]();
    }

    async readLine(): Promise<string | undefined> {
        if (!this.iterator) {
            throw new Error(`FileInputStream "${this.path}" is not open`);
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
        if (this.iterator && this.iterator.return) {
            await this.iterator.return();
            this.iterator = undefined;
        }
        if (this.file !== undefined) {            
            await this.file.close();
            this.file = undefined;
        }
    }
}

export class ArrayInputStream implements InputStream {
    private currentIndex = 0;

    constructor(private lines: string[]) {}

    open(): Promise<void> {
        return Promise.resolve();
    }

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

export class NullOutputStream implements OutputStream {
    open(): Promise<void> {
        return Promise.resolve();
    }

    writeLine(line: string): Promise<void> {
        return Promise.resolve();
    }

    close(): Promise<void> {
        return Promise.resolve();
    }
}

export class ConsoleOutputStream implements OutputStream {
    open(): Promise<void> {
        return Promise.resolve();
    }

    writeLine(line: string): Promise<void> {
        console.log(line);
        return Promise.resolve();
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
            throw new Error(`file "${this.path}" is already open`);
        }
        this.file = await fs.open(this.path, 'w');
    }

    async writeLine(line: string): Promise<void> {
        if (this.file === undefined) {
            throw new Error(`file "${this.path}" is not open!`);
        }
        await this.file.write(Buffer.from(line + '\n'));
    }

    async close(): Promise<void> {
        if (this.file !== undefined) {
            await this.file.close();
            this.file = undefined;
        }
    }
}
