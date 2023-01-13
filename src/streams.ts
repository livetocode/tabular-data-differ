import fs from 'fs/promises';
import lineByLine from 'n-readlines';

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
    private liner?: lineByLine;

    constructor(private readonly filename: string) {
    }

    open(): Promise<void> {
        this.liner = new lineByLine(this.filename);
        return Promise.resolve();
    }

    async readLine(): Promise<string | undefined> {
        if (!this.liner) {
            throw new Error(`FileInputStream "${this.filename}" is not open`);
        }
        while(true) {
            const result = this.liner.next();
            if(result === false) {
                return Promise.resolve(undefined);
            }
            const line = result.toString().trim();
            // ignore empty lines
            if (line.length > 0) {
                return Promise.resolve(line);
            }
        }
    }

    close(): Promise<void> {
        if (this.liner) {
            if ((this.liner as any)['fd'] !== null) {
                this.liner.close();
            }
            this.liner = undefined;
        }
        return Promise.resolve();
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
    private fd?: fs.FileHandle;

    constructor(public readonly path: string) {
    }
    
    async open(): Promise<void> {
        if (this.fd !== undefined) {
            throw new Error(`file "${this.path}" is already open`);
        }
        this.fd = await fs.open(this.path, 'w');
    }

    async writeLine(line: string): Promise<void> {
        if (this.fd === undefined) {
            throw new Error(`file "${this.path}" is not open!`);
        }
        await this.fd.write(Buffer.from(line + '\n'));
    }

    async close(): Promise<void> {
        if (this.fd !== undefined) {
            await this.fd.close();
            this.fd = undefined;
        }
    }
}
