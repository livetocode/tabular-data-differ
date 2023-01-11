import fs from 'fs';
import lineByLine from 'n-readlines';

//TODO: make stream operations async
export interface InputStream {
    open(): void;
    readLine(): string | undefined;
    close(): void;
}

export interface OutputStream {
    open(): void;
    writeLine(line: string): void;
    close(): void;
}

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
