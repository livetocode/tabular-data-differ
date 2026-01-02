import * as fs from 'fs';
import { ArrayInputStream, ConsoleOutputStream, FileInputStream, FileOutputStream, NullOutputStream } from "./streams";

describe('streams', () => {
    beforeAll(() => {
        fs.mkdirSync('./output/files', { recursive: true });
    });
    test('should read/write to a file', async () => {
        const f = new FileOutputStream('./output/files/test.txt');
        await f.open();
        const writer = f.createTextWriter();
        try {
            await writer.writeLine('Hello world!');
        } finally {
            await writer.close();
            await f.close();
        }
        const f2 = new FileInputStream('./output/files/test.txt');
        await f2.open();
        const reader = f2.createTextReader();
        try {
            const line = await reader.readLine();
            expect(line).toBe('Hello world!');
        } finally {
            await reader.close();
            await f2.close();
        }
    });
    test('should open input file only once', async () => {
        const f = new FileInputStream('./tests/a.csv');
        await f.open();
        try {
            await expect(async () => {
                await f.open();
            }).rejects.toThrow('file "./tests/a.csv" is already open');
        } finally {
            f.close();
        }        
    });
    test('should open output file only once', async () => {
        const f = new FileOutputStream('./output/files/test.txt');
        await f.open();
        try {
            await expect(async () => {
                await f.open();
            }).rejects.toThrow('file "./output/files/test.txt" is already open');
        } finally {
            f.close();
        }        
    });
    test('closing output file twice should work', async () => {
        const f = new FileOutputStream('./output/files/test.txt');
        await f.open();
        await f.close();
        await f.close();        
    });
    test('should fail to open output file in wrong directory', async () => {
        const f = new FileOutputStream('./output/wrong-directory/test.txt');
        await expect(async () => {
            await f.open();
        }).rejects.toThrow(`ENOENT: no such file or directory, open './output/wrong-directory/test.txt'`);
    });
    test('should fail to write to output file when not open', async () => {
        const f = new FileOutputStream('./output/files/test.txt');
        await expect(async () => {
            await f.write({ buffer: Buffer.from('Hello') });
        }).rejects.toThrow(`file \"./output/files/test.txt\" is not open!`);
    });
    test('should fail to write to output file when closed', async () => {
        const f = new FileOutputStream('./output/files/test.txt');
        await f.open();
        await f.close();
        await expect(async () => {
            await f.write({ buffer: Buffer.from('Hello') });
        }).rejects.toThrow(`file \"./output/files/test.txt\" is not open!`);
    });
    test('should fail to create text writer file when closed', async () => {
        const f = new FileOutputStream('./output/files/test.txt');
        await f.open();
        await f.close();
        expect(() => {
            f.createTextWriter();
        }).toThrow(`file \"./output/files/test.txt\" is not open!`);
    });
    test('should fail to read from input file when closed', async () => {
        const f = new FileInputStream('./tests/a.csv');
        await f.open();
        await f.close();
        await expect(async () => {
            await f.read({ length: 100 });
        }).rejects.toThrow(`file \"./tests/a.csv\" is not open!`);
    });
    test('should fail to create text reader file when closed', async () => {
        const f = new FileInputStream('./tests/a.csv');
        await f.open();
        await f.close();
        expect(() => {
            f.createTextReader();
        }).toThrow(`file \"./tests/a.csv\" is not open!`);
    });
    test('should fail to read line when the whole stream has been read', async () => {
        const f = new FileInputStream('./tests/a.csv');
        await f.open();
        const reader = f.createTextReader();
        while (await reader.readLine() !== undefined) {
        }
        await expect(async () => {
            await reader.readLine();
        }).rejects.toThrow(`FileTextReader is closed`);
        await f.close();
    });
    test('should read a buffer from a file', async () => {
        const f = new FileInputStream('./tests/a.csv');
        await f.open();
        const buffer = Buffer.alloc(8);
        const res = await f.read({ buffer });
        expect(res.buffer).toBe(buffer);
        expect(res.bytesRead).toBe(8);
        expect(buffer.toString()).toBe('id,a,b,c');
        await f.close();
    });
    test('should not read from ArrayInputStream', async () => {
        const f = new ArrayInputStream([])        
        await f.open();
        await expect(async () => {
            await f.read({});
        }).rejects.toThrow(`Method not implemented.`);
    });
    test('should write to a null stream', async () => {
        const f = new NullOutputStream();
        await f.open();
        const buffer = Buffer.from('Hello');
        const res = await f.write({ buffer });
        expect(res.buffer).toBe(buffer);
        expect(res.bytesWritten).toBe(buffer.length);
        const res2 = await f.write({ buffer, length: 2 });
        expect(res2.buffer).toBe(buffer);
        expect(res2.bytesWritten).toBe(2);
        const res3 = await f.write({ buffer, offset: 2 });
        expect(res3.buffer).toBe(buffer);
        expect(res3.bytesWritten).toBe(buffer.length - 2);
        const res4 = await f.write({ buffer, offset: 2, length: 2 });
        expect(res4.buffer).toBe(buffer);
        expect(res4.bytesWritten).toBe(2);
    });
    test('should not write a buffer to the Console', async () => {
        const f = new ConsoleOutputStream();
        await f.open();
        const buffer = Buffer.from('Hello');
        await expect(async () => {
            await f.write({ buffer });
        }).rejects.toThrow(`Cannot write to the console. Use a TextWriter instead.`);        
    });
    test('should write a buffer to a stream', async () => {
        const f = new FileOutputStream('./output/files/test.bin');
        await f.open();
        const buffer = Buffer.from('Hello');
        const res = await f.write({ buffer });
        await f.close();
        expect(res.buffer).toBe(buffer);
        expect(res.bytesWritten).toBe(5);

        const f2 = new FileInputStream('./output/files/test.bin');
        await f2.open();
        const buffer2 = Buffer.alloc(buffer.length);
        const res2 = await f2.read({ buffer: buffer2 });
        await f2.close();
        expect(res2.buffer).toBe(buffer2);
        expect(res2.bytesRead).toBe(5);
        expect(buffer2.toString()).toBe(buffer.toString());
    });
});