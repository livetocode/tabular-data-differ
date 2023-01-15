import { FileInputStream, FileOutputStream } from "./streams";

describe('streams', () => {
    test('should read/write to a file', async () => {
        const f = new FileOutputStream('./output/files/test.txt');
        await f.open();
        try {
            await f.writeLine('Hello world!');
        } finally {
            await f.close();
        }
        const f2 = new FileInputStream('./output/files/test.txt');
        await f2.open();
        try {
            const line = await f2.readLine();
            expect(line).toBe('Hello world!');
        } finally {
            await f2.close();
        }
    });
    test('should open input file only once', async () => {
        const f = new FileInputStream('./tests/a.csv');
        await f.open();
        try {
            await expect(async () => {
                await f.open();
            }).rejects.toThrowError('file "./tests/a.csv" is already open');
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
            }).rejects.toThrowError('file "./output/files/test.txt" is already open');
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
        }).rejects.toThrowError(`ENOENT: no such file or directory, open './output/wrong-directory/test.txt'`);
    });
    test('should fail to write to output file when not open', async () => {
        const f = new FileOutputStream('./output/files/test.txt');
        await expect(async () => {
            await f.writeLine('Hello');
        }).rejects.toThrowError(`file \"./output/files/test.txt\" is not open!`);
    });
    test('should fail to write to output file when closed', async () => {
        const f = new FileOutputStream('./output/files/test.txt');
        await f.open();
        await f.close();
        await expect(async () => {
            await f.writeLine('Hello');
        }).rejects.toThrowError(`file \"./output/files/test.txt\" is not open!`);
    });
});