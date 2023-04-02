import fs from 'fs';
import { describe, expect, test } from '@jest/globals';
import { DifferOptions, diff, UnorderedStreamsError, Differ, UniqueKeyViolationError, DuplicateKeyHandler } from './differ';
import { FormatWriter, FormatHeader, RowDiff, FormatFooter, CsvFormatReader, defaultRowComparer, Column, Row, CellValue, stringComparer, cellComparer } from './formats';
import { ArrayInputStream, FileOutputStream, NullOutputStream } from './streams';

class FakeFormatWriter implements FormatWriter{
    public header?: FormatHeader;
    public diffs: RowDiff[] = [];
    public footer?: FormatFooter;

    open(): Promise<void> {
        return Promise.resolve();
    }
    writeHeader(header: FormatHeader): Promise<void> {
        this.header = header;
        return Promise.resolve();
    }
    writeDiff(rowDiff: RowDiff): Promise<void>  {
        this.diffs.push(rowDiff);
        return Promise.resolve();
    }
    writeFooter(footer: FormatFooter): Promise<void>  {
       this.footer = footer;
       return Promise.resolve();
    }
    close(): Promise<void> {
        return Promise.resolve();
    }
}

type DiffOptions = Omit<DifferOptions, "oldSource" | "newSource"> & {
    oldLines: string[],
    newLines: string[], 
    keepSameRows?: boolean,
    changeLimit?: number,
};

async function diffStrings(options: DiffOptions): Promise<FakeFormatWriter> {
    const writer = new FakeFormatWriter();
    await diff({
        ...options,
        oldSource: { 
            format: 'csv',
            stream: new ArrayInputStream(options.oldLines) 
        },
        newSource: {
            format: 'csv',
            stream: new ArrayInputStream(options.newLines),
        },
    }).to({
        destination: {
            format: 'custom',
            writer,
        },
        keepSameRows: options.keepSameRows,
        changeLimit: options.changeLimit,
    });
    return writer;
}

function readAllText(path: string): string {
    return fs.readFileSync(path).toString();
}

describe('differ', () => {
    beforeAll(() => {
        if(!fs.existsSync('./output')) {
            fs.mkdirSync('./output');
        }
        if(!fs.existsSync('./output/files')) {
            fs.mkdirSync('./output/files');
        }
    });
    describe('validation errors', () => {
        test('should reject unknown source format', async () => {
            await expect(async () => {
                await diff({ 
                    oldSource: {
                        format: <any>'foobar',
                        stream: './tests/a.csv',
                    },
                    newSource: {
                        format: 'csv',
                        stream: './tests/b.csv',
                    },
                    keys: ['id'],
                }).to('null');
            }).rejects.toThrowError(`Unknown source format 'foobar'`);
        });
        test('should reject unknown destination format', async () => {
            await expect(async () => {
                await diff({ 
                    oldSource: {
                        format: 'csv',
                        stream: './tests/a.csv',
                    },
                    newSource: {
                        format: 'csv',
                        stream: './tests/b.csv',
                    },
                    keys: ['id'],
                }).to({
                    destination: {
                        format: <any>'foo',
                        stream: 'console',    
                    },
                });
            }).rejects.toThrowError(`Unknown destination format 'foo'`);
        });        
        test('should detect invalid ordering in ascending mode', async () => {
            await expect(() => diffStrings({
                oldLines: [
                    'ID,NAME,AGE',
                    '1,john,33',
                    '2,rachel,22',
                    '3,dave,44',
                ],
                newLines: [
                    'ID,NAME,AGE',
                    '1,john,33',
                    '3,dave,44',
                    '2,rachel,22',
                ],
                keys: ['ID'],
            })).rejects.toThrowError(`Expected rows to be ordered by \"ID ASC\" in new source but received:
  previous=3,dave,44
  current=2,rachel,22`);
        });        
        test('should detect invalid ordering in descending mode', async () => {
            await expect(() => diffStrings({
                oldLines: [
                    'ID,NAME,AGE',
                    '3,dave,44',
                    '2,rachel,22',
                    '1,john,33',
                ],
                newLines: [
                    'ID,NAME,AGE',
                    '3,dave,44',
                    '1,john,33',
                    '2,rachel,22',
                ],
                keys: [{
                    name: 'ID',
                    order: 'DESC',
                }],
            })).rejects.toThrowError(new UnorderedStreamsError(`Expected rows to be ordered by "ID DESC" in new source but received:
  previous=1,john,33
  current=2,rachel,22`));
        });        
        test('should detect primary key violation in old source', async () => {
            await expect(() => diffStrings({
                oldLines: [
                    'ID,NAME,AGE',
                    '1,john,33',
                    '2,rachel,22',
                    '3,dave,44',
                    '3,dave bis,444',
                    '4,noemie,11',
                ],
                newLines: [
                    'ID,NAME,AGE',
                    '1,john,33',
                    '2,rachel,22',
                    '3,dave,44',
                ],
                keys: ['ID'],
            })).rejects.toThrowError(new UniqueKeyViolationError(`Expected rows to be unique by "ID" in old source but received:
  previous=3,dave,44
  current=3,dave bis,444
Note that you can resolve this conflict automatically using the duplicateKeyHandling option.`));
        });
        test('should detect primary key violation in new source', async () => {
            await expect(() => diffStrings({
                oldLines: [
                    'ID,NAME,AGE',
                    '1,john,33',
                    '2,rachel,22',
                    '3,dave,44',
                ],
                newLines: [
                    'ID,NAME,AGE',
                    '1,john,33',
                    '2,rachel,22',
                    '3,dave,44',
                    '3,dave bis,444',
                    '4,noemie,11',
                ],
                keys: ['ID'],
            })).rejects.toThrowError(new UniqueKeyViolationError(`Expected rows to be unique by "ID" in new source but received:
  previous=3,dave,44
  current=3,dave bis,444
Note that you can resolve this conflict automatically using the duplicateKeyHandling option.`));
        });
        test('should detect duplicate keys and return the first row', async () => {
            const writer = await diffStrings({
                oldLines: [
                    'ID,NAME,AGE',
                    '1,john,33',
                    '2,rachel,22',
                    '3,dave,44',
                    '3,dave bis,444',
                    '4,noemie,11',
                ],
                newLines: [
                    'ID,NAME,AGE',
                    '1,john,33',
                    '2,rachel,22',
                    '3,dave,44',
                ],
                keys: ['ID'],
                duplicateKeyHandling: 'keepFirstRow',
                keepSameRows: true,
            });
            expect(writer.diffs).toEqual([
                {
                  delta: 0,
                  status: 'same',
                  oldRow: [ '1', 'john', '33' ],
                  newRow: [ '1', 'john', '33' ]
                },
                {
                  delta: 0,
                  status: 'same',
                  oldRow: [ '2', 'rachel', '22' ],
                  newRow: [ '2', 'rachel', '22' ]
                },
                {
                  delta: 0,
                  status: 'same',
                  oldRow: [ '3', 'dave', '44' ],
                  newRow: [ '3', 'dave', '44' ]
                },
                { delta: -1, status: 'deleted', oldRow: [ '4', 'noemie', '11' ] }
            ]);
        });        
        test('should detect duplicate keys and return the last row', async () => {
            const writer = await diffStrings({
                oldLines: [
                    'ID,NAME,AGE',
                    '1,john,33',
                    '2,rachel,22',
                    '3,dave,44',
                    '3,dave bis,444',
                    '4,noemie,11',
                ],
                newLines: [
                    'ID,NAME,AGE',
                    '1,john,33',
                    '2,rachel,22',
                    '3,dave,44',
                ],
                keys: ['ID'],
                duplicateKeyHandling: 'keepLastRow',
                keepSameRows: true,
            });
            expect(writer.diffs).toEqual([
                {
                  delta: 0,
                  status: 'same',
                  oldRow: [ '1', 'john', '33' ],
                  newRow: [ '1', 'john', '33' ]
                },
                {
                  delta: 0,
                  status: 'same',
                  oldRow: [ '2', 'rachel', '22' ],
                  newRow: [ '2', 'rachel', '22' ]
                },
                {
                  delta: 0,
                  status: 'modified',
                  oldRow: [ '3', 'dave bis', '444' ],
                  newRow: [ '3', 'dave', '44' ]
                },
                { delta: -1, status: 'deleted', oldRow: [ '4', 'noemie', '11' ] }
            ]);
        });        
        test('should detect duplicate keys and call aggregate function', async () => {
            let duplicateRows: Row[] = [];
            const duplicateKeyHandler: DuplicateKeyHandler = (rows) => {
                if (duplicateRows.length === 0) {
                    duplicateRows = rows;
                }
                return rows[rows.length - 1];
            };
            const writer = await diffStrings({
                oldLines: [
                    'ID,NAME,AGE',
                    '1,john,33',
                    '2,rachel,22',
                    '3,dave,44',
                    '3,dave bis,444',
                    '4,noemie,11',
                ],
                newLines: [
                    'ID,NAME,AGE',
                    '1,john,33',
                    '2,rachel,22',
                    '3,dave,44',
                ],
                keys: ['ID'],
                duplicateKeyHandling: duplicateKeyHandler,
                keepSameRows: true,
            });
            expect(writer.diffs).toEqual([
                {
                  delta: 0,
                  status: 'same',
                  oldRow: [ '1', 'john', '33' ],
                  newRow: [ '1', 'john', '33' ]
                },
                {
                  delta: 0,
                  status: 'same',
                  oldRow: [ '2', 'rachel', '22' ],
                  newRow: [ '2', 'rachel', '22' ]
                },
                {
                  delta: 0,
                  status: 'modified',
                  oldRow: [ '3', 'dave bis', '444' ],
                  newRow: [ '3', 'dave', '44' ]
                },
                { delta: -1, status: 'deleted', oldRow: [ '4', 'noemie', '11' ] }
            ]);
            expect(duplicateRows).toEqual([ 
                [ '3', 'dave', '44' ], 
                [ '3', 'dave bis', '444' ] 
            ]);
        });    
        test('should detect duplicate keys and call aggregate function, with buffer overflow', async () => {
            const dups = [];
            for (let i = 0; i < 100; i++) {
                dups.push(`3,dave bis${i},444`);
            }
            let duplicateRows: Row[] = [];
            const duplicateKeyHandler: DuplicateKeyHandler = (rows) => {
                if (duplicateRows.length === 0) {
                    duplicateRows = rows;
                }
                return rows[rows.length - 1];
            };
            const writer = await diffStrings({
                oldLines: [
                    'ID,NAME,AGE',
                    '1,john,33',
                    '2,rachel,22',
                    '3,dave,44',
                    ...dups,
                    '4,noemie,11',
                ],
                newLines: [
                    'ID,NAME,AGE',
                    '1,john,33',
                    '2,rachel,22',
                    '3,dave,44',
                ],
                keys: ['ID'],
                duplicateKeyHandling: duplicateKeyHandler,
                duplicateRowBufferOverflow: true,
                duplicateRowBufferSize: 10,
                keepSameRows: true,
            });
            expect(writer.diffs).toEqual([
                {
                  delta: 0,
                  status: 'same',
                  oldRow: [ '1', 'john', '33' ],
                  newRow: [ '1', 'john', '33' ]
                },
                {
                  delta: 0,
                  status: 'same',
                  oldRow: [ '2', 'rachel', '22' ],
                  newRow: [ '2', 'rachel', '22' ]
                },
                {
                  delta: 0,
                  status: 'modified',
                  oldRow: [ '3', 'dave bis99', '444' ],
                  newRow: [ '3', 'dave', '44' ]
                },
                { delta: -1, status: 'deleted', oldRow: [ '4', 'noemie', '11' ] }
            ]);
            expect(duplicateRows).toEqual([ 
                [ '3', 'dave bis90', '444' ],
                [ '3', 'dave bis91', '444' ],
                [ '3', 'dave bis92', '444' ],
                [ '3', 'dave bis93', '444' ],
                [ '3', 'dave bis94', '444' ],
                [ '3', 'dave bis95', '444' ],
                [ '3', 'dave bis96', '444' ],
                [ '3', 'dave bis97', '444' ],
                [ '3', 'dave bis98', '444' ],
                [ '3', 'dave bis99', '444' ]
            ]);
        });    
        test('should detect duplicate keys and throw an error when the buffer exceeds the limit', async () => {
            const dups = [];
            for (let i = 0; i < 10; i++) {
                dups.push('3,dave bis,444');
            }
            expect(diffStrings({
                oldLines: [
                    'ID,NAME,AGE',
                    '1,john,33',
                    '2,rachel,22',
                    '3,dave,44',
                    ...dups,
                    '4,noemie,11',
                ],
                newLines: [
                    'ID,NAME,AGE',
                    '1,john,33',
                    '2,rachel,22',
                    '3,dave,44',
                ],
                keys: ['ID'],
                duplicateKeyHandling: (rows) => rows[0],
                duplicateRowBufferSize: 5,
                keepSameRows: true,
            })).rejects.toThrowError('Too many duplicate rows');
        });                    
        test('should be able to execute twice', async () => {
            const differ = diff({
                oldSource: './tests/a.csv',
                newSource: './tests/b.csv',
                keys: ['id'],
            });
            const stats1 = await differ.to('./output/files/output.csv');
            const output1= readAllText('./output/files/output.csv');
            const stats2 = await differ.to('./output/files/output.csv');
            const output2= readAllText('./output/files/output.csv');
            expect(stats1.totalChanges).toBe(6);
            expect(stats2).toEqual(stats1);
            expect(output2).toEqual(output1);
        });         
        test('should not open output file twice', async () => {
            const f = new FileOutputStream('./output/files/output.csv');
            await f.open();
            try {
                await expect(async () => await f.open()).rejects.toThrowError('file \"./output/files/output.csv\" is already open');
            } finally {
                await f.close();
            }
        });       
        test('should have columns in old source', async () => {
            await expect(() => diffStrings({
                oldLines: [
                ],
                newLines: [
                    'ID,NAME,AGE',
                ],
                keys: ['ID'],
            })).rejects.toThrowError('Expected to find columns in old source');
        });
        test('should have columns in new source', async () => {
            await expect(() => diffStrings({
                oldLines: [
                    'ID,NAME,AGE',
                ],
                newLines: [
                ],
                keys: ['ID'],
            })).rejects.toThrowError('Expected to find columns in new source');            
        });
        test('should find keys in old columns', async () => {
            await expect(() => diffStrings({
                oldLines: [
                    'CODE,NAME,AGE',
                ],
                newLines: [
                    'ID,NAME,AGE',
                ],
                keys: ['ID'],
            })).rejects.toThrowError(`Could not find key 'ID' in old stream`);            
        });
        test('should find keys in new columns', async () => {
            await expect(() => diffStrings({
                oldLines: [
                    'ID,NAME,AGE',
                    'a1,a,33',
                ],
                newLines: [
                    'CODE,NAME,AGE',
                    'a1,a,33',
                ],
                keys: ['ID'],
            })).rejects.toThrowError(`Could not find key 'ID' in new stream`);            
        });
        test('should not allow calling diffs() twice', async () => {
            const ctx = await diff({
                oldSource: './tests/a.csv',
                newSource: './tests/b.csv',
                keys: ['id'],
            }).start();
            const diffs = [];
            for await (const rowDiff of ctx.diffs()) {
                diffs.push(rowDiff);
            }
            expect(diffs.length).toBe(11);
            expect(ctx.isOpen).toBeFalsy();
            await expect(async () => {
                for await (const rowDiff of ctx.diffs()) {

                }
            }).rejects.toThrowError('Cannot get diffs on closed streams. You should call "Differ.start()" again.');
        });
        test('should not allow calling to() twice', async () => {
            const ctx = await diff({
                oldSource: './tests/a.csv',
                newSource: './tests/b.csv',
                keys: ['id'],
            }).start();
            expect(ctx.isOpen).toBeTruthy();
            const stats = await ctx.to('null');
            expect(stats.totalComparisons).toBe(11);
            expect(ctx.isOpen).toBeFalsy();
            await expect(async () => {
                await ctx.to('null');
            }).rejects.toThrowError('Cannot get diffs on closed streams. You should call "Differ.start()" again.');
        });        
        test('should allow calling start() twice', async () => {
            const differ = diff({
                oldSource: './tests/a.csv',
                newSource: './tests/b.csv',
                keys: ['id'],
            });

            const ctx = await differ.start();
            expect(ctx.isOpen).toBeTruthy();
            const diffs = [];
            for await (const rowDiff of ctx.diffs()) {
                diffs.push(rowDiff);
            }
            expect(diffs.length).toBe(11);
            expect(ctx.isOpen).toBeFalsy();

            const ctx2 = await differ.start();
            expect(ctx2.isOpen).toBeTruthy();
            expect(ctx2).not.toBe(ctx);
            const diffs2 = [];
            for await (const rowDiff of ctx2.diffs()) {
                diffs2.push(rowDiff);
            }
            expect(ctx2.isOpen).toBeFalsy();
            expect(diffs2.length).toBe(11);
            expect(diffs2).toEqual(diffs);
        });
    });
    describe('changes', () => {        
        test('both files are empty', async () => {
            const res = await diffStrings({
                oldLines: [
                    'ID,NAME,AGE',
                ],
                newLines: [
                    'ID,NAME,AGE',
                ],
                keys: ['ID'],
            });
            expect(res.header?.columns).toEqual(['ID', 'NAME', 'AGE']);
            expect(res.diffs).toEqual([]);
            expect(res.footer?.stats).toEqual({
                totalComparisons: 0,
                totalChanges: 0,
                added: 0,
                modified: 0,
                deleted: 0,
                same: 0,
                changePercent: 0,
            });
        });        
        test('old is empty', async () => {
            const res = await diffStrings({
                oldLines: [
                    'ID,NAME,AGE',
                ],
                newLines: [
                    'ID,NAME,AGE',
                    '1,john,33',
                    '2,rachel,22',
                ],
                keys: ['ID'],
            });
            expect(res.header?.columns).toEqual(['ID', 'NAME', 'AGE']);
            expect(res.diffs).toEqual([
                {
                    status: 'added',
                    delta: 1,
                    oldRow: undefined,
                    newRow: ['1','john','33'],
                },
                {
                    status: 'added',
                    delta: 1,
                    oldRow: undefined,
                    newRow: ['2','rachel','22'],
                },
            ]);
            expect(res.footer?.stats).toEqual({
                totalComparisons: 2,
                totalChanges: 2,
                added: 2,
                modified: 0,
                deleted: 0,
                same: 0,
                changePercent: 100,
            });
        });    
        test('new is empty', async () => {
            const res = await diffStrings({
                oldLines: [
                    'ID,NAME,AGE',
                    '1,john,33',
                    '2,rachel,22',
                ],
                newLines: [
                    'ID,NAME,AGE',
                ],
                keys: ['ID'],
            });
            expect(res.header?.columns).toEqual(['ID', 'NAME', 'AGE']);
            expect(res.diffs).toEqual([
                {
                    status: 'deleted',
                    delta: -1,
                    oldRow: ['1','john','33'],
                    newRow: undefined,
                },
                {
                    status: 'deleted',
                    delta: -1,
                    oldRow: ['2','rachel','22'],
                    newRow: undefined,
                },
            ]);
            expect(res.footer?.stats).toEqual({
                totalComparisons: 2,
                totalChanges: 2,
                added: 0,
                modified: 0,
                deleted: 2,
                same: 0,
                changePercent: 100,
            });
        });    
        test('same and do not keep same rows', async () => {
            const res = await diffStrings({
                oldLines: [
                    'ID,NAME,AGE',
                    '1,john,33',
                    '2,rachel,22',
                    '3,dave,44',
                ],
                newLines: [
                    'ID,NAME,AGE',
                    '1,john,33',
                    '2,rachel,22',
                    '3,dave,44',
                ],
                keys: ['ID'],
            });
            expect(res.header?.columns).toEqual(['ID', 'NAME', 'AGE']);
            expect(res.diffs).toEqual([]);
            expect(res.footer?.stats).toEqual({
                totalComparisons: 3,
                totalChanges: 0,
                added: 0,
                modified: 0,
                deleted: 0,
                same: 3,
                changePercent: 0,
            });
        });
        test('same and keep same rows', async () => {
            const res = await diffStrings({
                oldLines: [
                    'ID,NAME,AGE',
                    '1,john,33',
                    '2,rachel,22',
                    '3,dave,44',
                ],
                newLines: [
                    'ID,NAME,AGE',
                    '1,john,33',
                    '2,rachel,22',
                    '3,dave,44',
                ],
                keys: ['ID'],
                keepSameRows: true,
            });
            expect(res.header?.columns).toEqual(['ID', 'NAME', 'AGE']);
            expect(res.diffs).toEqual([
                {
                    status: 'same',
                    delta: 0,
                    oldRow: ['1','john','33'],
                    newRow: ['1','john','33'],
                },
                {
                    status: 'same',
                    delta: 0,
                    oldRow: ['2','rachel','22'],
                    newRow: ['2','rachel','22'],
                },
                {
                    status: 'same',
                    delta: 0,
                    oldRow: ['3','dave','44'],
                    newRow: ['3','dave','44'],
                },                
            ]);
            expect(res.footer?.stats).toEqual({
                totalComparisons: 3,
                totalChanges: 0,
                added: 0,
                modified: 0,
                deleted: 0,
                same: 3,
                changePercent: 0,
            });
        });
        test('same with reordered columns', async () => {
            const res = await diffStrings({
                oldLines: [
                    'ID,NAME,AGE',
                    '1,john,33',
                    '2,rachel,22',
                    '3,dave,44',
                ],
                newLines: [
                    'ID,AGE,NAME',
                    '1,33,john',
                    '2,22,rachel',
                    '3,44,dave',
                ],
                keys: ['ID'],
            });
            expect(res.header?.columns).toEqual(['ID', 'AGE', 'NAME']);
            expect(res.diffs).toEqual([]);
            expect(res.footer?.stats).toEqual({
                totalComparisons: 3,
                totalChanges: 0,
                added: 0,
                modified: 0,
                deleted: 0,
                same: 3,
                changePercent: 0,
            });
        });        
        test('same with excluded columns', async () => {
            const res = await diffStrings({
                oldLines: [
                    'ID,NAME,AGE',
                    '1,john,33',
                    '2,rachel,22',
                    '3,dave,44',
                ],
                newLines: [
                    'ID,NAME,AGE',
                    '1,john,33',
                    '2,rachel,20',
                    '3,dave,44',
                ],
                keys: ['ID'],
                excludedColumns: ['AGE'],
            });
            expect(res.header?.columns).toEqual(['ID', 'NAME']);
            expect(res.diffs).toEqual([]);
            expect(res.footer?.stats).toEqual({
                totalComparisons: 3,
                totalChanges: 0,
                added: 0,
                modified: 0,
                deleted: 0,
                same: 3,
                changePercent: 0,
            });
        });        
        test('same with included columns', async () => {
            const res = await diffStrings({
                oldLines: [
                    'ID,NAME,AGE',
                    '1,john,33',
                    '2,rachel,22',
                    '3,dave,44',
                ],
                newLines: [
                    'ID,NAME,AGE',
                    '1,john,33',
                    '2,rachel,20',
                    '3,dave,44',
                ],
                keys: ['ID'],
                includedColumns: ['ID', 'NAME'],
            });
            expect(res.header?.columns).toEqual(['ID', 'NAME']);
            expect(res.diffs).toEqual([]);
            expect(res.footer?.stats).toEqual({
                totalComparisons: 3,
                totalChanges: 0,
                added: 0,
                modified: 0,
                deleted: 0,
                same: 3,
                changePercent: 0,
            });
        });    
        test('1 modified', async () => {
            const res = await diffStrings({
                oldLines: [
                    'ID,NAME,AGE',
                    '1,john,33',
                    '2,rachel,22',
                    '3,dave,44',
                ],
                newLines: [
                    'ID,NAME,AGE',
                    '1,john,33',
                    '2,rachel,20',
                    '3,dave,44',
                ],
                keys: ['ID'],
            });
            expect(res.header?.columns).toEqual(['ID', 'NAME', 'AGE']);
            expect(res.diffs).toEqual([{
                status: 'modified',
                delta: 0,
                oldRow: ['2','rachel','22'],
                newRow: ['2','rachel','20'],
            }]);
            expect(res.footer?.stats).toEqual({
                totalComparisons: 3,
                totalChanges: 1,
                added: 0,
                modified: 1,
                deleted: 0,
                same: 2,
                changePercent: 33.33,
            });
        });    
        test('all modified', async () => {
            const res = await diffStrings({
                oldLines: [
                    'ID,NAME,AGE',
                    '1,john,30',
                    '2,rachel,20',
                    '3,dave,40',
                ],
                newLines: [
                    'ID,NAME,AGE',
                    '1,john,33',
                    '2,rachel,22',
                    '3,dave,44',
                ],
                keys: ['ID'],
            });
            expect(res.header?.columns).toEqual(['ID', 'NAME', 'AGE']);
            expect(res.diffs).toEqual([
                {
                    status: 'modified',
                    delta: 0,
                    oldRow: ['1','john','30'],
                    newRow: ['1','john','33'],
                },
                {
                    status: 'modified',
                    delta: 0,
                    oldRow: ['2','rachel','20'],
                    newRow: ['2','rachel','22'],
                },
                {
                    status: 'modified',
                    delta: 0,
                    oldRow: ['3','dave','40'],
                    newRow: ['3','dave','44'],
                },
            ]);
            expect(res.footer?.stats).toEqual({
                totalComparisons: 3,
                totalChanges: 3,
                added: 0,
                modified: 3,
                deleted: 0,
                same: 0,
                changePercent: 100,
            });
        });    
        test('1 modified with reordered columns', async () => {
            const res = await diffStrings({
                oldLines: [
                    'ID,NAME,AGE',
                    '1,john,33',
                    '2,rachel,22',
                    '3,dave,44',
                ],
                newLines: [
                    'ID,AGE,NAME',
                    '1,33,john',
                    '2,20,rachel',
                    '3,44,dave',
                ],
                keys: ['ID'],
            });
            expect(res.header?.columns).toEqual(['ID', 'AGE', 'NAME']);
            expect(res.diffs).toEqual([{
                status: 'modified',
                delta: 0,
                oldRow: ['2','22','rachel'],
                newRow: ['2','20','rachel'],
            }]);
            expect(res.footer?.stats).toEqual({
                totalComparisons: 3,
                totalChanges: 1,
                added: 0,
                modified: 1,
                deleted: 0,
                same: 2,
                changePercent: 33.33,
            });
        });    
        test('1 modified with excluded columns', async () => {
            const res = await diffStrings({
                oldLines: [
                    'ID,NAME,AGE',
                    '1,john,33',
                    '2,rachel,22',
                    '3,dave,44',
                ],
                newLines: [
                    'ID,NAME,AGE',
                    '1,john,33',
                    '2,rach,22',
                    '3,dave,44',
                ],
                keys: ['ID'],
                excludedColumns: ['AGE'],
            });
            expect(res.header?.columns).toEqual(['ID', 'NAME']);
            expect(res.diffs).toEqual([{
                status: 'modified',
                delta: 0,
                oldRow: ['2','rachel'],
                newRow: ['2','rach'],
            }]);
            expect(res.footer?.stats).toEqual({
                totalComparisons: 3,
                totalChanges: 1,
                added: 0,
                modified: 1,
                deleted: 0,
                same: 2,
                changePercent: 33.33,
            });
        });    
        test('1 added with excluded columns', async () => {
            // this test will also help boost code coverage in normalizeOldRow
            const res = await diffStrings({
                oldLines: [
                    'ID,NAME,AGE',
                    '1,john,33',
                ],
                newLines: [
                    'ID,NAME,AGE',
                    '1,john,33',
                    '2,rachel,22',
                ],
                keys: ['ID'],
                excludedColumns: ['AGE'],
            });
            expect(res.header?.columns).toEqual(['ID', 'NAME']);
            expect(res.diffs).toEqual([
                { delta: 1, status: 'added', newRow: [ '2', 'rachel' ] }
            ]);
            expect(res.footer?.stats).toEqual({
                totalComparisons: 2,
                totalChanges: 1,
                added: 1,
                modified: 0,
                deleted: 0,
                same: 1,
                changePercent: 50,
            });
        });    
        test('1 deleted with excluded columns', async () => {
            // this test will also help boost code coverage in normalizeNewRow
            const res = await diffStrings({
                oldLines: [
                    'ID,NAME,AGE',
                    '1,john,33',
                    '2,rachel,22',
                ],
                newLines: [
                    'ID,NAME,AGE',
                    '1,john,33',
                ],
                keys: ['ID'],
                excludedColumns: ['AGE'],
            });
            expect(res.header?.columns).toEqual(['ID', 'NAME']);
            expect(res.diffs).toEqual([
                { delta: -1, status: 'deleted', oldRow: [ '2', 'rachel' ] }
            ]);
            expect(res.footer?.stats).toEqual({
                totalComparisons: 2,
                totalChanges: 1,
                added: 0,
                modified: 0,
                deleted: 1,
                same: 1,
                changePercent: 50,
            });
        });    
        test('1 modified with included columns', async () => {
            const res = await diffStrings({
                oldLines: [
                    'ID,NAME,AGE',
                    '1,john,33',
                    '2,rachel,22',
                    '3,dave,44',
                ],
                newLines: [
                    'ID,NAME,AGE',
                    '1,john,33',
                    '2,rach,22',
                    '3,dave,44',
                ],
                keys: ['ID'],
                includedColumns: ['ID', 'NAME'],
            });
            expect(res.header?.columns).toEqual(['ID', 'NAME']);
            expect(res.diffs).toEqual([{
                status: 'modified',
                delta: 0,
                oldRow: ['2','rachel'],
                newRow: ['2','rach'],
            }]);
            expect(res.footer?.stats).toEqual({
                totalComparisons: 3,
                totalChanges: 1,
                added: 0,
                modified: 1,
                deleted: 0,
                same: 2,
                changePercent: 33.33,
            });
        });    
        test('No modification but adding a new column should force the rows to be modified', async () => {
            const res = await diffStrings({
                oldLines: [
                    'ID,NAME,AGE',
                    '1,john,33',
                    '2,rachel,22',
                    '3,dave,44',
                ],
                newLines: [
                    'ID,NAME,AGE,NEW_COL',
                    '1,john,33,new1',
                    '2,rachel,22,new2',
                    '3,dave,44,new3',
                ],
                keys: ['ID'],
            });
            expect(res.header?.columns).toEqual(['ID', 'NAME', 'AGE', 'NEW_COL']);
            expect(res.diffs).toEqual([
                {
                    delta: 0,
                    status: 'modified',
                    oldRow: [ '1', 'john', '33', '' ],
                    newRow: [ '1', 'john', '33', 'new1' ]
                },
                {
                    delta: 0,
                    status: 'modified',
                    oldRow: [ '2', 'rachel', '22', '' ],
                    newRow: [ '2', 'rachel', '22', 'new2' ]
                },
                {
                    delta: 0,
                    status: 'modified',
                    oldRow: [ '3', 'dave', '44', '' ],
                    newRow: [ '3', 'dave', '44', 'new3' ]
                }
            ]);
            expect(res.footer?.stats).toEqual({
                totalComparisons: 3,
                totalChanges: 3,
                added: 0,
                modified: 3,
                deleted: 0,
                same: 0,
                changePercent: 100,
            });
        });            
        test('No modification but removing an old column should be transparent', async () => {
            const res = await diffStrings({
                oldLines: [
                    'ID,NAME,AGE,REMOVED_COL',
                    '1,john,33,rem1',
                    '2,rachel,22,rem2',
                    '3,dave,44,rem3',
                ],
                newLines: [
                    'ID,NAME,AGE',
                    '1,john,33',
                    '2,rachel,22',
                    '3,dave,44',
                ],
                keys: ['ID'],
            });
            expect(res.header?.columns).toEqual(['ID', 'NAME', 'AGE']);
            expect(res.diffs).toEqual([]);
            expect(res.footer?.stats).toEqual({
                totalComparisons: 3,
                totalChanges: 0,
                added: 0,
                modified: 0,
                deleted: 0,
                same: 3,
                changePercent: 0,
            });
        });    
        test('1 deleted', async () => {
            const res = await diffStrings({
                oldLines: [
                    'ID,NAME,AGE',
                    '1,john,33',
                    '2,rachel,22',
                    '3,dave,44',
                ],
                newLines: [
                    'ID,NAME,AGE',
                    '1,john,33',
                    '3,dave,44',
                ],
                keys: ['ID'],
            });
            expect(res.header?.columns).toEqual(['ID', 'NAME', 'AGE']);
            expect(res.diffs).toEqual([{
                status: 'deleted',
                delta: -1,
                oldRow: ['2','rachel','22'],
                newRow: undefined,
            }]);
            expect(res.footer?.stats).toEqual({
                totalComparisons: 3,
                totalChanges: 1,
                added: 0,
                modified: 0,
                deleted: 1,
                same: 2,
                changePercent: 33.33,
            });
        });    
        test('1 added', async () => {
            const res = await diffStrings({
                oldLines: [
                    'ID,NAME,AGE',
                    '1,john,33',
                    '3,dave,44',
                ],
                newLines: [
                    'ID,NAME,AGE',
                    '1,john,33',
                    '2,rachel,20',
                    '3,dave,44',
                ],
                keys: ['ID'],
            });
            expect(res.header?.columns).toEqual(['ID', 'NAME', 'AGE']);
            expect(res.diffs).toEqual([{
                status: 'added',
                delta: 1,
                oldRow: undefined,
                newRow: ['2','rachel','20'],
            }]);
            expect(res.footer?.stats).toEqual({
                totalComparisons: 3,
                totalChanges: 1,
                added: 1,
                modified: 0,
                deleted: 0,
                same: 2,
                changePercent: 33.33,
            });
        });            
        test('only new rows and previous rows have been deleted', async () => {
            const res = await diffStrings({
                oldLines: [
                    'ID,NAME,AGE',
                    '1,john,33',
                    '2,rachel,22',
                    '3,dave,44',
                ],
                newLines: [
                    'ID,NAME,AGE',
                    '4,paula,11',
                    '5,jane,66',
                ],
                keys: ['ID'],
            });
            expect(res.header?.columns).toEqual(['ID', 'NAME', 'AGE']);
            expect(res.diffs).toEqual([
                {
                    status: 'deleted',
                    delta: -1,
                    oldRow: ['1','john','33'],
                    newRow: undefined,
                },
                {
                    status: 'deleted',
                    delta: -1,
                    oldRow: ['2','rachel','22'],
                    newRow: undefined,
                },
                {
                    status: 'deleted',
                    delta: -1,
                    oldRow: ['3','dave','44'],
                    newRow: undefined,
                },
                {
                    status: 'added',
                    delta: 1,
                    oldRow: undefined,
                    newRow: ['4','paula','11'],
                },
                {
                    status: 'added',
                    delta: 1,
                    oldRow: undefined,
                    newRow: ['5','jane','66'],
                },
            ]);
            expect(res.footer?.stats).toEqual({
                totalComparisons: 5,
                totalChanges: 5,
                added: 2,
                modified: 0,
                deleted: 3,
                same: 0,
                changePercent: 100,
            });
        });            
        test('same, modified, added and deleted', async () => {
            const res = await diffStrings({
                oldLines: [
                    'ID,NAME,AGE',
                    '1,john,33',
                    '2,rachel,22',
                    '3,dave,44',
                ],
                newLines: [
                    'ID,NAME,AGE',
                    '1,john,33',
                    '2,rachel,20',
                    '4,paula,11',
                ],
                keys: ['ID'],
                keepSameRows: true,
            });
            expect(res.header?.columns).toEqual(['ID', 'NAME', 'AGE']);
            expect(res.diffs).toEqual([
                {
                    status: 'same',
                    delta: 0,
                    oldRow: ['1','john','33'],
                    newRow: ['1','john','33'],
                },
                {
                    status: 'modified',
                    delta: 0,
                    oldRow: ['2','rachel','22'],
                    newRow: ['2','rachel','20'],
                },
                {
                    status: 'deleted',
                    delta: -1,
                    oldRow: ['3','dave','44'],
                    newRow: undefined,
                },
                {
                    status: 'added',
                    delta: 1,
                    oldRow: undefined,
                    newRow: ['4','paula','11'],
                },
            ]);
            expect(res.footer?.stats).toEqual({
                totalComparisons: 4,
                totalChanges: 3,
                added: 1,
                modified: 1,
                deleted: 1,
                same: 1,
                changePercent: 75,
            });
        });            
        test('same, modified, added and deleted with a case insensitive primary key', async () => {
            const caseInsensitiveCompare = function(a: CellValue, b: CellValue): number {
                if (typeof a === 'string' && typeof b === 'string') {
                    return stringComparer(a.toLowerCase(), b.toLowerCase());
                }
                return cellComparer(a, b);
            }
            const res = await diffStrings({
                oldLines: [
                    'ID,NAME,AGE',
                    'a1,john,33',
                    'a2,rachel,22',
                    'a3,dave,44',
                ],
                newLines: [
                    'ID,NAME,AGE',
                    'A1,john,33',
                    'A2,rachel,20',
                    'A4,paula,11',
                ],
                keys: [
                    {
                        name: 'ID',
                        comparer: caseInsensitiveCompare,
                    }
                ],
                keepSameRows: true,                
            });
            expect(res.header?.columns).toEqual(['ID', 'NAME', 'AGE']);
            expect(res.diffs).toEqual([
                {
                    status: 'same',
                    delta: 0,
                    oldRow: ['a1','john','33'],
                    newRow: ['A1','john','33'],
                },
                {
                    status: 'modified',
                    delta: 0,
                    oldRow: ['a2','rachel','22'],
                    newRow: ['A2','rachel','20'],
                },
                {
                    status: 'deleted',
                    delta: -1,
                    oldRow: ['a3','dave','44'],
                    newRow: undefined,
                },
                {
                    status: 'added',
                    delta: 1,
                    oldRow: undefined,
                    newRow: ['A4','paula','11'],
                },
            ]);
            expect(res.footer?.stats).toEqual({
                totalComparisons: 4,
                totalChanges: 3,
                added: 1,
                modified: 1,
                deleted: 1,
                same: 1,
                changePercent: 75,
            });
        });            
        test('same, modified, added and deleted, in descending order', async () => {
            const res = await diffStrings({
                oldLines: [
                    'ID,NAME,AGE',
                    '3,dave,44',
                    '2,rachel,22',
                    '1,john,33',
                ],
                newLines: [
                    'ID,NAME,AGE',
                    '4,paula,11',
                    '2,rachel,20',
                    '1,john,33',
                ],
                keys: [{
                    name: 'ID',
                    order: 'DESC',
                }],
                keepSameRows: true,
            });
            expect(res.header?.columns).toEqual(['ID', 'NAME', 'AGE']);
            expect(res.diffs).toEqual([
                {
                    status: 'added',
                    delta: 1,
                    oldRow: undefined,
                    newRow: ['4','paula','11'],
                },
                {
                    status: 'deleted',
                    delta: -1,
                    oldRow: ['3','dave','44'],
                    newRow: undefined,
                },
                {
                    status: 'modified',
                    delta: 0,
                    oldRow: ['2','rachel','22'],
                    newRow: ['2','rachel','20'],
                },
                {
                    status: 'same',
                    delta: 0,
                    oldRow: ['1','john','33'],
                    newRow: ['1','john','33'],
                },
            ]);
            expect(res.footer?.stats).toEqual({
                totalComparisons: 4,
                totalChanges: 3,
                added: 1,
                modified: 1,
                deleted: 1,
                same: 1,
                changePercent: 75,
            });
        });
        test('same, modified, added and deleted, with a number primary key', async () => {
            const res = await diffStrings({
                oldLines: [
                    'ID,NAME,AGE',
                    '1,john,11',
                    '2,rachel,22',
                    '3,dave,33',
                    '11,john,111',
                    '12,rachel,122',
                    '13,dave,133',
                    '21,john,211',
                    '22,rachel,222',
                    '23,dave,233',
                ],
                newLines: [
                    'ID,NAME,AGE',
                    '1,john,11',
                    '2,rachel,2',
                    '11,john,111',
                    '12,rachel,122',
                    '13,dave,133',
                    '14,dave,144',
                    '21,john,211',
                    '23,dave,233',
                ],
                keys: [{
                    name: 'ID',
                    comparer: 'number',
                }],
                keepSameRows: true,
            });
            expect(res.header?.columns).toEqual(['ID', 'NAME', 'AGE']);
            expect(res.diffs).toEqual([
                {
                    delta: 0,
                    status: 'same',
                    oldRow: [ '1', 'john', '11' ],
                    newRow: [ '1', 'john', '11' ]
                },
                {
                    delta: 0,
                    status: 'modified',
                    oldRow: [ '2', 'rachel', '22' ],
                    newRow: [ '2', 'rachel', '2' ]
                },
                { delta: -1, status: 'deleted', oldRow: [ '3', 'dave', '33' ] },
                {
                    delta: 0,
                    status: 'same',
                    oldRow: [ '11', 'john', '111' ],
                    newRow: [ '11', 'john', '111' ]
                },
                {
                    delta: 0,
                    status: 'same',
                    oldRow: [ '12', 'rachel', '122' ],
                    newRow: [ '12', 'rachel', '122' ]
                },
                {
                    delta: 0,
                    status: 'same',
                    oldRow: [ '13', 'dave', '133' ],
                    newRow: [ '13', 'dave', '133' ]
                },
                { delta: 1, status: 'added', newRow: [ '14', 'dave', '144' ] },
                {
                    delta: 0,
                    status: 'same',
                    oldRow: [ '21', 'john', '211' ],
                    newRow: [ '21', 'john', '211' ]
                },
                { delta: -1, status: 'deleted', oldRow: [ '22', 'rachel', '222' ] },
                {
                    delta: 0,
                    status: 'same',
                    oldRow: [ '23', 'dave', '233' ],
                    newRow: [ '23', 'dave', '233' ]
                }
            ]);
            expect(res.footer?.stats).toEqual({
                totalComparisons: 10,
                totalChanges: 4,
                added: 1,
                modified: 1,
                deleted: 2,
                same: 6,
                changePercent: 40,
            });
        });
        test('same, modified, added and deleted, with a number primary key, in descending order', async () => {
            const res = await diffStrings({
                oldLines: [
                    'ID,NAME,AGE',
                    '23,dave,233',
                    '22,rachel,222',
                    '21,john,211',
                    '13,dave,133',
                    '12,rachel,122',
                    '11,john,111',
                    '3,dave,33',
                    '2,rachel,22',
                    '1,john,11',
                ],
                newLines: [
                    'ID,NAME,AGE',
                    '23,dave,233',
                    '21,john,211',
                    '14,dave,144',
                    '13,dave,133',
                    '12,rachel,122',
                    '11,john,111',
                    '2,rachel,2',
                    '1,john,11',
                ],
                keys: [{
                    name: 'ID',
                    comparer: 'number',
                    order: 'DESC',
                }],
                keepSameRows: true,
            });
            expect(res.header?.columns).toEqual(['ID', 'NAME', 'AGE']);
            expect(res.diffs).toEqual([
                {
                    delta: 0,
                    status: 'same',
                    oldRow: [ '23', 'dave', '233' ],
                    newRow: [ '23', 'dave', '233' ]
                },
                { delta: -1, status: 'deleted', oldRow: [ '22', 'rachel', '222' ] },
                {
                    delta: 0,
                    status: 'same',
                    oldRow: [ '21', 'john', '211' ],
                    newRow: [ '21', 'john', '211' ]
                },
                { delta: 1, status: 'added', newRow: [ '14', 'dave', '144' ] },
                {
                    delta: 0,
                    status: 'same',
                    oldRow: [ '13', 'dave', '133' ],
                    newRow: [ '13', 'dave', '133' ]
                },
                {
                    delta: 0,
                    status: 'same',
                    oldRow: [ '12', 'rachel', '122' ],
                    newRow: [ '12', 'rachel', '122' ]
                },
                {
                    delta: 0,
                    status: 'same',
                    oldRow: [ '11', 'john', '111' ],
                    newRow: [ '11', 'john', '111' ]
                },
                { delta: -1, status: 'deleted', oldRow: [ '3', 'dave', '33' ] },
                {
                    delta: 0,
                    status: 'modified',
                    oldRow: [ '2', 'rachel', '22' ],
                    newRow: [ '2', 'rachel', '2' ]
                },
                {
                    delta: 0,
                    status: 'same',
                    oldRow: [ '1', 'john', '11' ],
                    newRow: [ '1', 'john', '11' ]
                }
            ]);
            expect(res.footer?.stats).toEqual({
                totalComparisons: 10,
                totalChanges: 4,
                added: 1,
                modified: 1,
                deleted: 2,
                same: 6,
                changePercent: 40,
            });
        });
        test('same, modified, added and deleted, with a complex primary key', async () => {
            const res = await diffStrings({
                oldLines: [
                    'CODE,VERSION,NAME,PRICE',
                    'apple,1,Apple,0.5',
                    'apple,2,Apple,0.6',
                    'banana,1,Bananax,0.2',
                    'banana,2,Banana,0.2',
                    'banana,3,Banana,0.25',
                ],
                newLines: [
                    'CODE,VERSION,NAME,PRICE',
                    'apple,1,Apple,0.5',
                    'apple,2,Apples,0.6',
                    'banana,2,Banana,0.2',
                    'banana,3,Banana,0.25',
                    'banana,4,Banana,0.3',
                    'pear,1,Pear,0.8',
                ],
                keys: [
                    { 
                        name: 'CODE',
                        comparer: 'string',
                    },
                    {
                        name: 'VERSION',
                        comparer: 'number',
                        order: 'ASC',
                    }
                ],
                keepSameRows: true,
            });
            expect(res.header?.columns).toEqual(['CODE', 'VERSION', 'NAME', 'PRICE']);
            expect(res.diffs).toEqual([
                {
                    delta: 0,
                    status: 'same',
                    oldRow: [ 'apple', '1', 'Apple', '0.5' ],
                    newRow: [ 'apple', '1', 'Apple', '0.5' ]
                },
                {
                    delta: 0,
                    status: 'modified',
                    oldRow: [ 'apple', '2', 'Apple', '0.6' ],
                    newRow: [ 'apple', '2', 'Apples', '0.6' ]
                },
                {
                    delta: -1,
                    status: 'deleted',
                    oldRow: [ 'banana', '1', 'Bananax', '0.2' ]
                },
                {
                    delta: 0,
                    status: 'same',
                    oldRow: [ 'banana', '2', 'Banana', '0.2' ],
                    newRow: [ 'banana', '2', 'Banana', '0.2' ]
                },
                {
                    delta: 0,
                    status: 'same',
                    oldRow: [ 'banana', '3', 'Banana', '0.25' ],
                    newRow: [ 'banana', '3', 'Banana', '0.25' ]
                },
                {
                    delta: 1,
                    status: 'added',
                    newRow: [ 'banana', '4', 'Banana', '0.3' ]
                },
                { delta: 1, status: 'added', newRow: [ 'pear', '1', 'Pear', '0.8' ] }
            ]);
            expect(res.footer?.stats).toEqual({
                totalComparisons: 7,
                totalChanges: 4,
                added: 2,
                modified: 1,
                deleted: 1,
                same: 3,
                changePercent: 57.14,
            });
        });        
        test('same, modified, added and deleted, with a complex primary key, in descending order for 2nd pk field', async () => {
            const res = await diffStrings({
                oldLines: [
                    'CODE,VERSION,NAME,PRICE',
                    'apple,2,Apple,0.6',
                    'apple,1,Apple,0.5',
                    'banana,3,Banana,0.25',
                    'banana,2,Banana,0.2',
                    'banana,1,Bananax,0.2',
                ],
                newLines: [
                    'CODE,VERSION,NAME,PRICE',
                    'apple,2,Apples,0.6',
                    'apple,1,Apple,0.5',
                    'banana,4,Banana,0.3',
                    'banana,3,Banana,0.25',
                    'banana,2,Banana,0.2',
                    'pear,1,Pear,0.8',
                ],
                keys: [
                    'CODE',
                    {
                        name: 'VERSION',
                        comparer: 'number',
                        order: 'DESC'
                    }
                ],
                keepSameRows: true,
            });
            expect(res.header?.columns).toEqual(['CODE', 'VERSION', 'NAME', 'PRICE']);
            expect(res.diffs).toEqual([
                {
                    delta: 0,
                    status: 'modified',
                    oldRow: [ 'apple', '2', 'Apple', '0.6' ],
                    newRow: [ 'apple', '2', 'Apples', '0.6' ]
                },
                {
                    delta: 0,
                    status: 'same',
                    oldRow: [ 'apple', '1', 'Apple', '0.5' ],
                    newRow: [ 'apple', '1', 'Apple', '0.5' ]
                },
                {
                    delta: 1,
                    status: 'added',
                    newRow: [ 'banana', '4', 'Banana', '0.3' ]
                },
                {
                    delta: 0,
                    status: 'same',
                    oldRow: [ 'banana', '3', 'Banana', '0.25' ],
                    newRow: [ 'banana', '3', 'Banana', '0.25' ]
                },
                {
                    delta: 0,
                    status: 'same',
                    oldRow: [ 'banana', '2', 'Banana', '0.2' ],
                    newRow: [ 'banana', '2', 'Banana', '0.2' ]
                },
                {
                    delta: -1,
                    status: 'deleted',
                    oldRow: [ 'banana', '1', 'Bananax', '0.2' ]
                },
                { delta: 1, status: 'added', newRow: [ 'pear', '1', 'Pear', '0.8' ] }
            ]);
            expect(res.footer?.stats).toEqual({
                totalComparisons: 7,
                totalChanges: 4,
                added: 2,
                modified: 1,
                deleted: 1,
                same: 3,
                changePercent: 57.14,
            });
        });        
        test('keep first 2 changes', async () => {
            const res = await diffStrings({
                oldLines: [
                    'ID,NAME,AGE',
                    '1,john,33',
                    '2,rachel,22',
                    '3,dave,44',
                ],
                newLines: [
                    'ID,NAME,AGE',
                    '1,john,33',
                    '2,rachel,20',
                    '4,paula,11',
                ],
                keys: ['ID'],
                keepSameRows: true,
                changeLimit: 2,
            });
            expect(res.header?.columns).toEqual(['ID', 'NAME', 'AGE']);
            expect(res.diffs).toEqual([
                {
                    status: 'same',
                    delta: 0,
                    oldRow: ['1','john','33'],
                    newRow: ['1','john','33'],
                },
                {
                    status: 'modified',
                    delta: 0,
                    oldRow: ['2','rachel','22'],
                    newRow: ['2','rachel','20'],
                },
                {
                    status: 'deleted',
                    delta: -1,
                    oldRow: ['3','dave','44'],
                    newRow: undefined,
                },
            ]);
            expect(res.footer?.stats).toEqual({
                totalComparisons: 3,
                totalChanges: 2,
                added: 0,
                modified: 1,
                deleted: 1,
                same: 1,
                changePercent: 66.67,
            });
        });
        test('should work with real source files (CSV)', async () => {
            const output = new FakeFormatWriter();
            const differ = diff({
                oldSource: './tests/a.csv',
                newSource: './tests/b.csv',
                keys: ['id'],
            });
            await differ.to({
                destination: {
                    format: 'custom',
                    writer: output,
                }
            });
            expect(output.header?.columns).toEqual([ 'id', 'a', 'b', 'c' ]);
            expect(output.diffs).toEqual([
                { delta: -1, status: 'deleted', oldRow: [ '01', 'a1', 'b1', 'c1' ] },
                {
                  delta: 0,
                  status: 'modified',
                  oldRow: [ '04', 'a4', 'b4', 'c4' ],
                  newRow: [ '04', 'aa4', 'bb4', 'cc4' ]
                },
                { delta: -1, status: 'deleted', oldRow: [ '05', 'a5', 'b5', 'c5' ] },
                { delta: -1, status: 'deleted', oldRow: [ '06', 'a6', 'b6', 'c6' ] },
                { delta: 1, status: 'added', newRow: [ '10', 'a10', 'b10', 'c10' ] },
                { delta: 1, status: 'added', newRow: [ '11', 'a11', 'b11', 'c11' ] }          
            ]);
            expect(output.footer?.stats).toEqual({
                totalComparisons: 11,
                totalChanges: 6,
                changePercent: 54.55,
                added: 2,
                deleted: 3,
                modified: 1,
                same: 5        
            });
        });
        test('should work with real source files (CSV) and support duplicate key handling', async () => {
            const output = new FakeFormatWriter();
            const differ = diff({
                oldSource: './tests/a2.csv',
                newSource: './tests/b2.csv',
                keys: ['id'],
                duplicateKeyHandling: 'keepFirstRow',
            });
            const ctx = await differ.start();
            await ctx.to({
                destination: {
                    format: 'custom',
                    writer: output,
                }
            });
            expect(output.header?.columns).toEqual([ 'id', 'a', 'b', 'c' ]);
            expect(output.diffs).toEqual([
                { delta: -1, status: 'deleted', oldRow: [ '01', 'a1', 'b1', 'c1' ] },
                {
                  delta: 0,
                  status: 'modified',
                  oldRow: [ '04', 'a4', 'b4', 'c4' ],
                  newRow: [ '04', 'aa4', 'bb4', 'cc4' ]
                },
                { delta: -1, status: 'deleted', oldRow: [ '05', 'a5', 'b5', 'c5' ] },
                { delta: -1, status: 'deleted', oldRow: [ '06', 'a6', 'b6', 'c6' ] },
                { delta: 1, status: 'added', newRow: [ '10', 'a10', 'b10', 'c10' ] },
            ]);
            console.log(output.footer?.stats);
            expect(output.footer?.stats).toEqual({
                totalComparisons: 10,
                totalChanges: 5,
                changePercent: 50,
                added: 1,
                deleted: 3,
                modified: 1,
                same: 5
            });
            expect(ctx.oldSourceStats).toEqual({
                rows: 12,
                duplicateRows: 3,
                uniqueRows: 9,
                uniqueRowsWithDuplicates: 3,
                duplicationPercent: 25,
                uniqueRowDuplicationPercent: 33.3333,
                maxDuplicatesPerUniqueKey: 1,
                minDuplicatesPerUniqueKey: 1,
                averageDuplicatesPerUniqueKey: 1                
            });
            expect(ctx.newSourceStats).toEqual({
                rows: 10,
                duplicateRows: 3,
                uniqueRows: 7,
                uniqueRowsWithDuplicates: 2,
                duplicationPercent: 30,
                uniqueRowDuplicationPercent: 28.5714,
                maxDuplicatesPerUniqueKey: 2,
                minDuplicatesPerUniqueKey: 1,
                averageDuplicatesPerUniqueKey: 1.5                 
            });
        });
        test('should work with http streams (CSV)', async () => {
            const currentDir = process.cwd().replaceAll('\\', '/');
            const stats = await diff({
                oldSource: new URL(`file://localhost/${currentDir}/tests/a.csv`),
                newSource: new URL(`file://localhost/${currentDir}/tests/b.csv`),
                keys: ['id'],
            }).to(new URL(`file://localhost/${currentDir}/output/files/output.csv`));
            expect(stats).toEqual({
                totalComparisons: 11,
                totalChanges: 6,
                changePercent: 54.55,
                added: 2,
                deleted: 3,
                modified: 1,
                same: 5        
            });
            const output = readAllText('./output/files/output.csv');
            expect(output).toBe(`DIFF_STATUS,id,a,b,c
deleted,01,a1,b1,c1
modified,04,aa4,bb4,cc4
deleted,05,a5,b5,c5
deleted,06,a6,b6,c6
added,10,a10,b10,c10
added,11,a11,b11,c11
`);
        });
        test('should work with real source files (TSV)', async () => {
            const output = new FakeFormatWriter();
            const differ = new Differ({
                oldSource: {
                    format: 'csv',
                    stream: './tests/a.tsv',
                    delimiter: '\t',
                },
                newSource: {
                    format: 'csv',
                    stream: './tests/b.tsv',
                    delimiter: '\t',
                },
                keys: ['id'],
            });
            await differ.to({
                destination: {
                    format: 'custom',
                    writer: output,
                }
            });
            expect(output.header?.columns).toEqual([ 'id', 'a', 'b', 'c' ]);
            expect(output.diffs).toEqual([
                { delta: -1, status: 'deleted', oldRow: [ '01', 'a1', 'b1', 'c1' ] },
                {
                  delta: 0,
                  status: 'modified',
                  oldRow: [ '04', 'a4', 'b4', 'c4' ],
                  newRow: [ '04', 'aa4', 'bb4', 'cc4' ]
                },
                { delta: -1, status: 'deleted', oldRow: [ '05', 'a5', 'b5', 'c5' ] },
                { delta: -1, status: 'deleted', oldRow: [ '06', 'a6', 'b6', 'c6' ] },
                { delta: 1, status: 'added', newRow: [ '10', 'a10', 'b10', 'c10' ] },
                { delta: 1, status: 'added', newRow: [ '11', 'a11', 'b11', 'c11' ] }          
            ]);
            expect(output.footer?.stats).toEqual({
                totalComparisons: 11,
                totalChanges: 6,
                changePercent: 54.55,
                added: 2,
                deleted: 3,
                modified: 1,
                same: 5        
            });
        });
        test('should produce a csv file (to)', async () => {
            const stats = await diff({
                oldSource: './tests/a.csv',
                newSource: './tests/b.csv',
                keys: ['id'],
            }).to('./output/files/output.csv');
            expect(stats).toEqual({
                totalComparisons: 11,
                totalChanges: 6,
                changePercent: 54.55,
                added: 2,
                deleted: 3,
                modified: 1,
                same: 5        
            });
            const output = readAllText('./output/files/output.csv');
            expect(output).toBe(`DIFF_STATUS,id,a,b,c
deleted,01,a1,b1,c1
modified,04,aa4,bb4,cc4
deleted,05,a5,b5,c5
deleted,06,a6,b6,c6
added,10,a10,b10,c10
added,11,a11,b11,c11
`);
        });
        test('should produce a csv file (to.destination)', async () => {
            const stats = await diff({
                oldSource: './tests/a.csv',
                newSource: './tests/b.csv',
                keys: ['id'],
            }).to({
                destination: './output/files/output.csv',
            });
            expect(stats).toEqual({
                totalComparisons: 11,
                totalChanges: 6,
                changePercent: 54.55,
                added: 2,
                deleted: 3,
                modified: 1,
                same: 5        
            });
            const output = readAllText('./output/files/output.csv');
            expect(output).toBe(`DIFF_STATUS,id,a,b,c
deleted,01,a1,b1,c1
modified,04,aa4,bb4,cc4
deleted,05,a5,b5,c5
deleted,06,a6,b6,c6
added,10,a10,b10,c10
added,11,a11,b11,c11
`);
        });
        test('should produce a csv file with a custom status column name', async () => {
            const stats = await diff({
                oldSource: './tests/a.csv',
                newSource: './tests/b.csv',
                keys: ['id'],
            }).to({
                destination: {
                    format: 'csv',
                    stream: './output/files/output.csv',
                    statusColumnName: 'diff',
                }
            });
            expect(stats).toEqual({
                totalComparisons: 11,
                totalChanges: 6,
                changePercent: 54.55,
                added: 2,
                deleted: 3,
                modified: 1,
                same: 5        
            });
            const output = readAllText('./output/files/output.csv');
            expect(output).toBe(`diff,id,a,b,c
deleted,01,a1,b1,c1
modified,04,aa4,bb4,cc4
deleted,05,a5,b5,c5
deleted,06,a6,b6,c6
added,10,a10,b10,c10
added,11,a11,b11,c11
`);
        });
        test('should read/write CSV files with an explicit reader/writer', async () => {
            const stats = await diff({
                oldSource: {
                    stream: './tests/a.csv',
                    format: 'csv',
                },
                newSource: {
                    format: 'custom',
                    reader: new CsvFormatReader({ stream: './tests/b.csv' }),
                },
                keys: ['id'],
            }).to({
                destination: {
                    format: 'csv',
                    stream: new FileOutputStream('./output/files/output.csv'),    
                }
            });
            expect(stats).toEqual({
                totalComparisons: 11,
                totalChanges: 6,
                changePercent: 54.55,
                added: 2,
                deleted: 3,
                modified: 1,
                same: 5        
            });
            const output = readAllText('./output/files/output.csv');
            expect(output).toBe(`DIFF_STATUS,id,a,b,c
deleted,01,a1,b1,c1
modified,04,aa4,bb4,cc4
deleted,05,a5,b5,c5
deleted,06,a6,b6,c6
added,10,a10,b10,c10
added,11,a11,b11,c11
`);
        });
        test('should produce a csv file with old and new values', async () => {
            const stats = await diff({
                oldSource: './tests/a.csv',
                newSource: './tests/b.csv',
                keys: ['id'],
            }).to({ 
                destination: {
                    format: 'csv',
                    stream: './output/files/output.csv',
                    keepOldValues: true,
                }
            });
            expect(stats).toEqual({
                totalComparisons: 11,
                totalChanges: 6,
                changePercent: 54.55,
                added: 2,
                deleted: 3,
                modified: 1,
                same: 5        
            });
            const output = readAllText('./output/files/output.csv');
            expect(output).toBe(`DIFF_STATUS,id,a,b,c,OLD_id,OLD_a,OLD_b,OLD_c
deleted,,,,,01,a1,b1,c1
modified,04,aa4,bb4,cc4,04,a4,b4,c4
deleted,,,,,05,a5,b5,c5
deleted,,,,,06,a6,b6,c6
added,10,a10,b10,c10,,,,
added,11,a11,b11,c11,,,,
`);
        });
        test('should produce a tsv file', async () => {
            const stats = await diff({
                oldSource: {
                    format: 'tsv',
                    stream: './tests/a.tsv',
                },
                newSource: {
                    format: 'tsv',
                    stream: './tests/b.tsv',
                },
                keys: ['id'],
            }).to({
                destination: {
                    format: 'tsv',
                    stream: './output/files/output.tsv',
                }
            });
            expect(stats).toEqual({
                totalComparisons: 11,
                totalChanges: 6,
                changePercent: 54.55,
                added: 2,
                deleted: 3,
                modified: 1,
                same: 5        
            });
            const output = readAllText('./output/files/output.tsv');
            expect(output).toBe(`DIFF_STATUS	id	a	b	c
deleted	01	a1	b1	c1
modified	04	aa4	bb4	cc4
deleted	05	a5	b5	c5
deleted	06	a6	b6	c6
added	10	a10	b10	c10
added	11	a11	b11	c11
`);
        });
        test('should produce a tsv file from a csv and a tsv', async () => {
            const stats = await diff({
                oldSource: {
                    format: 'csv',
                    stream: './tests/a.csv',
                },
                newSource: {
                    format: 'csv',
                    stream: './tests/b.tsv',
                    delimiter: '\t',
                },
                keys: ['id'],
            }).to({
                destination: {
                    format: 'csv',
                    stream: './output/files/output.tsv',
                    delimiter: '\t',
                }
            });
            expect(stats).toEqual({
                totalComparisons: 11,
                totalChanges: 6,
                changePercent: 54.55,
                added: 2,
                deleted: 3,
                modified: 1,
                same: 5        
            });
            const output = readAllText('./output/files/output.tsv');
            expect(output).toBe(`DIFF_STATUS	id	a	b	c
deleted	01	a1	b1	c1
modified	04	aa4	bb4	cc4
deleted	05	a5	b5	c5
deleted	06	a6	b6	c6
added	10	a10	b10	c10
added	11	a11	b11	c11
`);
        });
        test('should read a json containing null values and convert them to empty strings in a CSV', async () => {
            const stats = await diff({
                oldSource: {
                    stream: new ArrayInputStream([
                        '[',
                        '{"id":1, "name":"john", "active": true, "cash": null}',
                        '{"id":2, "name":"mary", "active": false, "cash": 3210.22}',
                        ']'
                    ]),
                    format: 'json',
                },
                newSource: {
                    stream: new ArrayInputStream([
                        '[',
                        '{"id":1, "name":"john", "active": true, "cash": 1234.99}',
                        '{"id":2, "name":"mary", "active": true, "cash": null}',
                        ']'
                    ]),
                    format: 'json',
                },
                keys: ['id'],
            }).to('./output/files/output.csv');
            expect(stats).toEqual({
                totalComparisons: 2,
                totalChanges: 2,
                changePercent: 100,
                added: 0,
                deleted: 0,
                modified: 2,
                same: 0
            });
            const output = readAllText('./output/files/output.csv');
            expect(output).toBe(`DIFF_STATUS,id,name,active,cash
modified,1,john,true,1234.99
modified,2,mary,true,
`);
        });        
        test('should produce a json file', async () => {
            const stats = await diff({
                oldSource: './tests/a.csv',
                newSource: './tests/b.csv',
                keys: ['id'],
            }).to({
                destination: {
                    format: 'json',
                    stream: './output/files/output.json',    
                }
            });
            expect(stats).toEqual({
                totalComparisons: 11,
                totalChanges: 6,
                changePercent: 54.55,
                added: 2,
                deleted: 3,
                modified: 1,
                same: 5        
            });
            const output = readAllText('./output/files/output.json');
            expect(output).toBe(`{ "header": {"columns":["id","a","b","c"]}, "items": [
{"status":"deleted","data":["01","a1","b1","c1"]}
,{"status":"modified","data":["04","aa4","bb4","cc4"]}
,{"status":"deleted","data":["05","a5","b5","c5"]}
,{"status":"deleted","data":["06","a6","b6","c6"]}
,{"status":"added","data":["10","a10","b10","c10"]}
,{"status":"added","data":["11","a11","b11","c11"]}
], "footer": {"stats":{"totalComparisons":11,"totalChanges":6,"changePercent":54.55,"added":2,"deleted":3,"modified":1,"same":5}}}
`);
        });    
        test('should produce a json file containing numbers and booleans', async () => {
            const stats = await diff({
                oldSource: {
                    stream: new ArrayInputStream([
                        '[',
                        '{"id":1, "name":"john", "active": true, "cash": 1234.56}',
                        '{"id":2, "name":"mary", "active": false, "cash": 3210.22}',
                        '{"id":12, "name":"sarah", "active": true, "cash": 500}',
                        ']'
                    ]),
                    format: 'json',
                },
                newSource: {
                    stream: new ArrayInputStream([
                        '[',
                        '{"id":1, "name":"john", "active": true, "cash": 1234.99}',
                        '{"id":2, "name":"mary", "active": true, "cash": 3210.22}',
                        '{"id":12, "name":"sarah", "active": true, "cash": 600}',
                        ']'
                    ]),
                    format: 'json',
                },
                keys: ['id'],
            }).to({
                destination: {
                    format: 'json',
                    stream: './output/files/output.json',    
                },
            });
            expect(stats).toEqual({
                totalComparisons: 3,
                totalChanges: 3,
                changePercent: 100,
                added: 0,
                deleted: 0,
                modified: 3,
                same: 0    
            });
            const output = readAllText('./output/files/output.json');
            expect(output).toBe(`{ "header": {"columns":["id","name","active","cash"]}, "items": [
{"status":"modified","data":[1,"john",true,1234.99]}
,{"status":"modified","data":[2,"mary",true,3210.22]}
,{"status":"modified","data":[12,"sarah",true,600]}
], "footer": {"stats":{"totalComparisons":3,"totalChanges":3,"changePercent":100,"added":0,"deleted":0,"modified":3,"same":0}}}
`);
        });    
        test('should produce a json file with old and new values', async () => {
            const stats = await diff({
                oldSource: './tests/a.csv',
                newSource: './tests/b.csv',
                keys: ['id'],
            }).to({
                destination: {
                    format: 'json',
                    stream: './output/files/output.json',
                    keepOldValues: true,    
                }
            });
            expect(stats).toEqual({
                totalComparisons: 11,
                totalChanges: 6,
                changePercent: 54.55,
                added: 2,
                deleted: 3,
                modified: 1,
                same: 5        
            });
            const output = readAllText('./output/files/output.json');
            expect(output).toBe(`{ "header": {"columns":["id","a","b","c"]}, "items": [
{"status":"deleted","old":["01","a1","b1","c1"]}
,{"status":"modified","new":["04","aa4","bb4","cc4"],"old":["04","a4","b4","c4"]}
,{"status":"deleted","old":["05","a5","b5","c5"]}
,{"status":"deleted","old":["06","a6","b6","c6"]}
,{"status":"added","new":["10","a10","b10","c10"]}
,{"status":"added","new":["11","a11","b11","c11"]}
], "footer": {"stats":{"totalComparisons":11,"totalChanges":6,"changePercent":54.55,"added":2,"deleted":3,"modified":1,"same":5}}}
`);
        });    
        test('should produce a json file with labels in the header', async () => {
            const stats = await diff({
                oldSource: './tests/a.csv',
                newSource: './tests/b.csv',
                keys: ['id'],
            }).to({
                destination: {
                    format: 'json',
                    stream: './output/files/output.json',    
                },
                labels: {
                    generatedAt: '2023-01-02T01:21:57Z',
                    source: 'Some source...'
                }
            });
            expect(stats).toEqual({
                totalComparisons: 11,
                totalChanges: 6,
                changePercent: 54.55,
                added: 2,
                deleted: 3,
                modified: 1,
                same: 5        
            });
            const output = readAllText('./output/files/output.json');
            expect(output).toBe(`{ "header": {"columns":["id","a","b","c"],"labels":{"generatedAt":"2023-01-02T01:21:57Z","source":"Some source..."}}, "items": [
{"status":"deleted","data":["01","a1","b1","c1"]}
,{"status":"modified","data":["04","aa4","bb4","cc4"]}
,{"status":"deleted","data":["05","a5","b5","c5"]}
,{"status":"deleted","data":["06","a6","b6","c6"]}
,{"status":"added","data":["10","a10","b10","c10"]}
,{"status":"added","data":["11","a11","b11","c11"]}
], "footer": {"stats":{"totalComparisons":11,"totalChanges":6,"changePercent":54.55,"added":2,"deleted":3,"modified":1,"same":5}}}
`);
        });    
        test('should read a JSON input file', async () => {
            const stats = await diff({
                oldSource: {
                    stream: './tests/a.json',
                    format: 'json',
                },
                newSource: {
                    stream: './tests/b.json',
                    format: 'json',
                },
                keys: ['id'],
            }).to('./output/files/output.csv');
            expect(stats).toEqual({
                totalComparisons: 11,
                totalChanges: 6,
                changePercent: 54.55,
                added: 2,
                deleted: 3,
                modified: 1,
                same: 5        
            });
            const output = readAllText('./output/files/output.csv');
            expect(output).toBe(`DIFF_STATUS,id,a,b,c
deleted,01,a1,b1,c1
modified,04,aa4,bb4,cc4
deleted,05,a5,b5,c5
deleted,06,a6,b6,c6
added,10,a10,b10,c10
added,11,a11,b11,c11
`);
        });        
        test('should read a JSON and a CSV input file', async () => {
            const stats = await diff({
                oldSource: {
                    stream: './tests/a.csv',
                    format: 'csv',
                },
                newSource: {
                    stream: './tests/b.json',
                    format: 'json',
                },
                keys: ['id'],
            }).to('./output/files/output.csv');
            expect(stats).toEqual({
                totalComparisons: 11,
                totalChanges: 6,
                changePercent: 54.55,
                added: 2,
                deleted: 3,
                modified: 1,
                same: 5        
            });
            const output = readAllText('./output/files/output.csv');
            expect(output).toBe(`DIFF_STATUS,id,a,b,c
deleted,01,a1,b1,c1
modified,04,aa4,bb4,cc4
deleted,05,a5,b5,c5
deleted,06,a6,b6,c6
added,10,a10,b10,c10
added,11,a11,b11,c11
`);
        });        
        test('should display output on the console (to)', async () => {
            const stats = await diff({
                oldSource: './tests/a.csv',
                newSource: './tests/b.csv',
                keys: ['id'],
            }).to('console');
            expect(stats).toEqual({
                totalComparisons: 11,
                totalChanges: 6,
                changePercent: 54.55,
                added: 2,
                deleted: 3,
                modified: 1,
                same: 5        
            });
        });        
        test('should display output on the console (to.destination)', async () => {
            const stats = await diff({
                oldSource: './tests/a.csv',
                newSource: './tests/b.csv',
                keys: ['id'],
            }).to({
                destination: 'console',
            });
            expect(stats).toEqual({
                totalComparisons: 11,
                totalChanges: 6,
                changePercent: 54.55,
                added: 2,
                deleted: 3,
                modified: 1,
                same: 5        
            });
        });        
        test('should not produce anything but stats (to)', async () => {
            const stats = await diff({
                oldSource: './tests/a.csv',
                newSource: './tests/b.csv',
                keys: ['id'],
            }).to('null');
            expect(stats).toEqual({
                totalComparisons: 11,
                totalChanges: 6,
                changePercent: 54.55,
                added: 2,
                deleted: 3,
                modified: 1,
                same: 5        
            });
        });
        test('should not produce anything but stats (to.destination', async () => {
            const stats = await diff({
                oldSource: './tests/a.csv',
                newSource: './tests/b.csv',
                keys: ['id'],
            }).to({
                destination: 'null',
            });
            expect(stats).toEqual({
                totalComparisons: 11,
                totalChanges: 6,
                changePercent: 54.55,
                added: 2,
                deleted: 3,
                modified: 1,
                same: 5        
            });
        });
        test('should be able to iterate over the diffs', async () => {
            const ctx = await diff({
                oldSource: './tests/a.csv',
                newSource: './tests/b.csv',
                keys: ['id'],
            }).start();
            expect(ctx.isOpen).toBeTruthy();
            expect(ctx.columns).toEqual(['id', 'a', 'b', 'c']);
            expect(ctx.stats).toEqual({
                totalComparisons: 0,
                totalChanges: 0,
                changePercent: 0,
                added: 0,
                deleted: 0,
                modified: 0,
                same: 0
            });
            const diffs: RowDiff[] = [];
            for await (const rowDiff of ctx.diffs()) {
                diffs.push(rowDiff);
            }
            expect(ctx.isOpen).toBeFalsy();
            expect(ctx.columns).toEqual(['id', 'a', 'b', 'c']);
            expect(diffs).toEqual([
                { delta: -1, status: 'deleted', oldRow: [ '01', 'a1', 'b1', 'c1' ] },
                {
                  delta: 0,
                  status: 'same',
                  oldRow: [ '02', 'a2', 'b2', 'c2' ],
                  newRow: [ '02', 'a2', 'b2', 'c2' ]
                },
                {
                  delta: 0,
                  status: 'same',
                  oldRow: [ '03', 'a3', 'b3', 'c3' ],
                  newRow: [ '03', 'a3', 'b3', 'c3' ]
                },
                {
                  delta: 0,
                  status: 'modified',
                  oldRow: [ '04', 'a4', 'b4', 'c4' ],
                  newRow: [ '04', 'aa4', 'bb4', 'cc4' ]
                },
                { delta: -1, status: 'deleted', oldRow: [ '05', 'a5', 'b5', 'c5' ] },
                { delta: -1, status: 'deleted', oldRow: [ '06', 'a6', 'b6', 'c6' ] },
                {
                  delta: 0,
                  status: 'same',
                  oldRow: [ '07', 'a7', 'b7', 'c7' ],
                  newRow: [ '07', 'a7', 'b7', 'c7' ]
                },
                {
                  delta: 0,
                  status: 'same',
                  oldRow: [ '08', 'a8', 'b8', 'c8' ],
                  newRow: [ '08', 'a8', 'b8', 'c8' ]
                },
                {
                  delta: 0,
                  status: 'same',
                  oldRow: [ '09', 'a9', 'b9', 'c9' ],
                  newRow: [ '09', 'a9', 'b9', 'c9' ]
                },
                { delta: 1, status: 'added', newRow: [ '10', 'a10', 'b10', 'c10' ] },
                { delta: 1, status: 'added', newRow: [ '11', 'a11', 'b11', 'c11' ] }
            ]);
            expect(ctx.stats).toEqual({
                totalComparisons: 11,
                totalChanges: 6,
                changePercent: 54.55,
                added: 2,
                deleted: 3,
                modified: 1,
                same: 5        
            });
        });    
        test('should be able to get the column names before invoking the "to" method', async () => {
            const ctx = await diff({
                oldSource: './tests/a.csv',
                newSource: './tests/b.csv',
                keys: ['id'],
            }).start();
            expect(ctx.isOpen).toBeTruthy();
            expect(ctx.columns).toEqual(['id', 'a', 'b', 'c']);
            expect(ctx.stats).toEqual({
                totalComparisons: 0,
                totalChanges: 0,
                changePercent: 0,
                added: 0,
                deleted: 0,
                modified: 0,
                same: 0
            });
            const stats = await ctx.to('./output/files/output.csv');
            const output = readAllText('./output/files/output.csv');
            expect(ctx.isOpen).toBeFalsy();
            expect(ctx.columns).toEqual(['id', 'a', 'b', 'c']);
            expect(output).toBe(`DIFF_STATUS,id,a,b,c
deleted,01,a1,b1,c1
modified,04,aa4,bb4,cc4
deleted,05,a5,b5,c5
deleted,06,a6,b6,c6
added,10,a10,b10,c10
added,11,a11,b11,c11
`);
            expect(stats).toEqual({
                totalComparisons: 11,
                totalChanges: 6,
                changePercent: 54.55,
                added: 2,
                deleted: 3,
                modified: 1,
                same: 5        
            });
            expect(ctx.stats).toEqual(stats);
        });
        test('should be able to filter the output', async () => {
            const ctx = await diff({
                oldSource: './tests/c.csv',
                newSource: './tests/d.csv',
                keys: [
                    'CODE',
                    {
                        name: 'VERSION',
                        comparer: 'number',
                    }
                ],
            }).start();
            expect(ctx.isOpen).toBeTruthy();
            expect(ctx.columns).toEqual(['CODE', 'VERSION', 'NAME', 'CATEGORY', 'PRICE']);
            expect(ctx.stats).toEqual({
                totalComparisons: 0,
                totalChanges: 0,
                changePercent: 0,
                added: 0,
                deleted: 0,
                modified: 0,
                same: 0
            });
            const catIdx = ctx.columns.indexOf('CATEGORY');
            const stats = await ctx.to({
                destination: {
                    format: 'csv',
                    stream: './output/files/output.csv',
                    keepOldValues: true,
                },
                filter: (rowDiff) => ['Fruit', 'Meat'].includes(rowDiff.newRow?.[catIdx]?.toString() ?? rowDiff.oldRow?.[catIdx]?.toString() ?? ''),
            });
            const output = readAllText('./output/files/output.csv');
            expect(ctx.isOpen).toBeFalsy();
            expect(ctx.columns).toEqual(['CODE', 'VERSION', 'NAME', 'CATEGORY', 'PRICE']);
            expect(output).toBe(`DIFF_STATUS,CODE,VERSION,NAME,CATEGORY,PRICE,OLD_CODE,OLD_VERSION,OLD_NAME,OLD_CATEGORY,OLD_PRICE
added,apple,3,Apple,Fruit,0.4,,,,,
modified,banana,1,Banana,Fruit,0.25,banana,1,Bannana,Fruit,0.25
deleted,,,,,,banana,2,Banana,Fruit,0.27
added,banana,4,Banana,Fruit,0.4,,,,,
added,beef,2,Beef,Meat,11,,,,,
added,pear,3,Pear,Fruit,0.35,,,,,
`);
            expect(ctx.stats).not.toEqual(stats);
            expect(stats).toEqual({
                totalComparisons: 12,
                totalChanges: 6,
                changePercent: 50,
                added: 4,
                deleted: 1,
                modified: 1,
                same: 6    
            });
        });            
        test('should be able to get the columns and close the files', async () => {
            const ctx = await diff({
                oldSource: './tests/a.csv',
                newSource: './tests/b.csv',
                keys: ['id'],
            }).start();
            expect(ctx.columns).toEqual(['id', 'a', 'b', 'c']);
            expect(ctx.isOpen).toBeTruthy();
            ctx.close();
            expect(ctx.isOpen).toBeFalsy();
        });
        test('should work with NullOutputStream', async () => {
            const ctx = await diff({
                oldSource: './tests/a.csv',
                newSource: './tests/b.csv',
                keys: ['id'],
            }).to({
                destination: {
                    format: 'csv',
                    stream: new NullOutputStream(),
                },
            });
        });
        test('should work with explicit null output stream', async () => {
            const ctx = await diff({
                oldSource: './tests/a.csv',
                newSource: './tests/b.csv',
                keys: ['id'],
            }).to({
                destination: {
                    format: 'csv',
                    stream: 'null',
                },
            });
        });
        test('should work with explicit console stream', async () => {
            const ctx = await diff({
                oldSource: './tests/a.csv',
                newSource: './tests/b.csv',
                keys: ['id'],
            }).to({
                destination: {
                    format: 'csv',
                    stream: 'console',
                },
            });
        });
        test('comparing the same file should not produce any change', async () => {
            const output = new FakeFormatWriter();
            const differ = diff({
                oldSource: './tests/a.csv',
                newSource: './tests/a.csv',
                keys: ['id'],
            });
            await differ.to({
                destination: {
                    format: 'custom',
                    writer: output,
                }
            });
            expect(output.header?.columns).toEqual([ 'id', 'a', 'b', 'c' ]);
            expect(output.diffs).toEqual([     
            ]);
            expect(output.footer?.stats).toEqual({
                totalComparisons: 9,
                totalChanges: 0,
                changePercent: 0,
                added: 0,
                deleted: 0,
                modified: 0,
                same: 9    
            });
        });
        test('should ignore duplicate rows, with single key', async () => {
            const stats = await diff({
                oldSource: {
                    stream: new ArrayInputStream([
                        '[',
                        '{"id":1, "name":"john", "active": true, "cash": null}',
                        '{"id":2, "name":"mary", "active": false, "cash": 3210.22}', // same and duplicate
                        '{"id":2, "name":"mary", "active": false, "cash": 3210.22}', // same and duplicate
                        '{"id":3, "name":"sarah", "active": true, "cash": 500}',
                        '{"id":8, "name":"lily", "active": true, "cash": 100}', // deleted row and duplicate
                        '{"id":8, "name":"lily", "active": true, "cash": 100}', // deleted row and duplicate
                        ']'
                    ]),
                    format: 'json',
                },
                newSource: {
                    stream: new ArrayInputStream([
                        '[',
                        '{"id":1, "name":"john", "active": true, "cash": 1234.99}',
                        '{"id":2, "name":"mary", "active": false, "cash": 3210.22}', // same and duplicate
                        '{"id":2, "name":"mary", "active": false, "cash": 3210.22}', // same and duplicate
                        '{"id":2, "name":"mary", "active": false, "cash": 3210.22}', // same and duplicate
                        '{"id":3, "name":"sarah", "active": true, "cash": 600}',
                        '{"id":4, "name":"david", "active": false, "cash": 700}', // added and duplicate
                        '{"id":4, "name":"david", "active": false, "cash": 700}', // added and duplicate
                        ']'
                    ]),
                    format: 'json',
                },
                keys: ['id'],
                duplicateKeyHandling: 'keepFirstRow',
            }).to('./output/files/output.csv');
            const output = readAllText('./output/files/output.csv');
            expect(stats).toEqual({
                totalComparisons: 5,
                totalChanges: 4,
                changePercent: 80,
                added: 1,
                deleted: 1,
                modified: 2,
                same: 1
            });
            expect(output).toBe(`DIFF_STATUS,id,name,active,cash
modified,1,john,true,1234.99
modified,3,sarah,true,600
added,4,david,false,700
deleted,8,lily,true,100
`);
        });                     
        test('should ignore duplicate rows, with multiple keys', async () => {
            const stats = await diff({
                oldSource: {
                    stream: new ArrayInputStream([
                        '[',
                        '{"id":1, "name":"john", "active": true, "cash": 1234.99}',
                        '{"id":2, "name":"mary", "active": false, "cash": 3210.22}',
                        '{"id":2, "name":"mary", "active": false, "cash": 3210.22}',
                        ']'
                    ]),
                    format: 'json',
                },
                newSource: {
                    stream: new ArrayInputStream([
                        '[',
                        '{"id":1, "name":"john", "active": true, "cash": 1234.99}',
                        '{"id":2, "name":"mary", "active": false, "cash": 3210.22}',
                        '{"id":2, "name":"mary", "active": false, "cash": 3210.22}',
                        '{"id":3, "name":"sarah", "active": true, "cash": 600}',
                        ']'
                    ]),
                    format: 'json',
                },
                keys: ['id', 'name', 'active', 'cash'],
                duplicateKeyHandling: 'keepFirstRow',
            }).to('./output/files/output.csv');
            expect(stats).toEqual({
                totalComparisons: 3,
                totalChanges: 1,
                changePercent: 33.33,
                added: 1,
                deleted: 0,
                modified: 0,
                same: 2
            });
            const output = readAllText('./output/files/output.csv');
            expect(output).toBe(`DIFF_STATUS,id,name,active,cash
added,3,sarah,true,600
`);
        });               
        test('should not consider a row to be a duplicate if the non key columns are different', async () => {
            await expect(async () => {
                await diff({
                    oldSource: {
                        stream: new ArrayInputStream([
                            '[',
                            '{"id":1, "name":"john", "active": true, "cash": null}',
                            '{"id":2, "name":"mary", "active": false, "cash": 3210.22}',
                            '{"id":2, "name":"mary", "active": false, "cash": 100}',
                            '{"id":3, "name":"sarah", "active": true, "cash": 500}',
                            ']'
                        ]),
                        format: 'json',
                    },
                    newSource: {
                        stream: new ArrayInputStream([
                            '[',
                            '{"id":1, "name":"john", "active": true, "cash": 1234.99}',
                            '{"id":2, "name":"mary", "active": false, "cash": 3210.22}',
                            '{"id":2, "name":"mary", "active": false, "cash": 100}',
                            '{"id":3, "name":"sarah", "active": true, "cash": 600}',
                            ']'
                        ]),
                        format: 'json',
                    },
                    keys: ['id'],
                }).to('./output/files/output.csv');
            }).rejects.toThrowError(`Expected rows to be unique by "id" in old source but received:
  previous=2,mary,false,3210.22
  current=2,mary,false,100`);
        });
        test('should accept an iterable source', async () => {
            const stats = await diff({
                oldSource: {
                    format: 'iterable',
                    provider: someAsyncSource,
                },
                newSource: {
                    format: 'iterable',
                    provider: () => someAsyncSource(2),
                },
                keys: ['id'],
            }).to('./output/files/output.csv');
            const output = readAllText('./output/files/output.csv');
            expect(output).toBe(`DIFF_STATUS,id,name,age
deleted,3,Cindy,44
`);
            expect(stats).toEqual({
                totalComparisons: 3,
                totalChanges: 1,
                changePercent: 33.33,
                added: 0,
                deleted: 1,
                modified: 0,
                same: 2
            });
        });
    });
});

async function *someAsyncSource(limit?: number) {
    let items = [
        {
            id: 1,
            name: 'John',
            age: 33,
        },
        {
            id: 2,
            name: 'Mary',
            age: 22,
        },
        {
            id: 3,
            name: 'Cindy',
            age: 44,
        },
    ];  
    if (limit !== undefined){
        items = items.slice(0, limit);
    }
    for (const item of items) {
        yield item;
    }
}




