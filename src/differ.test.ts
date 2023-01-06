import fs from 'fs';
import {describe, expect, test} from '@jest/globals';
import { defaultRowComparer, diff, Differ, parseCsvLine, Column, serializeRowAsCsvLine, DifferOptions, FileOutputStream, ArrayInputStream, FileInputStream, RowDiff, StreamWriter, StreamWriterFooter, StreamWriterHeader, UnorderedStreamsError, NullOutputStream, CsvStreamReader, CsvStreamWriter, JsonStreamReader } from './differ';

class FakeOutputWriter implements StreamWriter{
    public header?: StreamWriterHeader;
    public diffs: RowDiff[] = [];
    public footer?: StreamWriterFooter;

    open(): void {}
    writeHeader(header: StreamWriterHeader): void {
        this.header = header;
    }
    writeDiff(rowDiff: RowDiff): void {
        this.diffs.push(rowDiff);
    }
    writeFooter(footer: StreamWriterFooter): void {
       this.footer = footer;
    }
    close(): void {}
}

type DiffOptions = Omit<DifferOptions, "oldSource" | "newSource"> & {
    oldLines: string[],
    newLines: string[], 
    keepSameRows?: boolean,
    changeLimit?: number,
};

function diffStrings(options: DiffOptions): FakeOutputWriter {
    const result = new FakeOutputWriter();
    diff({
        ...options,
        oldSource: { 
            stream: new ArrayInputStream(options.oldLines) 
        },
        newSource: {
            stream: new ArrayInputStream(options.newLines),
        },
    }).to({
        format: (_options) => result,
        keepSameRows: options.keepSameRows,
        changeLimit: options.changeLimit,
    });
    return result;
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
    describe('parsing', () => {
        test('a,b,c', () => {
           const row = parseCsvLine(',', 'a,b,c');
           expect(row).toEqual(['a', 'b', 'c']);
        });    
        test('empty list', () => {
            const row = parseCsvLine(',', ',,');
            expect(row).toEqual(['', '', '']);
         });    
         test('with spaces', () => {
            const row = parseCsvLine(',', 'a a,b b,c c');
            expect(row).toEqual(['a a', 'b b', 'c c']);
         });    
         test('with comma', () => {
            const row = parseCsvLine(',', '"1,1","2,2","3,3"');
            expect(row).toEqual(['1,1', '2,2', '3,3']);
         });    
         test('with double quote', () => {
            const row = parseCsvLine(',', '"a ""b"" c","""a b c""","3 "","" 3"');
            expect(row).toEqual(['a "b" c', '"a b c"', '3 "," 3']);
         });    
         test('with tab separator', () => {
            const row = parseCsvLine('\t', 'a\tb\tc');
            expect(row).toEqual(['a', 'b', 'c']);
        });    
        test('with quoted tab separator', () => {
            const row = parseCsvLine('\t', 'a\t"b\tb"\tc');
            expect(row).toEqual(['a', 'b\tb', 'c']);
        });    
        test('with trailing comma', () => {
            const row = parseCsvLine(',', 'a,b,c,');
            expect(row).toEqual(['a', 'b', 'c', '']);
         });    
    });
    describe('JSON reader', () => {
        test('single compact row', () => {
            const stream = new ArrayInputStream([
                '[{"id": "1","a":"a1","b":"b1","c":"c1"}]',
            ]);
            const reader = new JsonStreamReader({ stream });
            reader.open();
            const header = reader.readHeader();
            expect(header.columns).toEqual(['id','a','b','c']);
            const row1 = reader.readRow();
            expect(row1).toEqual(['1', 'a1', 'b1', 'c1']);
            const done = reader.readRow();
            expect(done).toBeUndefined();
            reader.close();
        });
        test('single indented row', () => {
            const stream = new ArrayInputStream([
                '[',
                '  {"id": "1","a":"a1","b":"b1","c":"c1"}',
                ']'
            ]);
            const reader = new JsonStreamReader({ stream });
            reader.open();
            const header = reader.readHeader();
            expect(header.columns).toEqual(['id','a','b','c']);
            const row1 = reader.readRow();
            expect(row1).toEqual(['1', 'a1', 'b1', 'c1']);
            const done = reader.readRow();
            expect(done).toBeUndefined();
            reader.close();
        });
        test('inlined brackets, with trailing comma', () => {
            const stream = new ArrayInputStream([
                '[{"id": "1","a":"a1","b":"b1","c":"c1"},',
                '{"id": "2","a":"a2","b":"b2","c":"c2"},',
                '{"id": "3","a":"a3","b":"b3","c":"c3"}]',
            ]);
            const reader = new JsonStreamReader({ stream });
            reader.open();
            const header = reader.readHeader();
            expect(header.columns).toEqual(['id','a','b','c']);
            const row1 = reader.readRow();
            expect(row1).toEqual(['1', 'a1', 'b1', 'c1']);
            const row2 = reader.readRow();
            expect(row2).toEqual(['2', 'a2', 'b2', 'c2']);
            const row3 = reader.readRow();
            expect(row3).toEqual(['3', 'a3', 'b3', 'c3']);
            const done = reader.readRow();
            expect(done).toBeUndefined();
            reader.close();
        });
        test('outlined brackets, with trailing comma', () => {
            const stream = new ArrayInputStream([
                '[',
                '  {"id": "1","a":"a1","b":"b1","c":"c1"},',
                '  {"id": "2","a":"a2","b":"b2","c":"c2"},',
                '  {"id": "3","a":"a3","b":"b3","c":"c3"}',
                ']',
            ]);
            const reader = new JsonStreamReader({ stream });
            reader.open();
            const header = reader.readHeader();
            expect(header.columns).toEqual(['id','a','b','c']);
            const row1 = reader.readRow();
            expect(row1).toEqual(['1', 'a1', 'b1', 'c1']);
            const row2 = reader.readRow();
            expect(row2).toEqual(['2', 'a2', 'b2', 'c2']);
            const row3 = reader.readRow();
            expect(row3).toEqual(['3', 'a3', 'b3', 'c3']);
            const done = reader.readRow();
            expect(done).toBeUndefined();
            reader.close();
        });
        test('inlined brackets, with preceding comma', () => {
            const stream = new ArrayInputStream([
                '[{"id": "1","a":"a1","b":"b1","c":"c1"}',
                ',{"id": "2","a":"a2","b":"b2","c":"c2"}',
                ',{"id": "3","a":"a3","b":"b3","c":"c3"}]',
            ]);
            const reader = new JsonStreamReader({ stream });
            reader.open();
            const header = reader.readHeader();
            expect(header.columns).toEqual(['id','a','b','c']);
            const row1 = reader.readRow();
            expect(row1).toEqual(['1', 'a1', 'b1', 'c1']);
            const row2 = reader.readRow();
            expect(row2).toEqual(['2', 'a2', 'b2', 'c2']);
            const row3 = reader.readRow();
            expect(row3).toEqual(['3', 'a3', 'b3', 'c3']);
            const done = reader.readRow();
            expect(done).toBeUndefined();
            reader.close();
        });
        test('outlined brackets, with preceding comma', () => {
            const stream = new ArrayInputStream([
                '[',
                '  {"id": "1","a":"a1","b":"b1","c":"c1"}',
                '  ,{"id": "2","a":"a2","b":"b2","c":"c2"}',
                '  ,{"id": "3","a":"a3","b":"b3","c":"c3"}',
                ']',
            ]);
            const reader = new JsonStreamReader({ stream });
            reader.open();
            const header = reader.readHeader();
            expect(header.columns).toEqual(['id','a','b','c']);
            const row1 = reader.readRow();
            expect(row1).toEqual(['1', 'a1', 'b1', 'c1']);
            const row2 = reader.readRow();
            expect(row2).toEqual(['2', 'a2', 'b2', 'c2']);
            const row3 = reader.readRow();
            expect(row3).toEqual(['3', 'a3', 'b3', 'c3']);
            const done = reader.readRow();
            expect(done).toBeUndefined();
            reader.close();
        });
        test('empty string should fail', () => {
            const stream = new ArrayInputStream([
                '',
            ]);
            const reader = new JsonStreamReader({ stream });
            reader.open();
            expect(() => {
                reader.readHeader();
            }).toThrowError('Expected to find at least one object');
        });        
        test('empty stream should fail', () => {
            const stream = new ArrayInputStream([]);
            const reader = new JsonStreamReader({ stream });
            reader.open();
            expect(() => {
                reader.readHeader();
            }).toThrowError('Expected to find at least one object');
        });        
        test('row should contain an object or fail, while reading the header', () => {
            const stream = new ArrayInputStream([
                '123',
            ]);
            const reader = new JsonStreamReader({ stream });
            reader.open();
            expect(() => {
                reader.readHeader();
            }).toThrowError('Expected to find a JSON object');
        });        
        test('row should contain an object or fail, while reading the header', () => {
            const stream = new ArrayInputStream([
                '[',
                '  {"id": "1","a":"a1","b":"b1","c":"c1"},',
                '  123,',
                '  {"id": "3","a":"a3","b":"b3","c":"c3"}',
                ']',
            ]);
            const reader = new JsonStreamReader({ stream });
            reader.open();
            reader.readHeader();
            const row1 = reader.readRow();
            expect(row1).toEqual(['1', 'a1', 'b1', 'c1']);
            expect(() => {
                reader.readRow();
            }).toThrowError('Expected to find a JSON object');
        });        
    });
    describe('formatting', () => {
        test('a,b,c', () => {
            const txt = serializeRowAsCsvLine(['a', 'b', 'c']);
            expect(txt).toEqual('a,b,c');
         });    
         test('empty list', () => {
            const txt = serializeRowAsCsvLine(['', '', '']);
            expect(txt).toEqual(',,');
         });    
         test('with space, comma, double quote', () => {
            const txt = serializeRowAsCsvLine(['a":"a', 'b b', 'c,c']);
            expect(txt).toEqual('"a"":""a",b b,"c,c"');
         });    
 
    });
    describe('comparison', () => {
        test('should have at least one key', () => {
            expect(() => defaultRowComparer([], undefined, undefined)).toThrowError('Expected to have at least one entry in the columns parameter');
        });
        describe('undefined rows', () => {
            const keys: Column[] = [{
                name: 'id',
                oldIndex: 0,
                newIndex: 0,
            }]
            test('equal', () => {
                const res = defaultRowComparer(keys, undefined, undefined);
                expect(res).toBe(0);
            });    
            test('less than', () => {
                const a = ['2', 'a'];
                const b = undefined;
                const res = defaultRowComparer(keys, a, b);
                expect(res).toBe(-1);
            });    
            test('greater than', () => {
                const a = undefined;
                const b = ['2', 'a'];
                const res = defaultRowComparer(keys, a, b);
                expect(res).toBe(1);
            });    
        });        
        describe('single pk column', () => {
            const keys: Column[] = [{
                name: 'id',
                oldIndex: 0,
                newIndex: 0,
            }]
            test('equal', () => {
                const a = ['1', 'a'];
                const b = ['1', 'b'];
                const res = defaultRowComparer(keys, a, b);
                expect(res).toBe(0);
            });    
            test('less than', () => {
                const a = ['1', 'b'];
                const b = ['2', 'a'];
                const res = defaultRowComparer(keys, a, b);
                expect(res).toBe(-1);
            });    
            test('greater than', () => {
                const a = ['2', 'a'];
                const b = ['1', 'b'];
                const res = defaultRowComparer(keys, a, b);
                expect(res).toBe(1);
            });    
        });
        describe('2 pk columns', () => {
            const keys: Column[] = [
                {
                    name: 'i1d',
                    oldIndex: 0,
                    newIndex: 0,
                },
                {
                    name: 'id2',
                    oldIndex: 1,
                    newIndex: 1,
                },
            ]
            test('equal', () => {
                const a = ['1', '1', 'c'];
                const b = ['1', '1', 'd'];
                const res = defaultRowComparer(keys, a, b);
                expect(res).toBe(0);
            });    
            test('less than', () => {
                const a = ['1', 'a', 'c'];
                const b = ['1', 'b', 'c'];
                const res = defaultRowComparer(keys, a, b);
                expect(res).toBe(-1);
            });    
            test('less than undefined field', () => {
                const a = ['1'];
                const b = ['1', 'b', 'c'];
                const res = defaultRowComparer(keys, a, b);
                expect(res).toBe(-1);
            });    
            test('greater than', () => {
                const a = ['1', 'b', 'c'];
                const b = ['1', 'a', 'c'];
                const res = defaultRowComparer(keys, a, b);
                expect(res).toBe(1);
            });    
            test('greater than undefined field', () => {
                const a = ['1', 'b', 'c'];
                const b = ['1'];
                const res = defaultRowComparer(keys, a, b);
                expect(res).toBe(1);
            });    
        });
    });
    describe('validation errors', () => {
        test('should detect invalid ordering in ascending mode', () => {
            expect(() => diffStrings({
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
            })).toThrowError(`Expected rows to be ordered by \"ID ASC\" in new source but received:
  previous=3,dave,44
  current=2,rachel,22`);
        });        
        test('should detect invalid ordering in descending mode', () => {
            expect(() => diffStrings({
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
            })).toThrowError(new UnorderedStreamsError(`Expected rows to be ordered by "ID DESC" in new source but received:
  previous=1,john,33
  current=2,rachel,22`));
        });        
        test('should be able to execute twice', () => {
            const differ = diff({
                oldSource: './tests/a.csv',
                newSource: './tests/b.csv',
                keys: ['id'],
            });
            const stats1 = differ.to('./output/files/output.csv');
            const output1= readAllText('./output/files/output.csv');
            const stats2 = differ.to('./output/files/output.csv');
            const output2= readAllText('./output/files/output.csv');
            expect(stats1.totalChanges).toBe(6);
            expect(stats2).toEqual(stats1);
            expect(output2).toEqual(output1);
        });         
        test('should not open output file twice', () => {
            const f = new FileOutputStream('./output/files/output.csv');
            f.open();
            try {
                expect(() => f.open()).toThrowError('file \"./output/files/output.csv\" is already open');
            } finally {
                f.close();
            }
        });       
        test('should have columns in old source', () => {
            expect(() => diffStrings({
                oldLines: [
                ],
                newLines: [
                    'ID,NAME,AGE',
                ],
                keys: ['ID'],
            })).toThrowError('Expected to find columns in old source');
        });
        test('should have columns in new source', () => {
            expect(() => diffStrings({
                oldLines: [
                    'ID,NAME,AGE',
                ],
                newLines: [
                ],
                keys: ['ID'],
            })).toThrowError('Expected to find columns in new source');            
        });
        test('should find keys in old columns', () => {
            expect(() => diffStrings({
                oldLines: [
                    'CODE,NAME,AGE',
                ],
                newLines: [
                    'ID,NAME,AGE',
                ],
                keys: ['ID'],
            })).toThrowError(`Could not find key 'ID' in old stream`);            
        });
        test('should find keys in new columns', () => {
            expect(() => diffStrings({
                oldLines: [
                    'ID,NAME,AGE',
                    'a1,a,33',
                ],
                newLines: [
                    'CODE,NAME,AGE',
                    'a1,a,33',
                ],
                keys: ['ID'],
            })).toThrowError(`Could not find key 'ID' in new stream`);            
        });
        test('should not allow calling diffs() twice', () => {
            const ctx = diff({
                oldSource: './tests/a.csv',
                newSource: './tests/b.csv',
                keys: ['id'],
            }).start();
            const diffs = [...ctx.diffs()];
            expect(diffs.length).toBe(11);
            expect(ctx.isOpen).toBeFalsy();
            expect(() => {
                [...ctx.diffs()];
            }).toThrowError('Cannot get diffs on closed streams. You should call "Differ.start()" again.');
        });
        test('should allow calling start() twice', () => {
            const differ = diff({
                oldSource: './tests/a.csv',
                newSource: './tests/b.csv',
                keys: ['id'],
            });

            const ctx = differ.start();
            expect(ctx.isOpen).toBeTruthy();
            const diffs = [...ctx.diffs()];
            expect(diffs.length).toBe(11);
            expect(ctx.isOpen).toBeFalsy();

            const ctx2 = differ.start();
            expect(ctx2.isOpen).toBeTruthy();
            expect(ctx2).not.toBe(ctx);
            const diffs2 = [...ctx2.diffs()];
            expect(ctx2.isOpen).toBeFalsy();
            expect(diffs2.length).toBe(11);
            expect(diffs2).toEqual(diffs);
        });
        test('should not be able to read input streams after a close', () => {
            const stream = new FileInputStream('./tests/a.csv');
            stream.open();
            const header = stream.readLine();
            expect(header).toBe('id,a,b,c');
            stream.close();
            expect(() => {
                stream.readLine();
            }).toThrowError('FileInputStream "./tests/a.csv" is not open');
        });
    });
    describe('changes', () => {        
        test('both files are empty', () => {
            const res = diffStrings({
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
        test('old is empty', () => {
            const res = diffStrings({
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
        test('new is empty', () => {
            const res = diffStrings({
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
        test('same and do not keep same rows', () => {
            const res = diffStrings({
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
        test('same and keep same rows', () => {
            const res = diffStrings({
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
        test('same with reordered columns', () => {
            const res = diffStrings({
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
        test('same with excluded columns', () => {
            const res = diffStrings({
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
        test('same with included columns', () => {
            const res = diffStrings({
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
        test('1 modified', () => {
            const res = diffStrings({
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
        test('all modified', () => {
            const res = diffStrings({
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
        test('1 modified with reordered columns', () => {
            const res = diffStrings({
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
        test('1 modified with excluded columns', () => {
            const res = diffStrings({
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
        test('1 added with excluded columns', () => {
            // this test will also help boost code coverage in normalizeOldRow
            const res = diffStrings({
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
        test('1 deleted with excluded columns', () => {
            // this test will also help boost code coverage in normalizeNewRow
            const res = diffStrings({
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
        test('1 modified with included columns', () => {
            const res = diffStrings({
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
        test('No modification but adding a new column should force the rows to be modified', () => {
            const res = diffStrings({
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
        test('No modification but removing an old column should be transparent', () => {
            const res = diffStrings({
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
        test('1 deleted', () => {
            const res = diffStrings({
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
        test('1 added', () => {
            const res = diffStrings({
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
        test('only new rows and previous rows have been deleted', () => {
            const res = diffStrings({
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
        test('same, modified, added and deleted', () => {
            const res = diffStrings({
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
        test('same, modified, added and deleted, in descending order', () => {
            const res = diffStrings({
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
        test('same, modified, added and deleted, with a number primary key', () => {
            const res = diffStrings({
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
        test('same, modified, added and deleted, with a number primary key, in descending order', () => {
            const res = diffStrings({
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
        test('same, modified, added and deleted, with a complex primary key', () => {
            const res = diffStrings({
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
        test('same, modified, added and deleted, with a complex primary key, in descending order for 2nd pk field', () => {
            const res = diffStrings({
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
        test('keep first 2 changes', () => {
            const res = diffStrings({
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
        test('should work with real source files (CSV)', () => {
            const output = new FakeOutputWriter();
            const differ = diff({
                oldSource: './tests/a.csv',
                newSource: './tests/b.csv',
                keys: ['id'],
            });
            differ.to({
                format: (_options) => output,
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
        test('should work with real source files (TSV)', () => {
            const output = new FakeOutputWriter();
            const differ = new Differ({
                oldSource: {
                    stream: './tests/a.tsv',
                    delimiter: '\t',
                },
                newSource: {
                    stream: './tests/b.tsv',
                    delimiter: '\t',
                },
                keys: ['id'],
            });
            differ.to({
                format: (_options) => output,
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
        test('should produce a csv file', () => {
            const stats = diff({
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
        test('should read/write CSV files with an explicit reader/writer', () => {
            const stats = diff({
                oldSource: {
                    stream: './tests/a.csv',
                    format: 'csv',
                },
                newSource: {
                    stream: './tests/b.csv',
                    format: (options) => new CsvStreamReader(options),
                },
                keys: ['id'],
            }).to({
                stream: new FileOutputStream('./output/files/output.csv'),
                format: 'csv',
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
        test('should produce a csv file with old and new values', () => {
            const stats = diff({
                oldSource: './tests/a.csv',
                newSource: './tests/b.csv',
                keys: ['id'],
            }).to({ 
                stream: './output/files/output.csv',
                keepOldValues: true,
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
        test('should produce a tsv file', () => {
            const stats = diff({
                oldSource: {
                    stream: './tests/a.tsv',
                    delimiter: '\t',
                },
                newSource: {
                    stream: './tests/b.tsv',
                    delimiter: '\t',
                },
                keys: ['id'],
            }).to({
                stream: './output/files/output.tsv',
                delimiter: '\t',
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
        test('should produce a tsv file from a csv and a tsv', () => {
            const stats = diff({
                oldSource: {
                    stream: './tests/a.csv',
                },
                newSource: {
                    stream: './tests/b.tsv',
                    delimiter: '\t',
                },
                keys: ['id'],
            }).to({
                stream: './output/files/output.tsv',
                delimiter: '\t',
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
        test('should produce a json file', () => {
            const stats = diff({
                oldSource: './tests/a.csv',
                newSource: './tests/b.csv',
                keys: ['id'],
            }).to({
                stream: './output/files/output.json',
                format: 'json',
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
        test('should produce a json file with old and new values', () => {
            const stats = diff({
                oldSource: './tests/a.csv',
                newSource: './tests/b.csv',
                keys: ['id'],
            }).to({
                stream: './output/files/output.json',
                format: 'json',
                keepOldValues: true,
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
        test('should produce a json file with labels in the header', () => {
            const stats = diff({
                oldSource: './tests/a.csv',
                newSource: './tests/b.csv',
                keys: ['id'],
            }).to({
                stream: './output/files/output.json',
                format: 'json',
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
        test('should read a JSON input file', () => {
            const stats = diff({
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
        test('should read a JSON and a CSV input file', () => {
            const stats = diff({
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
        test('should display output on the console', () => {
            const stats = diff({
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
        test('should not produce anything but stats', () => {
            const stats = diff({
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
        test('should be able to iterate over the diffs', () => {
            const ctx = diff({
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
            for (const rowDiff of ctx.diffs()) {
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
        test('should be able to get the column names before invoking the "to" method', () => {
            const ctx = diff({
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
            const stats = ctx.to('./output/files/output.csv');
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
        test('should be able to filter the output', () => {
            const ctx = diff({
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
            const stats = ctx.to({
                stream: './output/files/output.csv',
                filter: (rowDiff) => (rowDiff.newRow?.[catIdx] ?? rowDiff.oldRow?.[catIdx]) !== 'Vegetable',
                keepOldValues: true,
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
        test('should be able to get the columns and close the files', () => {
            const ctx = diff({
                oldSource: './tests/a.csv',
                newSource: './tests/b.csv',
                keys: ['id'],
            }).start();
            expect(ctx.columns).toEqual(['id', 'a', 'b', 'c']);
            expect(ctx.isOpen).toBeTruthy();
            ctx.close();
            expect(ctx.isOpen).toBeFalsy();
        });
        test('should work with NullOutputStream', () => {
            const ctx = diff({
                oldSource: './tests/a.csv',
                newSource: './tests/b.csv',
                keys: ['id'],
            }).to({
                stream: new NullOutputStream(),
            });
        });
        test('should work with explicit null output stream', () => {
            const ctx = diff({
                oldSource: './tests/a.csv',
                newSource: './tests/b.csv',
                keys: ['id'],
            }).to({
                stream: 'null',
            });
        });
        test('should work with explicit null console stream', () => {
            const ctx = diff({
                oldSource: './tests/a.csv',
                newSource: './tests/b.csv',
                keys: ['id'],
            }).to({
                stream: 'console',
            });
        });
    });
});


