import fs from 'fs';
import {describe, expect, test} from '@jest/globals';
import { defaultRowComparer, Differ, parseCsvLine, RowHeader, serializeRowAsCsvLine, DifferOptions, FileOutputStream, ArrayInputStream, FileInputStream, RowDiff, StreamWriter, DiffStats, OutputStream, StreamWriterFooter, StreamWriterHeader } from './differ';

class FakeOutputWriter implements StreamWriter{
    public header?: StreamWriterHeader;
    public diffs: RowDiff[] = [];
    public footer?: StreamWriterFooter;

    open(): void {}
    writeHeader(header: StreamWriterHeader): void {
        this.header = header;
    }
    writeDiff(diff: RowDiff): void {
        this.diffs.push(diff);
    }
    writeFooter(footer: StreamWriterFooter): void {
       this.footer = footer;
    }
    close(): void {}
}

type DiffOptions = Omit<DifferOptions, "oldSource" | "newSource" | "output" | "outputWriter"> & {
    oldLines: string[],
    newLines: string[], 
    keepSameRows?: boolean,
    changeLimit?: number,
};

function diff(options: DiffOptions): FakeOutputWriter {
    const result = new FakeOutputWriter();
    const differ = new Differ({
        ...options,
        oldSource: { 
            stream: new ArrayInputStream(options.oldLines) 
        },
        newSource: {
            stream: new ArrayInputStream(options.newLines),
        },
        output: {
            format: (_options) => result,
            keepSameRows: options.keepSameRows,
            changeLimit: options.changeLimit,
        }
    });
    differ.execute();
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
            expect(() => defaultRowComparer([], undefined, undefined)).toThrowError('Expected to have at least one key in keys parameter');
        });
        describe('undefined rows', () => {
            const keys: RowHeader[] = [{
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
        describe('single field', () => {
            const keys: RowHeader[] = [{
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
        describe('2 fields', () => {
            const keys: RowHeader[] = [
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
            expect(() => diff({
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
                keyFields: ['ID'],
            })).toThrowError('Expected rows to be in ascending order in new source but received: previous=3,dave,44, current=2,rachel,22');
        });        
        test('should detect invalid ordering in descending mode', () => {
            expect(() => diff({
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
                keyFields: ['ID'],
                descendingOrder: true,
            })).toThrowError('Expected rows to be in descending order in new source but received: previous=1,john,33, current=2,rachel,22');            
        });        
        test('should be able to execute twice', () => {
            const differ = new Differ({
                oldSource: './tests/a.csv',
                newSource: './tests/b.csv',
                output: 'null',
                keyFields: ['id'],
            });
            const stats1 = differ.execute();
            const stats2 = differ.execute();
            expect(stats1.totalChanges).toBe(6);
            expect(stats1).toEqual(stats2);
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
        test('should have headers in old source', () => {
            expect(() => diff({
                oldLines: [
                ],
                newLines: [
                    'ID,NAME,AGE',
                ],
                keyFields: ['ID'],
            })).toThrowError('Expected to find headers in old source');
        });
        test('should have headers in new source', () => {
            expect(() => diff({
                oldLines: [
                    'ID,NAME,AGE',
                ],
                newLines: [
                ],
                keyFields: ['ID'],
            })).toThrowError('Expected to find headers in new source');            
        });
        test('should match headers in both sources', () => {
            expect(() => diff({
                oldLines: [
                    'ID,NAME,AGE',
                ],
                newLines: [
                    'ID,TITLE,AGE',
                ],
                keyFields: ['ID'],
            })).toThrowError(`Could not find new header 'TITLE' in old headers:
old=ID,NAME,AGE
new=ID,TITLE,AGE`);            
        });
        test('should find keys in old headers', () => {
            expect(() => diff({
                oldLines: [
                    'CODE,NAME,AGE',
                ],
                newLines: [
                    'ID,NAME,AGE',
                ],
                keyFields: ['ID'],
            })).toThrowError(`Could not find key 'ID' in old headers: CODE,NAME,AGE`);            
        });
        test('should find keys in new headers', () => {
            expect(() => diff({
                oldLines: [
                    'ID,NAME,AGE',
                    'a1,a,33',
                ],
                newLines: [
                    'CODE,NAME,AGE',
                    'a1,a,33',
                ],
                keyFields: ['ID'],
            })).toThrowError(`Could not find key 'ID' in new headers: CODE,NAME,AGE`);            
        });
    });
    describe('changes', () => {        
        test('both files are empty', () => {
            const res = diff({
                oldLines: [
                    'ID,NAME,AGE',
                ],
                newLines: [
                    'ID,NAME,AGE',
                ],
                keyFields: ['ID'],
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
            const res = diff({
                oldLines: [
                    'ID,NAME,AGE',
                ],
                newLines: [
                    'ID,NAME,AGE',
                    '1,john,33',
                    '2,rachel,22',
                ],
                keyFields: ['ID'],
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
            const res = diff({
                oldLines: [
                    'ID,NAME,AGE',
                    '1,john,33',
                    '2,rachel,22',
                ],
                newLines: [
                    'ID,NAME,AGE',
                ],
                keyFields: ['ID'],
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
            const res = diff({
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
                keyFields: ['ID'],
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
            const res = diff({
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
                keyFields: ['ID'],
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
            const res = diff({
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
                keyFields: ['ID'],
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
            const res = diff({
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
                keyFields: ['ID'],
                excludedFields: ['AGE'],
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
            const res = diff({
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
                keyFields: ['ID'],
                includedFields: ['ID', 'NAME'],
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
            const res = diff({
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
                keyFields: ['ID'],
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
            const res = diff({
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
                keyFields: ['ID'],
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
            const res = diff({
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
                keyFields: ['ID'],
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
            const res = diff({
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
                keyFields: ['ID'],
                excludedFields: ['AGE'],
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
        test('1 modified with included columns', () => {
            const res = diff({
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
                keyFields: ['ID'],
                includedFields: ['ID', 'NAME'],
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
        test('1 deleted', () => {
            const res = diff({
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
                keyFields: ['ID'],
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
            const res = diff({
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
                keyFields: ['ID'],
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
            const res = diff({
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
                keyFields: ['ID'],
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
            const res = diff({
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
                keyFields: ['ID'],
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
            const res = diff({
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
                keyFields: ['ID'],
                keepSameRows: true,
                descendingOrder: true,
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
        test('keep first 2 changes', () => {
            const res = diff({
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
                keyFields: ['ID'],
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
            const differ = new Differ({
                oldSource: './tests/a.csv',
                newSource: './tests/b.csv',
                output: {
                    format: (_options) => output,
                },
                keyFields: ['id'],
            });
            differ.execute();
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
                output: {
                    format: (_options) => output,
                },
                keyFields: ['id'],
            });
            differ.execute();
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
            const differ = new Differ({
                oldSource: './tests/a.csv',
                newSource: './tests/b.csv',
                output: './output/files/output.csv',
                keyFields: ['id'],
            });
            const stats = differ.execute();
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
        test('should produce a tsv file', () => {
            const differ = new Differ({
                oldSource: {
                    stream: './tests/a.tsv',
                    delimiter: '\t',
                },
                newSource: {
                    stream: './tests/b.tsv',
                    delimiter: '\t',
                },
                output: {
                    stream: './output/files/output.tsv',
                    delimiter: '\t',
                },
                keyFields: ['id'],
            });
            const stats = differ.execute();
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
            const differ = new Differ({
                oldSource: {
                    stream: './tests/a.csv',
                },
                newSource: {
                    stream: './tests/b.tsv',
                    delimiter: '\t',
                },
                output: {
                    stream: './output/files/output.tsv',
                    delimiter: '\t',
                },
                keyFields: ['id'],
            });
            const stats = differ.execute();
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
            const differ = new Differ({
                oldSource: './tests/a.csv',
                newSource: './tests/b.csv',
                output: {
                    stream: './output/files/output.json',
                    format: 'json',
                },
                keyFields: ['id'],
            });
            const stats = differ.execute();
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
            const differ = new Differ({
                oldSource: './tests/a.csv',
                newSource: './tests/b.csv',
                output: {
                    stream: './output/files/output.json',
                    format: 'json',
                    keepOldValues: true,
                },
                keyFields: ['id'],
            });
            const stats = differ.execute();
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
            const differ = new Differ({
                oldSource: './tests/a.csv',
                newSource: './tests/b.csv',
                output: {
                    stream: './output/files/output.json',
                    format: 'json',
                    labels: {
                        generatedAt: '2023-01-02T01:21:57Z',
                        source: 'Some source...'
                    }
                },
                keyFields: ['id'],
            });
            const stats = differ.execute();
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
        test('should display output on the console', () => {
            const differ = new Differ({
                oldSource: './tests/a.csv',
                newSource: './tests/b.csv',
                output: 'console',
                keyFields: ['id'],
            });
            const stats = differ.execute();
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
            const differ = new Differ({
                oldSource: './tests/a.csv',
                newSource: './tests/b.csv',
                output: 'null',
                keyFields: ['id'],
            });
            const stats = differ.execute();
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
    });
});


