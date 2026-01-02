import { BufferedFormatReader, Column, defaultRowComparer, IterableFormatReader, JsonFormatReader, JsonFormatWriter, numberComparer, parseCsvLine, serializeRowAsCsvLine } from "./formats";
import { ArrayInputStream } from "./streams";

describe('formats', () => {

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
        test('single compact row', async () => {
            const stream = new ArrayInputStream([
                '[{"id": "1","a":"a1","b":"b1","c":"c1"}]',
            ]);
            const reader = new JsonFormatReader({ stream });
            await reader.open();
            const header = await reader.readHeader();
            expect(header.columns).toEqual(['id','a','b','c']);
            const row1 = await reader.readRow();
            expect(row1).toEqual(['1', 'a1', 'b1', 'c1']);
            const done = await reader.readRow();
            expect(done).toBeUndefined();
            await reader.close();
        });
        test('single indented row', async () => {
            const stream = new ArrayInputStream([
                '[',
                '  {"id": "1","a":"a1","b":"b1","c":"c1"}',
                ']'
            ]);
            const reader = new JsonFormatReader({ stream });
            await reader.open();
            const header = await reader.readHeader();
            expect(header.columns).toEqual(['id','a','b','c']);
            const row1 = await reader.readRow();
            expect(row1).toEqual(['1', 'a1', 'b1', 'c1']);
            const done = await reader.readRow();
            expect(done).toBeUndefined();
            await reader.close();
        });
        test('inlined brackets, with trailing comma', async () => {
            const stream = new ArrayInputStream([
                '[{"id": "1","a":"a1","b":"b1","c":"c1"},',
                '{"id": "2","a":"a2","b":"b2","c":"c2"},',
                '{"id": "3","a":"a3","b":"b3","c":"c3"}]',
            ]);
            const reader = new JsonFormatReader({ stream });
            await reader.open();
            const header = await reader.readHeader();
            expect(header.columns).toEqual(['id','a','b','c']);
            const row1 = await reader.readRow();
            expect(row1).toEqual(['1', 'a1', 'b1', 'c1']);
            const row2 = await reader.readRow();
            expect(row2).toEqual(['2', 'a2', 'b2', 'c2']);
            const row3 = await reader.readRow();
            expect(row3).toEqual(['3', 'a3', 'b3', 'c3']);
            const done = await reader.readRow();
            expect(done).toBeUndefined();
            await reader.close();
        });
        test('outlined brackets, with trailing comma', async () => {
            const stream = new ArrayInputStream([
                '[',
                '  {"id": "1","a":"a1","b":"b1","c":"c1"},',
                '  {"id": "2","a":"a2","b":"b2","c":"c2"},',
                '  {"id": "3","a":"a3","b":"b3","c":"c3"}',
                ']',
            ]);
            const reader = new JsonFormatReader({ stream });
            await reader.open();
            const header = await reader.readHeader();
            expect(header.columns).toEqual(['id','a','b','c']);
            const row1 = await reader.readRow();
            expect(row1).toEqual(['1', 'a1', 'b1', 'c1']);
            const row2 = await reader.readRow();
            expect(row2).toEqual(['2', 'a2', 'b2', 'c2']);
            const row3 = await reader.readRow();
            expect(row3).toEqual(['3', 'a3', 'b3', 'c3']);
            const done = await reader.readRow();
            expect(done).toBeUndefined();
            await reader.close();
        });
        test('inlined brackets, with preceding comma', async () => {
            const stream = new ArrayInputStream([
                '[{"id": "1","a":"a1","b":"b1","c":"c1"}',
                ',{"id": "2","a":"a2","b":"b2","c":"c2"}',
                ',{"id": "3","a":"a3","b":"b3","c":"c3"}]',
            ]);
            const reader = new JsonFormatReader({ stream });
            await reader.open();
            const header = await reader.readHeader();
            expect(header.columns).toEqual(['id','a','b','c']);
            const row1 = await reader.readRow();
            expect(row1).toEqual(['1', 'a1', 'b1', 'c1']);
            const row2 = await reader.readRow();
            expect(row2).toEqual(['2', 'a2', 'b2', 'c2']);
            const row3 = await reader.readRow();
            expect(row3).toEqual(['3', 'a3', 'b3', 'c3']);
            const done = await reader.readRow();
            expect(done).toBeUndefined();
            await reader.close();
        });
        test('outlined brackets, with preceding comma', async () => {
            const stream = new ArrayInputStream([
                '[',
                '  {"id": "1","a":"a1","b":"b1","c":"c1"}',
                '  ,{"id": "2","a":"a2","b":"b2","c":"c2"}',
                '  ,{"id": "3","a":"a3","b":"b3","c":"c3"}',
                ']',
            ]);
            const reader = new JsonFormatReader({ stream });
            await reader.open();
            const header = await reader.readHeader();
            expect(header.columns).toEqual(['id','a','b','c']);
            const row1 = await reader.readRow();
            expect(row1).toEqual(['1', 'a1', 'b1', 'c1']);
            const row2 = await reader.readRow();
            expect(row2).toEqual(['2', 'a2', 'b2', 'c2']);
            const row3 = await reader.readRow();
            expect(row3).toEqual(['3', 'a3', 'b3', 'c3']);
            const done = await reader.readRow();
            expect(done).toBeUndefined();
            await reader.close();
        });
        test('reading a closed stream should fail', async () => {
            const stream = new ArrayInputStream([
                '',
            ]);
            const reader = new JsonFormatReader({ stream });
            await expect(async () => {
                await reader.readHeader();
            }).rejects.toThrow('Cannot access textReader because stream is not open');
        });        
        test('writing to a closed stream should fail', async () => {
            const writer = new JsonFormatWriter({ stream: './output/files/output.json' });
            await expect(async () => {
                await writer.writeHeader({columns: ['id', 'name']});
            }).rejects.toThrow('Cannot access textWriter because stream is not open');
        });        
        test('empty string should fail', async () => {
            const stream = new ArrayInputStream([
                '',
            ]);
            const reader = new JsonFormatReader({ stream });
            await reader.open();
            await expect(async () => {
                await reader.readHeader();
            }).rejects.toThrow('Expected to find at least one object');
        });        
        test('empty stream should fail', async () => {
            const stream = new ArrayInputStream([]);
            const reader = new JsonFormatReader({ stream });
            await reader.open();
            await expect(async () => {
                await reader.readHeader();
            }).rejects.toThrow('Expected to find at least one object');
        });        
        test('row should contain an object or fail, while reading the header', async () => {
            const stream = new ArrayInputStream([
                '123',
            ]);
            const reader = new JsonFormatReader({ stream });
            await reader.open();
            await expect(async () => {
                await reader.readHeader();
            }).rejects.toThrow('Expected to find a JSON object');
        });        
        test('row should contain an object or fail, while reading the header', async () => {
            const stream = new ArrayInputStream([
                '[',
                '  {"id": "1","a":"a1","b":"b1","c":"c1"},',
                '  123,',
                '  {"id": "3","a":"a3","b":"b3","c":"c3"}',
                ']',
            ]);
            const reader = new JsonFormatReader({ stream });
            await reader.open();
            await reader.readHeader();
            const row1 = await reader.readRow();
            expect(row1).toEqual(['1', 'a1', 'b1', 'c1']);
            await expect(async () => {
                await reader.readRow();
            }).rejects.toThrow('Expected to find a JSON object');
        });
        test('should not convert object values to string', async () => {
            const stream = new ArrayInputStream([
                '[{"id": 1,"a":"a1","b":true,"c":3.14,"d":null}]',
            ]);
            const reader = new JsonFormatReader({ stream });
            await reader.open();
            const header = await reader.readHeader();
            expect(header.columns).toEqual(['id','a','b','c', 'd']);
            const row1 = await reader.readRow();
            expect(row1).toEqual([1, 'a1', true, 3.14, null]);
            const done = await reader.readRow();
            expect(done).toBeUndefined();
            await reader.close();
        });

    });
    // TODO: write test for JsonFormatWriter and test row with non string values being properly serialized as json record
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
            expect(() => defaultRowComparer([], undefined, undefined)).toThrow('Expected to have at least one entry in the columns parameter');
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
        test('number comparison', () => {
            expect(numberComparer(null, null)).toBe(0);
            expect(numberComparer(1, null)).toBe(1);
            expect(numberComparer(null, 1)).toBe(-1);
            expect(numberComparer(1, '')).toBe(1);
            expect(numberComparer('', 1)).toBe(-1);
            expect(numberComparer(0, 0)).toBe(0);
            expect(numberComparer('0', 0)).toBe(0);
            expect(numberComparer(0, '0')).toBe(0);
            expect(numberComparer(1.1, 1.1)).toBe(0);
            expect(numberComparer(1.1, 1.2)).toBe(-1);
            expect(numberComparer(1.2, 1.1)).toBe(1);
            expect(numberComparer(-10, 0)).toBe(-1);
            expect(numberComparer(0, -10)).toBe(1);
            expect(numberComparer(0, 10)).toBe(-1);
            expect(numberComparer(10, 0)).toBe(1);
            expect(numberComparer(null, '')).toBe(-1);
            expect(numberComparer('', null)).toBe(1);
            expect(numberComparer(null, true)).toBe(-1);
            expect(numberComparer(true, null)).toBe(1);
            expect(numberComparer(true, true)).toBe(0);
            expect(numberComparer(true, false)).toBe(1);
            expect(numberComparer(false, true)).toBe(-1);
            expect(numberComparer(null, 'abc')).toBe(-1);
            expect(numberComparer('abc', null)).toBe(1);
            expect(numberComparer('1', '1')).toBe(0);
            expect(numberComparer('1', '2')).toBe(-1);
            expect(numberComparer('2', '1')).toBe(1);
            expect(numberComparer('2', '11')).toBe(-1);
            expect(numberComparer('11', '2')).toBe(1);
            expect(numberComparer('1.1', '1.1')).toBe(0);
            expect(numberComparer('1.1', '1.2')).toBe(-1);
            expect(numberComparer('1.2', '1.1')).toBe(1);
            expect(numberComparer('x1.1', 'x1.1')).toBe(0);
            expect(numberComparer('x1.1', '1.1')).toBe(-1);
            expect(numberComparer('1.1', 'x1.1')).toBe(1);
        });
    });
    describe('Iterable source', () => {
        test('should read all objects', async () => {
            const format = new IterableFormatReader({
                provider: someAsyncSource,
            });
            await format.open();
            const header = await format.readHeader();
            expect(header.columns).toEqual(['id', 'name', 'age']);
            const row1 = await format.readRow();
            expect(row1).toEqual([1, 'John', 33]);
            const row2 = await format.readRow();
            expect(row2).toEqual([2, 'Mary', 22]);
            const row3 = await format.readRow();
            expect(row3).toEqual([3, 'Cindy', 44]);
            const row4 = await format.readRow();
            expect(row4).toBeUndefined();
            await format.close();
        });
        test('should read first object', async () => {
            const format = new IterableFormatReader({
                provider: someAsyncSource,
            });
            await format.open();
            const header = await format.readHeader();
            expect(header.columns).toEqual(['id', 'name', 'age']);
            const row1 = await format.readRow();
            expect(row1).toEqual([1, 'John', 33]);
            await format.close();
        });
        test('should re-open', async () => {
            const format = new IterableFormatReader({
                provider: someAsyncSource,
            });
            await format.open();
            const header = await format.readHeader();
            expect(header.columns).toEqual(['id', 'name', 'age']);
            const row1 = await format.readRow();
            expect(row1).toEqual([1, 'John', 33]);
            await format.close();
            await format.open();
            const headerBis = await format.readHeader();
            expect(headerBis.columns).toEqual(['id', 'name', 'age']);
            const row1Bis = await format.readRow();
            expect(row1Bis).toEqual([1, 'John', 33]);
            await format.close();
        });
        test('should open first', async () => {
            const format = new IterableFormatReader({
                provider: someAsyncSource,
            });
            await expect(async () => {
                await format.readHeader();
            }).rejects.toThrow('You must call open before reading content!');
        });
        test('should open once', async () => {
            const format = new IterableFormatReader({
                provider: someAsyncSource,
            });
            await format.open();
            await expect(async () => {
                await format.open();
            }).rejects.toThrow('Reader is already open!')            
        });
        test('should not be empty', async () => {
            const format = new IterableFormatReader({
                provider: () => someAsyncSource(0),
            });
            await format.open();
            try {
                await expect(async () => {
                    await format.readHeader();
                }).rejects.toThrow('Expected to find at least one object');    
            } finally {
                format.close();
            }
        });
    });
    describe('BufferedFormatReader', () => {
        test('should peek rows', async () => {
            const format = new BufferedFormatReader(new IterableFormatReader({
                provider: someAsyncSource,
            }));
            await format.open();
            const header = await format.readHeader();
            expect(header.columns).toEqual(['id', 'name', 'age']);
            const row1 = await format.readRow();
            expect(row1).toEqual([1, 'John', 33]);
            const row2 = await format.peekRow();
            expect(row2).toEqual([2, 'Mary', 22]);
            const row2b = await format.peekRow();
            expect(row2b).toBe(row2);
            const row2c = await format.readRow();
            expect(row2c).toBe(row2);
            const row3 = await format.readRow();
            expect(row3).toEqual([3, 'Cindy', 44]);
            const row4 = await format.readRow();
            expect(row4).toBeUndefined();
            await format.close();
        });
    })
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

