import { Column, defaultRowComparer, JsonFormatReader, parseCsvLine, serializeRowAsCsvLine } from "./formats";
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
        test('empty string should fail', async () => {
            const stream = new ArrayInputStream([
                '',
            ]);
            const reader = new JsonFormatReader({ stream });
            await reader.open();
            await expect(async () => {
                await reader.readHeader();
            }).rejects.toThrowError('Expected to find at least one object');
        });        
        test('empty stream should fail', async () => {
            const stream = new ArrayInputStream([]);
            const reader = new JsonFormatReader({ stream });
            await reader.open();
            await expect(async () => {
                await reader.readHeader();
            }).rejects.toThrowError('Expected to find at least one object');
        });        
        test('row should contain an object or fail, while reading the header', async () => {
            const stream = new ArrayInputStream([
                '123',
            ]);
            const reader = new JsonFormatReader({ stream });
            await reader.open();
            await expect(async () => {
                await reader.readHeader();
            }).rejects.toThrowError('Expected to find a JSON object');
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
            }).rejects.toThrowError('Expected to find a JSON object');
        });
        test('should convert object values to string', async () => {
            const stream = new ArrayInputStream([
                '[{"id": 1,"a":"a1","b":true,"c":3.14}]',
            ]);
            const reader = new JsonFormatReader({ stream });
            await reader.open();
            const header = await reader.readHeader();
            expect(header.columns).toEqual(['id','a','b','c']);
            const row1 = await reader.readRow();
            expect(row1).toEqual(['1', 'a1', 'true', '3.14']);
            const done = await reader.readRow();
            expect(done).toBeUndefined();
            await reader.close();
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

});