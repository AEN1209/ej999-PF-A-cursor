import assert from 'assert';
import fs from 'fs';
import path from 'path';
import { processSalesData, SaleRecord, AggregatedSales, processBatch, writeAggregatedData } from '../src/process_sales';
import { parse, CsvError } from 'csv-parse';

const testInputCsvPath = path.join(__dirname, 'test_sales_data.csv');
const testOutputCsvPath = path.join(__dirname, 'test_yearly_monthly_sales.csv');

async function runTest(name: string, testFunction: () => Promise<void>) {
    console.log(`Running test: ${name}`);
    try {
        await testFunction();
        console.log(`✔ Test passed: ${name}`);
    } catch (error) {
        console.error(`✖ Test failed: ${name}`);
        console.error(error);
        process.exit(1);
    }
}

function setupTestEnvironment() {
    if (fs.existsSync(testInputCsvPath)) {
        fs.unlinkSync(testInputCsvPath);
    }
    if (fs.existsSync(testOutputCsvPath)) {
        fs.unlinkSync(testOutputCsvPath);
    }
}

async function testSmallCsvFile() {
    setupTestEnvironment();
    const csvContent = [
        'ID,order_id,customer_id,total,fecha',
        '1,101,1,100.00,2023-01-15T10:00:00.000Z',
        '2,102,1,200.00,2023-01-20T11:00:00.000Z',
        '3,103,2,150.00,2023-02-01T12:00:00.000Z',
        '4,104,3,250.00,2023-02-10T13:00:00.000Z',
        '5,105,1,50.00,2022-12-25T09:00:00.000Z',
        '6,106,2,300.00,2023-01-05T08:00:00.000Z'
    ].join('\n');

    fs.writeFileSync(testInputCsvPath, csvContent);

    await processSalesData(testInputCsvPath, testOutputCsvPath);

    const outputContent = fs.readFileSync(testOutputCsvPath, 'utf-8');
    const records = await new Promise<any[]>((resolve, reject) => {
        parse(outputContent, { columns: true, skip_empty_lines: true }, (err: CsvError | undefined, records: any[]) => {
            if (err) reject(err); else resolve(records);
        });
    });

    assert.strictEqual(records.length, 3, 'Should have 3 aggregated records'); // 2022-12, 2023-01, 2023-02

    const rec2022_12 = records.find((r: any) => r.year === '2022' && r.month === '12');
    assert.ok(rec2022_12, 'Record for 2022-12 should exist');
    assert.strictEqual(rec2022_12.numberOfSales, '1');
    assert.strictEqual(rec2022_12.maxTotal, '50.00');
    assert.strictEqual(rec2022_12.minTotal, '50.00');
    assert.strictEqual(rec2022_12.averageTotal, '50.00');
    assert.strictEqual(rec2022_12.standardDeviation, '0.00');

    const rec2023_01 = records.find((r: any) => r.year === '2023' && r.month === '1');
    assert.ok(rec2023_01, 'Record for 2023-01 should exist');
    assert.strictEqual(rec2023_01.numberOfSales, '3');
    assert.strictEqual(rec2023_01.maxTotal, '300.00');
    assert.strictEqual(rec2023_01.minTotal, '100.00');
    assert.strictEqual(rec2023_01.averageTotal, '200.00');
    assert.strictEqual(rec2023_01.standardDeviation, '81.65');

    const rec2023_02 = records.find((r: any) => r.year === '2023' && r.month === '2');
    assert.ok(rec2023_02, 'Record for 2023-02 should exist');
    assert.strictEqual(rec2023_02.numberOfSales, '2');
    assert.strictEqual(rec2023_02.maxTotal, '250.00');
    assert.strictEqual(rec2023_02.minTotal, '150.00');
    assert.strictEqual(rec2023_02.averageTotal, '200.00');
    assert.strictEqual(rec2023_02.standardDeviation, '50.00');
}

async function testEmptyCsvFile() {
    setupTestEnvironment();
    const csvContent = 'ID,order_id,customer_id,total,fecha';
    fs.writeFileSync(testInputCsvPath, csvContent);

    await processSalesData(testInputCsvPath, testOutputCsvPath);

    const outputContent = fs.readFileSync(testOutputCsvPath, 'utf-8');
    const records = await new Promise<any[]>((resolve, reject) => {
        parse(outputContent, { columns: true, skip_empty_lines: true }, (err: CsvError | undefined, records: any[]) => {
            if (err) reject(err); else resolve(records);
        });
    });

    assert.strictEqual(records.length, 0, 'Should have 0 aggregated records for empty file');
}

async function testSameYearMonthRecords() {
    setupTestEnvironment();
    const csvContent = [
        'ID,order_id,customer_id,total,fecha',
        '1,101,1,100.00,2023-01-15T10:00:00.000Z',
        '2,102,2,200.00,2023-01-16T11:00:00.000Z',
        '3,103,3,300.00,2023-01-17T12:00:00.000Z'
    ].join('\n');

    fs.writeFileSync(testInputCsvPath, csvContent);

    await processSalesData(testInputCsvPath, testOutputCsvPath);

    const outputContent = fs.readFileSync(testOutputCsvPath, 'utf-8');
    const records = await new Promise<any[]>((resolve, reject) => {
        parse(outputContent, { columns: true, skip_empty_lines: true }, (err: CsvError | undefined, records: any[]) => {
            if (err) reject(err); else resolve(records);
        });
    });

    assert.strictEqual(records.length, 1, 'Should have 1 aggregated record for same year/month');
    const rec2023_01 = records.find((r: any) => r.year === '2023' && r.month === '1');
    assert.ok(rec2023_01, 'Record for 2023-01 should exist');
    assert.strictEqual(rec2023_01.numberOfSales, '3');
    assert.strictEqual(rec2023_01.maxTotal, '300.00');
    assert.strictEqual(rec2023_01.minTotal, '100.00');
    assert.strictEqual(rec2023_01.averageTotal, '200.00');
    assert.strictEqual(rec2023_01.standardDeviation, '81.65');
}

async function testSingleRecord() {
    setupTestEnvironment();
    const csvContent = [
        'ID,order_id,customer_id,total,fecha',
        '1,101,1,100.00,2023-01-15T10:00:00.000Z'
    ].join('\n');

    fs.writeFileSync(testInputCsvPath, csvContent);

    await processSalesData(testInputCsvPath, testOutputCsvPath);

    const outputContent = fs.readFileSync(testOutputCsvPath, 'utf-8');
    const records = await new Promise<any[]>((resolve, reject) => {
        parse(outputContent, { columns: true, skip_empty_lines: true }, (err: CsvError | undefined, records: any[]) => {
            if (err) reject(err); else resolve(records);
        });
    });

    assert.strictEqual(records.length, 1, 'Should have 1 aggregated record for single record');
    const rec2023_01 = records.find((r: any) => r.year === '2023' && r.month === '1');
    assert.ok(rec2023_01, 'Record for 2023-01 should exist');
    assert.strictEqual(rec2023_01.numberOfSales, '1');
    assert.strictEqual(rec2023_01.maxTotal, '100.00');
    assert.strictEqual(rec2023_01.minTotal, '100.00');
    assert.strictEqual(rec2023_01.averageTotal, '100.00');
    assert.strictEqual(rec2023_01.standardDeviation, '0.00');
}

async function main() {
    await runTest('should correctly process a small CSV file', testSmallCsvFile);
    await runTest('should handle an empty CSV file', testEmptyCsvFile);
    await runTest('should handle records with same year and month', testSameYearMonthRecords);
    await runTest('should correctly calculate for a single record', testSingleRecord);
}

main();
