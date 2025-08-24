import { expect } from 'chai';
import fs from 'fs';
import path from 'path';
import { processSalesData, SaleRecord, AggregatedSales, processBatch, writeAggregatedData } from '../src/process_sales';
import { parse, CsvError } from 'csv-parse';

describe('Sales Data Processor', () => {
    const testInputCsvPath = path.join(__dirname, 'test_sales_data.csv');
    const testOutputCsvPath = path.join(__dirname, 'test_yearly_monthly_sales.csv');

    beforeEach(() => {
        // Clean up previous test files if any
        if (fs.existsSync(testInputCsvPath)) {
            fs.unlinkSync(testInputCsvPath);
        }
        if (fs.existsSync(testOutputCsvPath)) {
            fs.unlinkSync(testOutputCsvPath);
        }
    });

    afterEach(() => {
        // Clean up test files
        if (fs.existsSync(testInputCsvPath)) {
            fs.unlinkSync(testInputCsvPath);
        }
        if (fs.existsSync(testOutputCsvPath)) {
            fs.unlinkSync(testOutputCsvPath);
        }
    });

    it('should correctly process a small CSV file', async () => {
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

        expect(records).to.have.lengthOf(3); // 2022-12, 2023-01, 2023-02

        const rec2022_12 = records.find(r => r.year === '2022' && r.month === '12');
        expect(rec2022_12).to.exist;
        expect(rec2022_12.numberOfSales).to.equal('1');
        expect(rec2022_12.maxTotal).to.equal('50.00');
        expect(rec2022_12.minTotal).to.equal('50.00');
        expect(rec2022_12.averageTotal).to.equal('50.00');
        expect(rec2022_12.standardDeviation).to.equal('0.00');

        const rec2023_01 = records.find(r => r.year === '2023' && r.month === '1');
        expect(rec2023_01).to.exist;
        expect(rec2023_01.numberOfSales).to.equal('3');
        expect(rec2023_01.maxTotal).to.equal('300.00');
        expect(rec2023_01.minTotal).to.equal('100.00');
        // (100 + 200 + 300) / 3 = 600 / 3 = 200
        expect(rec2023_01.averageTotal).to.equal('200.00');
        // For std dev: values are 100, 200, 300. Mean is 200.
        // (100-200)^2 + (200-200)^2 + (300-200)^2 = (-100)^2 + 0^2 + (100)^2 = 10000 + 0 + 10000 = 20000
        // Variance = 20000 / 3 = 6666.666...
        // Std Dev = sqrt(6666.666...) = 81.6496...
        expect(rec2023_01.standardDeviation).to.equal('81.65');

        const rec2023_02 = records.find(r => r.year === '2023' && r.month === '2');
        expect(rec2023_02).to.exist;
        expect(rec2023_02.numberOfSales).to.equal('2');
        expect(rec2023_02.maxTotal).to.equal('250.00');
        expect(rec2023_02.minTotal).to.equal('150.00');
        // (150 + 250) / 2 = 400 / 2 = 200
        expect(rec2023_02.averageTotal).to.equal('200.00');
        // For std dev: values are 150, 250. Mean is 200.
        // (150-200)^2 + (250-200)^2 = (-50)^2 + (50)^2 = 2500 + 2500 = 5000
        // Variance = 5000 / 2 = 2500
        // Std Dev = sqrt(2500) = 50
        expect(rec2023_02.standardDeviation).to.equal('50.00');
    });

    it('should handle an empty CSV file', async () => {
        const csvContent = 'ID,order_id,customer_id,total,fecha';
        fs.writeFileSync(testInputCsvPath, csvContent);

        await processSalesData(testInputCsvPath, testOutputCsvPath);

        const outputContent = fs.readFileSync(testOutputCsvPath, 'utf-8');
        const records = await new Promise<any[]>((resolve, reject) => {
            parse(outputContent, { columns: true, skip_empty_lines: true }, (err: CsvError | undefined, records: any[]) => {
                if (err) reject(err); else resolve(records);
            });
        });

        expect(records).to.have.lengthOf(0);
    });

    it('should handle records with same year and month', async () => {
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

        expect(records).to.have.lengthOf(1);
        const rec2023_01 = records.find(r => r.year === '2023' && r.month === '1');
        expect(rec2023_01).to.exist;
        expect(rec2023_01.numberOfSales).to.equal('3');
        expect(rec2023_01.maxTotal).to.equal('300.00');
        expect(rec2023_01.minTotal).to.equal('100.00');
        expect(rec2023_01.averageTotal).to.equal('200.00');
        expect(rec2023_01.standardDeviation).to.equal('81.65');
    });

    it('should correctly calculate for a single record', async () => {
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

        expect(records).to.have.lengthOf(1);
        const rec2023_01 = records.find(r => r.year === '2023' && r.month === '1');
        expect(rec2023_01).to.exist;
        expect(rec2023_01.numberOfSales).to.equal('1');
        expect(rec2023_01.maxTotal).to.equal('100.00');
        expect(rec2023_01.minTotal).to.equal('100.00');
        expect(rec2023_01.averageTotal).to.equal('100.00');
        expect(rec2023_01.standardDeviation).to.equal('0.00');
    });

});
