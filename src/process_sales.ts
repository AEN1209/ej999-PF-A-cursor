import fs from 'fs';
import path from 'path';
import { parse } from 'csv-parse';
import { stringify } from 'csv-stringify';

interface SaleRecord {
    ID: number;
    order_id: number;
    customer_id: number;
    total: number;
    fecha: Date;
}

interface AggregatedSales {
    year: number;
    month: number;
    numberOfSales: number;
    maxTotal: number;
    minTotal: number;
    sumTotal: number; // Used for calculating average and standard deviation
    sumOfSquares: number; // Used for calculating standard deviation
}

const INPUT_CSV_PATH = path.join(__dirname, '..' + path.sep + '..' + path.sep + 'sales_data.csv');
const OUTPUT_CSV_PATH = path.join(__dirname, '..' + path.sep + '..' + path.sep + 'yearly_monthly_sales.csv');

const BATCH_SIZE = 10000;

async function processSalesData(inputPath: string, outputPath: string): Promise<void> {
    return new Promise((resolve, reject) => {
        const aggregatedData: Map<string, AggregatedSales> = new Map();
        let recordsBuffer: SaleRecord[] = [];
        let recordCount = 0;

        const parser = fs.createReadStream(inputPath)
            .pipe((parse as any)({
                columns: true,
                skip_empty_lines: true,
                cast: (value: string, context: { column: string, records: any[], index: number }) => {
                    if (context.column === 'ID' || context.column === 'order_id' || context.column === 'customer_id') {
                        return parseInt(value, 10);
                    }
                    if (context.column === 'total') {
                        return parseFloat(value);
                    }
                    if (context.column === 'fecha') {
                        return new Date(value);
                    }
                    return value;
                }
            }));

        parser.on('readable', function() {
            let record;
            while ((record = parser.read()) !== null) {
                recordsBuffer.push(record as SaleRecord);
                recordCount++;

                if (recordsBuffer.length >= BATCH_SIZE) {
                    processBatch(recordsBuffer, aggregatedData);
                    recordsBuffer = [];
                }
            }
        });

        parser.on('end', async () => {
            if (recordsBuffer.length > 0) {
                processBatch(recordsBuffer, aggregatedData);
            }
            await writeAggregatedData(aggregatedData, outputPath);
            console.log(`Processed ${recordCount} records and saved aggregated data to ${outputPath}`);
            resolve();
        });

        parser.on('error', (err: Error) => {
            console.error('Error reading CSV:', err);
            reject(err);
        });
    });
}

function processBatch(batch: SaleRecord[], aggregatedData: Map<string, AggregatedSales>): void {
    for (const record of batch) {
        const year = record.fecha.getFullYear();
        const month = record.fecha.getMonth() + 1; // getMonth() is 0-indexed
        const key = `${year}-${month}`;

        if (!aggregatedData.has(key)) {
            aggregatedData.set(key, {
                year,
                month,
                numberOfSales: 0,
                maxTotal: -Infinity,
                minTotal: Infinity,
                sumTotal: 0,
                sumOfSquares: 0,
            });
        }

        const data = aggregatedData.get(key)!;
        data.numberOfSales++;
        data.sumTotal += record.total;
        data.sumOfSquares += record.total * record.total;
        data.maxTotal = Math.max(data.maxTotal, record.total);
        data.minTotal = Math.min(data.minTotal, record.total);
    }
}

async function writeAggregatedData(aggregatedData: Map<string, AggregatedSales>, outputPath: string): Promise<void> {
    const records = Array.from(aggregatedData.values()).map(data => {
        const mean = data.sumTotal / data.numberOfSales;
        const variance = (data.sumOfSquares / data.numberOfSales) - (mean * mean);
        const stdDev = Math.sqrt(variance);

        return {
            year: data.year,
            month: data.month,
            numberOfSales: data.numberOfSales,
            maxTotal: data.maxTotal.toFixed(2),
            minTotal: data.minTotal.toFixed(2),
            averageTotal: mean.toFixed(2),
            standardDeviation: stdDev.toFixed(2),
        };
    });

    records.sort((a, b) => {
        if (a.year !== b.year) {
            return a.year - b.year;
        }
        return a.month - b.month;
    });

    const stringifier = stringify({
        header: true,
        columns: ['year', 'month', 'numberOfSales', 'maxTotal', 'minTotal', 'averageTotal', 'standardDeviation']
    });

    const writableStream = fs.createWriteStream(outputPath);
    stringifier.pipe(writableStream);

    for (const record of records) {
        stringifier.write(record);
    }
    stringifier.end();

    return new Promise((resolve, reject) => {
        writableStream.on('finish', resolve);
        writableStream.on('error', reject);
    });
}

if (require.main === module) {
    processSalesData(INPUT_CSV_PATH, OUTPUT_CSV_PATH)
        .catch(error => {
            console.error('Failed to process sales data:', error);
            process.exit(1);
        });
}

export { processSalesData, SaleRecord, AggregatedSales, processBatch, writeAggregatedData, INPUT_CSV_PATH, OUTPUT_CSV_PATH };
