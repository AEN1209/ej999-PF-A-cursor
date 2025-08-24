"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.OUTPUT_CSV_PATH = exports.INPUT_CSV_PATH = void 0;
exports.processSalesData = processSalesData;
exports.processBatch = processBatch;
exports.writeAggregatedData = writeAggregatedData;
const fs_1 = __importDefault(require("fs"));
const path_1 = __importDefault(require("path"));
const csv_parse_1 = require("csv-parse");
const csv_stringify_1 = require("csv-stringify");
const INPUT_CSV_PATH = path_1.default.join(__dirname, '..' + path_1.default.sep + '..' + path_1.default.sep + 'sales_data.csv');
exports.INPUT_CSV_PATH = INPUT_CSV_PATH;
const OUTPUT_CSV_PATH = path_1.default.join(__dirname, '..' + path_1.default.sep + '..' + path_1.default.sep + 'yearly_monthly_sales.csv');
exports.OUTPUT_CSV_PATH = OUTPUT_CSV_PATH;
const BATCH_SIZE = 10000;
async function processSalesData(inputPath, outputPath) {
    return new Promise((resolve, reject) => {
        const aggregatedData = new Map();
        let recordsBuffer = [];
        let recordCount = 0;
        const parser = fs_1.default.createReadStream(inputPath)
            .pipe(csv_parse_1.parse({
            columns: true,
            skip_empty_lines: true,
            cast: (value, context) => {
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
        parser.on('readable', function () {
            let record;
            while ((record = parser.read()) !== null) {
                recordsBuffer.push(record);
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
        parser.on('error', (err) => {
            console.error('Error reading CSV:', err);
            reject(err);
        });
    });
}
function processBatch(batch, aggregatedData) {
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
        const data = aggregatedData.get(key);
        data.numberOfSales++;
        data.sumTotal += record.total;
        data.sumOfSquares += record.total * record.total;
        data.maxTotal = Math.max(data.maxTotal, record.total);
        data.minTotal = Math.min(data.minTotal, record.total);
    }
}
async function writeAggregatedData(aggregatedData, outputPath) {
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
    const stringifier = (0, csv_stringify_1.stringify)({
        header: true,
        columns: ['year', 'month', 'numberOfSales', 'maxTotal', 'minTotal', 'averageTotal', 'standardDeviation']
    });
    const writableStream = fs_1.default.createWriteStream(outputPath);
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
//# sourceMappingURL=process_sales.js.map