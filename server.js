import express from 'express';
import { Client } from '@elastic/elasticsearch';
import bodyParser from 'body-parser';
import fs from 'fs';
import csv from 'csv-parser';
import cors from 'cors';

const app = express();
const client = new Client({ node: 'http://localhost:9200' });
app.use(cors());
app.use(bodyParser.json());

// Create a collection (index)
app.post('0000/:collectionName', async (req, res) => {
    const { collectionName } = req.params;
    try {
        await client.indices.create({
            index: collectionName,
        });
        res.status(201).send({ message: `Collection ${collectionName} created successfully` });
    } catch (error) {
        console.error('Error creating collection:', error);
        res.status(500).send(error);
    }
});

// Index employee data excluding a specified column
app.post('/index-data/:collectionName/:excludeColumn', async (req, res) => {
    const { collectionName, excludeColumn } = req.params;
    const results = [];

    fs.createReadStream('./employees.csv')
        .pipe(csv())
        .on('data', (data) => results.push(data))
        .on('end', async () => {
            try {
                for (const employee of results) {
                    // Remove the excluded column
                    delete employee[excludeColumn];
                    await client.index({
                        index: collectionName,
                        document: employee,
                    });
                }
                res.status(201).send({ message: `Data indexed into ${collectionName} excluding ${excludeColumn}` });
            } catch (error) {
                console.error('Error indexing document:', error);
                res.status(500).send(error);
            }
        });
});

// Search by column value
app.get('/search-by-column/:collectionName/:columnName/:columnValue', async (req, res) => {
    const { collectionName, columnName, columnValue } = req.params;
    try {
        const result = await client.search({
            index: collectionName,
            query: {
                match: { [columnName]: columnValue }
            }
        });
        res.status(200).send(result.hits.hits);
    } catch (error) {
        console.error('Error searching by column:', error);
        res.status(500).send(error);
    }
});

// Get employee count
app.get('/employee-count/:collectionName', async (req, res) => {
    const { collectionName } = req.params;
    try {
        const count = await client.count({ index: collectionName });
        res.status(200).send({ count: count.count });
    } catch (error) {
        console.error('Error getting employee count:', error);
        res.status(500).send(error);
    }
});

// Delete an employee by ID
app.delete('/delete-employee/:collectionName/:employeeId', async (req, res) => {
    const { collectionName, employeeId } = req.params;
    try {
        await client.delete({
            index: collectionName,
            id: employeeId
        });
        res.status(200).send({ message: `Employee ${employeeId} deleted from ${collectionName}` });
    } catch (error) {
        console.error('Error deleting employee:', error);
        res.status(500).send(error);
    }
});

// Get department facets (group by department)
app.get('/department-facets/:collectionName', async (req, res) => {
    const { collectionName } = req.params;
    try {
        const result = await client.search({
            index: collectionName,
            size: 0,
            aggs: {
                department_count: {
                    terms: {
                        field: 'Department.keyword'
                    }
                }
            }
        });
        res.status(200).send(result.aggregations.department_count.buckets);
    } catch (error) {
        console.error('Error getting department facets:', error);
        res.status(500).send(error);
    }
});

// Start the server
const PORT = process.env.PORT || 5000;
app.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
});
