'use strict';

const connectionStringParser = require('mssql/lib/connectionstring');
const { getClient, setClient, clearClient } = require('./connectionState');
const { getObjectsFromDatabase, getDatabaseCollationOption } = require('./databaseService/databaseService');
const {
	reverseCollectionsToJSON,
	mergeCollectionsWithViews,
	getCollectionsRelationships,
} = require('./reverseEngineeringService/reverseEngineeringService');
const logInfo = require('./helpers/logInfo');
const filterRelationships = require('./helpers/filterRelationships');
const getOptionsFromConnectionInfo = require('./helpers/getOptionsFromConnectionInfo');
const { adaptJsonSchema } = require('./helpers/adaptJsonSchema');

module.exports = {
	async connect(connectionInfo, logger, callback, app) {
		const client = getClient();
		if (!client) {
			await setClient(connectionInfo);
			return getClient();
		}

		return client;
	},

	disconnect(connectionInfo, logger, callback, app) {
		clearClient();
		callback();
	},

	async testConnection(connectionInfo, logger, callback, app) {
		try {
			logInfo('Test connection', connectionInfo, logger);
			await this.connect(connectionInfo);
			callback(null);
		} catch(error) {
			logger.log('error', { message: error.message, stack: error.stack, error }, 'Test connection');
			callback({ message: error.message, stack: error.stack });
		}
	},

	getDatabases(connectionInfo, logger, callback, app) {
		callback();
	},

	getDocumentKinds(connectionInfo, logger, callback, app) {
		callback();
	},

	async getDbCollectionsNames(connectionInfo, logger, callback, app) {
		try {
			logInfo('Retrieving databases and tables information', connectionInfo, logger);
			const client = await this.connect(connectionInfo);
			if (!client.config.database) {
				throw new Error('No database specified');
			}

			const objects = await getObjectsFromDatabase(client);
			const dbName = client.config.database;
            const collationData = (await getDatabaseCollationOption(client, dbName, logger)) || [];
			logInfo('Database collation: ', collationData[0], logger);
			callback(null, objects);
		} catch(error) {
			logger.log('error', { message: error.message, stack: error.stack, error }, 'Retrieving databases and tables information');
			callback({ message: error.message, stack: error.stack });
		}
	},

	async getDbCollectionsData(collectionsInfo, logger, callback, app) {
		try {
			logger.log('info', collectionsInfo, 'Retrieving schema', collectionsInfo.hiddenKeys);
			logger.progress({ message: 'Start reverse-engineering process', containerName: '', entityName: '' });
			const { collections } = collectionsInfo.collectionData;
			const client = getClient();
			if (!client.config.database) {
				throw new Error('No database specified');
			}

			const reverseEngineeringOptions = getOptionsFromConnectionInfo(collectionsInfo);
			const [jsonSchemas, relationships] = await Promise.all([
				await reverseCollectionsToJSON(logger)(client, collections, reverseEngineeringOptions),
				await getCollectionsRelationships(logger)(client, collections),
			]);
			callback(null, mergeCollectionsWithViews(jsonSchemas), null, filterRelationships(relationships, jsonSchemas));
		} catch (error) {
			logger.log('error', { message: error.message, stack: error.stack, error }, 'Reverse-engineering process failed');
			callback({ message: error.message, stack: error.stack })
		}
	},

	parseConnectionString({ connectionString = '' }, logger, callback) {
		try {
			const parsedConnectionStringData = connectionStringParser.resolve(connectionString);
			const parsedData = {
				databaseName: parsedConnectionStringData.database,
				host: parsedConnectionStringData.server,
				port: parsedConnectionStringData.port,
				authMethod: 'Username / Password',
				userName: parsedConnectionStringData.user,
				userPassword: parsedConnectionStringData.password
			}; 
			callback(null, { parsedData });
		} catch(err) {
			logger.log('error', { message: err.message, stack: err.stack, err }, 'Parsing connection string failed');
			callback({ message: err.message, stack: err.stack });
		}
	},

	adaptJsonSchema(data, logger, callback, app) {
		const formatError = error => {
			return Object.assign({ title: 'Adapt JSON Schema' }, Object.getOwnPropertyNames(error).reduce((accumulator, key) => {
				return Object.assign(accumulator, {
					[key]: error[key]
				});
			}, {}));
		};
		logger.log('info', 'Adaptation of JSON Schema started...', 'Adapt JSON Schema');
		try {
			const jsonSchema = JSON.parse(data.jsonSchema);
			const adaptedJsonSchema = adaptJsonSchema(app.require('lodash'), jsonSchema);
			
			logger.log('info', 'Adaptation of JSON Schema finished.', 'Adapt JSON Schema');

			callback(null, {
				jsonSchema: JSON.stringify(adaptedJsonSchema)
			});
		} catch(error) {
			const formattedError = formatError(error);
			callback(formattedError);
		}
	}
};
