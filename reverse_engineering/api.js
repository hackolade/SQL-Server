'use strict';

const connectionStringParser = require('mssql/lib/connectionstring');
const { getClient, setClient, clearClient } = require('./connectionState');
const { getObjectsFromDatabase, getDatabaseCollationOption, getDescriptionComments } = require('./databaseService/databaseService');
const {
	reverseCollectionsToJSON,
	mergeCollectionsWithViews,
	getCollectionsRelationships,
	logDatabaseVersion,
} = require('./reverseEngineeringService/reverseEngineeringService');
const logInfo = require('./helpers/logInfo');
const filterRelationships = require('./helpers/filterRelationships');
const getOptionsFromConnectionInfo = require('./helpers/getOptionsFromConnectionInfo');
const { adaptJsonSchema } = require('./helpers/adaptJsonSchema');
const crypto = require('crypto');
const randomstring = require("randomstring");
const base64url = require('base64url');

module.exports = {
	async connect(connectionInfo, logger, callback, app) {
		const client = getClient();
		if (!client) {
			await setClient(connectionInfo, 0, logger);
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
			if (connectionInfo.authMethod === 'Azure Active Directory (MFA)') {
				await this.getExternalBrowserUrl(connectionInfo, logger, callback, app);
			} else {
				const client = await this.connect(connectionInfo, logger);
				await logDatabaseVersion(client, logger);
			}
			callback(null);
		} catch(error) {
			logger.log('error', { message: error.message, stack: error.stack, error }, 'Test connection');
			callback({ message: error.message, stack: error.stack });
		}
	},

	async getExternalBrowserUrl(connectionInfo, logger, cb, app) {
		const verifier = randomstring.generate(32);
		const base64Digest = crypto
			.createHash("sha256")
			.update(verifier)
			.digest("base64");
		const challenge = base64url.fromBase64(base64Digest);
		const tenantId = connectionInfo.connectionTenantId || connectionInfo.tenantId || 'common';
		const clientId = '0dc36597-bc44-49f8-a4a7-ae5401959b85';
		const loginHint = connectionInfo.loginHint ? `login_hint=${encodeURIComponent(connectionInfo.loginHint)}&` : '';
		const redirectUrl = `http://localhost:${connectionInfo.redirectPort || 8080}`;

		cb(null, { proofKey: verifier, url:`https://login.microsoftonline.com/${tenantId}/oauth2/authorize?${loginHint}code_challenge_method=S256&code_challenge=${challenge}&response_type=code&response_mode=query&client_id=${clientId}&redirect_uri=${redirectUrl}&prompt=select_account&resource=https://database.windows.net/`});
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
			const client = await this.connect(connectionInfo, logger);
			if (!client.config.database) {
				throw new Error('No database specified');
			}

			await logDatabaseVersion(client, logger);
			
			const objects = await getObjectsFromDatabase(client);
			const dbName = client.config.database;
            const collationData = (await getDatabaseCollationOption(client, dbName, logger)) || [];
			logger.log('info', { collation: collationData[0] }, 'Database collation');
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
			const dbName = client.config.database
			if (!dbName) {
				throw new Error('No database specified');
			}

			const reverseEngineeringOptions = getOptionsFromConnectionInfo(collectionsInfo);
			const [jsonSchemas, relationships] = await Promise.all([
				await reverseCollectionsToJSON(logger)(client, collections, reverseEngineeringOptions),
				await getCollectionsRelationships(logger)(client, collections),
			]);

			// Getting unique schemas names
			const schemas = [...new Set(jsonSchemas.map(({dbName}) => dbName))].map(dbName => ({schema: dbName}))
			let schemasWithTables = schemas.map(({schema}) => ({schema, tablesCount: 0}));
			let viewsWithSchemas = schemas.map(({schema}) => ({schema, viewsCount: 0}));
			const entities = jsonSchemas.map(({collectionName: name, dbName, data}) => {
				const type = data && Object.keys(data).includes('viewAttrbute') ? 'view' : 'table'
				if (type === 'view') {
					viewsWithSchemas = viewsWithSchemas.map((schema) => schema.schema === dbName ? ({schema: schema.schema, viewsCount: schema.viewsCount + 1}) : schema)
				}
				if (type === 'table') {
					schemasWithTables = schemasWithTables.map((schema) => schema.schema === dbName ? ({schema: schema.schema, tablesCount: schema.tablesCount + 1}) : schema)
				}
				return {
					schema: dbName, 
					entity: {type, name}
				}
			})

			viewsWithSchemas = viewsWithSchemas.filter(schema => schema.viewsCount > 0).map(({schema}) => ({schema, entity: {type: 'view'}}))
			schemasWithTables = schemasWithTables.filter(schema => schema.tablesCount > 0).map(({schema}) => ({schema, entity: {type: 'table'}}))

			const descriptionComments = (await Promise
				.all([...schemas, ...viewsWithSchemas, ...schemasWithTables, ...entities]
				.map(commentParams => getDescriptionComments(client, dbName, commentParams, logger)))).flat();

			let jsonSchemasWithDescriptionComments = [...jsonSchemas]
			descriptionComments.forEach(({objtype, objname, value}) => {
				if (objtype === 'SCHEMA') {
					jsonSchemasWithDescriptionComments = jsonSchemasWithDescriptionComments
					.map(collection => collection.dbName === objname ? 
						{
							...collection, 
							bucketInfo: {
								...collection.bucketInfo, 
								description: value
							}
						} : collection )

				} else if (objtype === 'TABLE' || objtype === 'VIEW') {
					jsonSchemasWithDescriptionComments = jsonSchemasWithDescriptionComments
					.map(collection => collection.collectionName === objname ? 
						{
							...collection, 
							entityLevel: {
								...collection.entityLevel, 
								description: value
							}
						} : collection )

				} else if (objtype === 'COLUMN') {
					jsonSchemasWithDescriptionComments = jsonSchemasWithDescriptionComments
					.map(collection => 
						!collection?.data 
						&& collection?.validation?.jsonSchema?.properties
						&& Object.keys(collection?.validation?.jsonSchema?.properties).includes(objname) ? 
					{
						...collection, 
						validation: {
							...collection.validation, 
							jsonSchema: {
								...collection.validation.jsonSchema,
								properties: {
									...collection.validation.jsonSchema.properties,
									[objname]: {
										...collection.validation.jsonSchema.properties[objname],
										description: value
									}
								}
							}
						}
					} : collection )
				}
			})
			callback(null, mergeCollectionsWithViews(jsonSchemasWithDescriptionComments), null, filterRelationships(relationships, jsonSchemasWithDescriptionComments));
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
