const { commentDropStatements } = require('./helpers/commentDropStatements');
const { DROP_STATEMENTS } = require('./helpers/constants');
const logInfo = require('../reverse_engineering/helpers/logInfo');
const { connect } = require('../reverse_engineering/api');
const applyToInstanceHelper = require('./helpers/applyToInstanceHelper');

module.exports = {
	generateScript(data, logger, callback, app) {
		try {
			const { getAlterScript } = require('./helpers/alterScriptFromDeltaHelper');

			const collection = JSON.parse(data.jsonSchema);
			if (!collection) {
				throw new Error(
					'"comparisonModelCollection" is not found. Alter script can be generated only from Delta model',
				);
			}
			const script = getAlterScript(collection, app, data.options);

			const applyDropStatements = data.options?.additionalOptions?.some(
				option => option.id === 'applyDropStatements' && option.value,
			);

			callback(null, applyDropStatements ? script : commentDropStatements(script));
		} catch (error) {
			logger.log(
				'error',
				{ message: error.message, stack: error.stack },
				'MS SQL Server Forward-Engineering Error',
			);

			callback({ message: error.message, stack: error.stack });
		}
	},
	generateViewScript(data, logger, callback, app) {
		callback(new Error('Forward-Engineering of delta model on view level is not supported'));
	},
	generateContainerScript(data, logger, callback, app) {
		try {
			data.jsonSchema = data.collections[0];
			this.generateScript(data, logger, callback, app);
		} catch (error) {
			logger.log(
				'error',
				{ message: error.message, stack: error.stack },
				'MS SQL Server Forward-Engineering Error',
			);

			callback({ message: error.message, stack: error.stack });
		}
	},
	isDropInStatements(data, logger, callback, app) {
		try {
			const cb = (error, script = '') =>
				callback(
					error,
					DROP_STATEMENTS.some(statement => script.includes(statement)),
				);

			if (data.level === 'container') {
				this.generateContainerScript(data, logger, cb, app);
			} else {
				this.generateScript(data, logger, cb, app);
			}
		} catch (e) {
			callback({ message: e.message, stack: e.stack });
		}
	},
	async testConnection(connectionInfo, logger, callback, app) {
		try {
			logInfo('Test connection', connectionInfo, logger);
			await connect(connectionInfo);
			callback(null);
		} catch(error) {
			logger.log('error', { message: error.message, stack: error.stack, error }, 'Test connection');
			callback({ message: error.message, stack: error.stack });
		}
	},
	async applyToInstance(connectionInfo, logger, callback, app) {
		logger.clear();
		logInfo('Apply To Instance', connectionInfo, logger);
		
		try {
			await applyToInstanceHelper.applyToInstance(connectionInfo, logger, app);
			callback(null);
		} catch (error) {
			callback(error);
		}
	},
};
