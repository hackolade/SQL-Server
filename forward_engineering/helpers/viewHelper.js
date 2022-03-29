const commentIfDeactivated = require('./commentIfDeactivated');

module.exports = app => {
	const _ = app.require('lodash');
	const { assignTemplates } = app.require('@hackolade/ddl-fe-utils');
	const { divideIntoActivatedAndDeactivated } = app.require('@hackolade/ddl-fe-utils').general;

	const createViewSelectStatement = ({ terminator, keys, tables, selectStatement, isParentActivated }) => {
		const mapKeys = key => (key.alias ? `[${key.name}] AS [${key.alias}]` : `[${key.name}]`);
		let selectKeys = keys.map(mapKeys).join(',');

		if (isParentActivated) {
			const dividedKeys = divideIntoActivatedAndDeactivated(keys, mapKeys);
			const activatedKeys = dividedKeys.activatedItems.join(',');
			const deactivatedKeys = dividedKeys.deactivatedItems.length
				? commentIfDeactivated(dividedKeys.deactivatedItems.join(','), { isActivated: false }, true)
				: '';
			selectKeys = activatedKeys + deactivatedKeys || '*';
		}

		return tables
			.map(table => {
				return selectStatement
					? assignTemplates(selectStatement, { tableName: getFullTableName(table), terminator })
					: `SELECT ${selectKeys}\nFROM ${getFullTableName(table)}`;
			})
			.join('\nUNION ALL\n');
	};

	const getFullTableName = table => {
		let name = `[${table.name}]`;

		if (table.schemaName) {
			name = `[${table.schemaName}].` + name;
		}

		if (table.databaseName) {
			name = `[${table.databaseName}].` + name;
		}

		return name;
	};

	const getPartitionedTables = (partitionedTables, relatedSchemas, relatedContainers = {}) => {
		const schemaIds = partitionedTables.map(item => item.table);
		const names = schemaIds
			.map(id => relatedSchemas[id])
			.filter(Boolean)
			.map(schema => {
				let schemaName = '';
				let databaseName = '';

				if (relatedContainers[schema.bucketId]) {
					const db = _.first(relatedContainers[schema.bucketId]) || {};
					schemaName = db.code || db.name || '';
					databaseName = db.databaseName || '';
				}

				return {
					name: schema.code || schema.collectionName,
					databaseName,
					schemaName,
				};
			});

		return names;
	};

	return {
		createViewSelectStatement,
		getPartitionedTables,
	};
};
