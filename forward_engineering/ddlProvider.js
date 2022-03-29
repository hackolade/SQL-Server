const defaultTypes = require('./configs/defaultTypes');
const types = require('./configs/types');
const templates = require('./configs/templates');
const commentIfDeactivated = require('./helpers/commentIfDeactivated');

module.exports = (baseProvider, options, app) => {
	const _ = app.require('lodash');
	const { assignTemplates } = app.require('@hackolade/ddl-fe-utils');
	const { tab, checkAllKeysDeactivated, divideIntoActivatedAndDeactivated } =
		app.require('@hackolade/ddl-fe-utils').general;

	const { decorateType, getIdentity, getEncryptedWith } = require('./helpers/columnDefinitionHelper')(app);
	const {
		createIndex,
		hydrateIndex,
		getMemoryOptimizedIndexes,
		createMemoryOptimizedIndex,
		hydrateTableIndex,
		createTableIndex,
	} = require('./helpers/indexHelper')(app);
	const {
		getViewData,
		getTableName,
		getTableOptions,
		hasType,
		getDefaultConstraints,
		foreignKeysToString,
		checkIndexActivated,
		getDefaultValue,
		getTempTableTime,
		foreignActiveKeysToString,
	} = require('./helpers/general')(app);
	const keyHelper = require('./helpers/keyHelper')(app);
	const { getTerminator } = require('./helpers/optionsHelper');
	const { createKeyConstraint, createDefaultConstraint, generateConstraintsString } =
		require('./helpers/constraintsHelper')(app);
	const { wrapIfNotExistSchema, wrapIfNotExistDatabase, wrapIfNotExistTable, wrapIfNotExistView } =
		require('./helpers/ifNotExistStatementHelper')(app);
	const { createViewSelectStatement, getPartitionedTables } = require('./helpers/viewHelper')(app);

	const terminator = getTerminator(options);

	return {
		createSchema({ schemaName, databaseName, ifNotExist }) {
			const schemaTerminator = ifNotExist ? ';' : terminator;
			let schemaStatement = assignTemplates(templates.createSchema, {
				name: schemaName,
				terminator: schemaTerminator,
			});

			if (!databaseName) {
				return ifNotExist
					? wrapIfNotExistSchema({ templates, schemaStatement, schemaName, terminator })
					: schemaStatement;
			}

			const databaseStatement = wrapIfNotExistDatabase({
				templates,
				databaseName,
				terminator,
				databaseStatement: assignTemplates(templates.createDatabase, {
					name: databaseName,
					terminator: schemaTerminator,
				}),
			});

			if (ifNotExist) {
				return (
					databaseStatement +
					'\n\n' +
					wrapIfNotExistSchema({ templates, schemaStatement, schemaName, terminator })
				);
			}

			return databaseStatement + '\n\n' + schemaStatement;
		},

		createTable(
			{
				name,
				columns,
				checkConstraints,
				foreignKeyConstraints,
				keyConstraints,
				options,
				schemaData,
				defaultConstraints,
				memoryOptimizedIndexes,
				temporalTableTime,
				ifNotExist,
			},
			isActivated,
		) {
			const tableTerminator = ifNotExist ? ';' : terminator;
			const tableName = getTableName(name, schemaData.schemaName);
			const dividedKeysConstraints = divideIntoActivatedAndDeactivated(
				keyConstraints.map(createKeyConstraint(templates, tableTerminator, isActivated)),
				key => key.statement,
			);
			const keyConstraintsString = generateConstraintsString(dividedKeysConstraints, isActivated);
			const temporalTableTimeStatement =
				temporalTableTime.startTime && temporalTableTime.endTime
					? `,\n\tPERIOD FOR SYSTEM_TIME(${temporalTableTime.startTime}, ${temporalTableTime.endTime})`
					: '';
			const dividedForeignKeys = divideIntoActivatedAndDeactivated(foreignKeyConstraints, key => key.statement);
			const foreignKeyConstraintsString = generateConstraintsString(dividedForeignKeys, isActivated);
			const tableStatement = assignTemplates(templates.createTable, {
				name: tableName,
				column_definitions: columns.join(',\n\t'),
				temporalTableTime: temporalTableTimeStatement,
				checkConstraints: checkConstraints.length ? ',\n\t' + checkConstraints.join(',\n\t') : '',
				foreignKeyConstraints: foreignKeyConstraintsString,
				options: getTableOptions(options),
				keyConstraints: keyConstraintsString,
				memoryOptimizedIndexes: memoryOptimizedIndexes.length
					? ',\n\t' +
					  memoryOptimizedIndexes
							.map(createMemoryOptimizedIndex(isActivated))
							.map(index => commentIfDeactivated(index.statement, index))
							.join(',\n\t')
					: '',
				terminator: tableTerminator,
			});
			const defaultConstraintsStatements = defaultConstraints
				.map(data => createDefaultConstraint(templates, tableTerminator)(data, tableName))
				.join('\n');

			const fullTableStatement = [tableStatement, defaultConstraintsStatements].filter(Boolean).join('\n\n');

			return ifNotExist
				? wrapIfNotExistTable({
						tableStatement: fullTableStatement,
						templates,
						tableName,
						terminator,
				  })
				: fullTableStatement;
		},

		convertColumnDefinition(columnDefinition) {
			const type = hasType(columnDefinition.type)
				? _.toUpper(columnDefinition.type)
				: getTableName(columnDefinition.type, columnDefinition.schemaName);
			const notNull = columnDefinition.nullable ? '' : ' NOT NULL';
			const primaryKey = columnDefinition.primaryKey ? ' PRIMARY KEY' : '';
			const defaultValue = getDefaultValue(
				columnDefinition.default,
				columnDefinition.defaultConstraint?.name,
				type,
			);
			const sparse = columnDefinition.sparse ? ' SPARSE' : '';
			const maskedWithFunction = columnDefinition.maskedWithFunction
				? ` MASKED WITH (FUNCTION='${columnDefinition.maskedWithFunction}')`
				: '';
			const identity = getIdentity(columnDefinition.identity);
			const encryptedWith = !_.isEmpty(columnDefinition.encryption)
				? getEncryptedWith(columnDefinition.encryption[0])
				: '';
			const unique = columnDefinition.unique ? ' UNIQUE' : '';
			const temporalTableTime = getTempTableTime(
				columnDefinition.isTempTableStartTimeColumn,
				columnDefinition.isTempTableEndTimeColumn,
				columnDefinition.isHidden,
			);

			return commentIfDeactivated(
				assignTemplates(templates.columnDefinition, {
					name: columnDefinition.name,
					type: decorateType(type, columnDefinition),
					primary_key: primaryKey + unique,
					not_null: notNull,
					default: defaultValue,
					sparse,
					maskedWithFunction,
					identity,
					encryptedWith,
					terminator,
					temporalTableTime,
				}),
				columnDefinition,
			);
		},

		createIndex(tableName, index, dbData, isParentActivated = true) {
			const isActivated = checkIndexActivated(index);
			if (!isParentActivated) {
				return createTableIndex(terminator, tableName, index, isActivated && isParentActivated);
			}
			return commentIfDeactivated(
				createTableIndex(terminator, tableName, index, isActivated && isParentActivated),
				{
					isActivated,
				},
			);
		},

		createCheckConstraint(checkConstraint) {
			return assignTemplates(templates.checkConstraint, {
				name: checkConstraint.name,
				notForReplication: checkConstraint.enforceForReplication ? '' : ' NOT FOR REPLICATION',
				expression: _.trim(checkConstraint.expression).replace(/^\(([\s\S]*)\)$/, '$1'),
				terminator,
			});
		},

		createForeignKeyConstraint(
			{
				name,
				foreignKey,
				primaryTable,
				primaryKey,
				primaryTableActivated,
				foreignTableActivated,
				primarySchemaName,
			},
			dbData,
			schemaData,
		) {
			const isAllPrimaryKeysDeactivated = checkAllKeysDeactivated(primaryKey);
			const isAllForeignKeysDeactivated = checkAllKeysDeactivated(foreignKey);
			const isActivated =
				!isAllPrimaryKeysDeactivated &&
				!isAllForeignKeysDeactivated &&
				primaryTableActivated &&
				foreignTableActivated;

			return {
				statement: assignTemplates(templates.createForeignKeyConstraint, {
					primaryTable: getTableName(primaryTable, primarySchemaName || schemaData.schemaName),
					name,
					foreignKey: isActivated ? foreignKeysToString(foreignKey) : foreignActiveKeysToString(foreignKey),
					primaryKey: isActivated ? foreignKeysToString(primaryKey) : foreignActiveKeysToString(primaryKey),
					terminator,
				}),
				isActivated,
			};
		},

		createForeignKey(
			{ name, foreignTable, foreignKey, primaryTable, primaryKey, primaryTableActivated, foreignTableActivated },
			dbData,
			schemaData,
		) {
			const isAllPrimaryKeysDeactivated = checkAllKeysDeactivated(primaryKey);
			const isAllForeignKeysDeactivated = checkAllKeysDeactivated(foreignKey);

			return {
				statement: assignTemplates(templates.createForeignKey, {
					primaryTable: getTableName(primaryTable, schemaData.schemaName),
					foreignTable: getTableName(foreignTable, schemaData.schemaName),
					name,
					foreignKey: foreignKeysToString(foreignKey),
					primaryKey: foreignKeysToString(primaryKey),
					terminator,
				}),
				isActivated:
					!isAllPrimaryKeysDeactivated &&
					!isAllForeignKeysDeactivated &&
					primaryTableActivated &&
					foreignTableActivated,
			};
		},

		createView(
			{
				name,
				keys,
				partitionedTables,
				partitioned,
				viewAttrbute,
				withCheckOption,
				selectStatement,
				schemaData,
				ifNotExist,
			},
			dbData,
			isActivated,
		) {
			const viewTerminator = ifNotExist ? ';' : terminator;
			const viewData = getViewData(keys, schemaData);

			if (partitioned) {
				selectStatement = createViewSelectStatement({
					tables: partitionedTables,
					selectStatement,
					keys,
					terminator: viewTerminator,
					isParentActivated: isActivated,
				});
			}

			if ((_.isEmpty(viewData.tables) || _.isEmpty(viewData.columns)) && !selectStatement) {
				return '';
			}

			let columnsAsString = viewData.columns.map(column => column.statement).join(',\n\t\t');

			if (isActivated) {
				const dividedColumns = divideIntoActivatedAndDeactivated(viewData.columns, column => column.statement);
				const deactivatedColumnsString = dividedColumns.deactivatedItems.length
					? commentIfDeactivated(
							dividedColumns.deactivatedItems.join(',\n\t\t'),
							{ isActivated: false },
							true,
					  )
					: '';
				columnsAsString = dividedColumns.activatedItems.join(',\n\t\t') + deactivatedColumnsString;
			}

			const viewName = getTableName(name, schemaData.schemaName);
			const viewStatement = assignTemplates(templates.createView, {
				name: viewName,
				view_attribute: viewAttrbute ? `WITH ${viewAttrbute}\n` : '',
				check_option: withCheckOption ? `WITH CHECK OPTION` : '',
				select_statement: _.trim(selectStatement)
					? _.trim(tab(selectStatement)) + '\n'
					: assignTemplates(templates.viewSelectStatement, {
							tableName: viewData.tables.join(', '),
							keys: columnsAsString,
							terminator: viewTerminator,
					  }),
				terminator: viewTerminator,
			});

			return ifNotExist ? wrapIfNotExistView({ templates, viewStatement, viewName, terminator }) : viewStatement;
		},

		createViewIndex(viewName, index, dbData, isParentActivated) {
			const isActivated = checkIndexActivated(index);
			return commentIfDeactivated(createIndex(terminator, viewName, index, isActivated && isParentActivated), {
				isActivated: isParentActivated ? isActivated : true,
			});
		},

		createUdt(udt) {
			const notNull = udt.nullable ? '' : ' NOT NULL';
			const type = decorateType(hasType(udt.type) ? _.toUpper(udt.type) : udt.type, udt);

			return assignTemplates(templates.createUdtFromBaseType, {
				name: getTableName(udt.name, udt.schemaName),
				base_type: type,
				not_null: notNull,
				terminator,
			});
		},

		getDefaultType(type) {
			return defaultTypes[type];
		},

		getTypesDescriptors() {
			return types;
		},

		hasType(type) {
			return hasType(type);
		},

		hydrateColumn({ columnDefinition, jsonSchema, schemaData, parentJsonSchema }) {
			let encryption = [];

			if (Array.isArray(jsonSchema.encryption)) {
				encryption = jsonSchema.encryption.map(
					({ COLUMN_ENCRYPTION_KEY: key, ENCRYPTION_TYPE: type, ENCRYPTION_ALGORITHM: algorithm }) => ({
						key,
						type,
						algorithm,
					}),
				);
			} else if (_.isPlainObject(jsonSchema.encryption)) {
				encryption = [
					{
						key: jsonSchema.encryption.COLUMN_ENCRYPTION_KEY,
						type: jsonSchema.encryption.ENCRYPTION_TYPE,
						algorithm: jsonSchema.encryption.ENCRYPTION_ALGORITHM,
					},
				];
			}

			const isTempTableStartTimeColumn =
				jsonSchema.GUID === _.get(parentJsonSchema, 'periodForSystemTime[0].startTime[0].keyId', '');
			const isTempTableEndTimeColumn =
				jsonSchema.GUID === _.get(parentJsonSchema, 'periodForSystemTime[0].endTime[0].keyId', '');
			const isTempTableStartTimeColumnHidden =
				_.get(parentJsonSchema, 'periodForSystemTime[0].startTime[0].type', '') === 'hidden';
			const isTempTableEndTimeColumnHidden =
				_.get(parentJsonSchema, 'periodForSystemTime[0].startTime[0].type', '') === 'hidden';

			return Object.assign({}, columnDefinition, {
				default: jsonSchema.defaultConstraintName ? '' : columnDefinition.default,
				defaultConstraint: {
					name: jsonSchema.defaultConstraintName,
					value: columnDefinition.default,
				},
				primaryKey: keyHelper.isInlinePrimaryKey(jsonSchema),
				xmlConstraint: String(jsonSchema.XMLconstraint || ''),
				xmlSchemaCollection: String(jsonSchema.xml_schema_collection || ''),
				sparse: Boolean(jsonSchema.sparse),
				maskedWithFunction: String(jsonSchema.maskedWithFunction || ''),
				identity: {
					seed: Number(_.get(jsonSchema, 'identity.identitySeed', 0)),
					increment: Number(_.get(jsonSchema, 'identity.identityIncrement', 0)),
				},
				schemaName: schemaData.schemaName,
				unique: keyHelper.isInlineUnique(jsonSchema),
				isTempTableStartTimeColumn,
				isTempTableEndTimeColumn,
				isHidden: isTempTableStartTimeColumn
					? isTempTableStartTimeColumnHidden
					: isTempTableEndTimeColumnHidden,
				encryption,
				hasMaxLength: columnDefinition.hasMaxLength || jsonSchema.type === 'jsonObject',
			});
		},

		hydrateIndex(indexData, tableData, schemaData) {
			const isMemoryOptimized = _.get(tableData, '[0].memory_optimized', false);

			if (isMemoryOptimized) {
				return;
			}

			return hydrateTableIndex(indexData, schemaData);
		},

		hydrateViewIndex(indexData, schemaData) {
			return hydrateIndex(indexData, schemaData);
		},

		hydrateCheckConstraint(checkConstraint) {
			return {
				name: checkConstraint.chkConstrName,
				expression: checkConstraint.constrExpression,
				existingData: checkConstraint.constrCheck,
				enforceForUpserts: checkConstraint.constrEnforceUpserts,
				enforceForReplication: checkConstraint.constrEnforceReplication,
			};
		},

		hydrateSchema(containerData) {
			return {
				schemaName: containerData.name,
				databaseName: containerData.databaseName,
				ifNotExist: containerData.ifNotExist,
			};
		},

		hydrateTable({ tableData, entityData, jsonSchema, idToNameHashTable }) {
			const isMemoryOptimized = _.get(entityData, '[0].memory_optimized', false);
			const temporalTableTimeStartColumnName =
				idToNameHashTable[_.get(jsonSchema, 'periodForSystemTime[0].startTime[0].keyId', '')];
			const temporalTableTimeEndColumnName =
				idToNameHashTable[_.get(jsonSchema, 'periodForSystemTime[0].endTime[0].keyId', '')];
			return Object.assign({}, tableData, {
				foreignKeyConstraints: tableData.foreignKeyConstraints || [],
				keyConstraints: keyHelper.getTableKeyConstraints({ jsonSchema }),
				defaultConstraints: getDefaultConstraints(tableData.columnDefinitions),
				ifNotExist: jsonSchema.ifNotExist,
				options: {
					memory_optimized: isMemoryOptimized,
					durability: _.get(entityData, '[0].durability', ''),
					systemVersioning: _.get(entityData, '[0].systemVersioning', false),
					historyTable: _.get(entityData, '[0].historyTable', ''),
					dataConsistencyCheck: _.get(entityData, '[0].dataConsistencyCheck', false),
					temporal: _.get(entityData, '[0].temporal', false),
					ledger: _.get(entityData, '[0].ledger', false),
					ledger_view: _.get(entityData, '[0].ledger_view'),
					transaction_id_column_name: _.get(entityData, '[0].transaction_id_column_name'),
					sequence_number_column_name: _.get(entityData, '[0].sequence_number_column_name'),
					operation_type_id_column_name: _.get(entityData, '[0].operation_type_id_column_name'),
					operation_type_desc_column_name: _.get(entityData, '[0].operation_type_desc_column_name'),
					append_only: _.get(entityData, '[0].append_only', false),
					temporalTableTimeStartColumnName,
					temporalTableTimeEndColumnName,
				},
				temporalTableTime: {
					startTime: temporalTableTimeStartColumnName,
					endTime: temporalTableTimeEndColumnName,
				},
				memoryOptimizedIndexes: isMemoryOptimized
					? getMemoryOptimizedIndexes(entityData, tableData.schemaData)
					: [],
			});
		},

		hydrateViewColumn(data) {
			return {
				dbName: _.get(data.containerData, '[0].databaseName', ''),
				schemaName: data.dbName,
				alias: data.alias,
				name: data.name,
				tableName: data.entityName,
				isActivated: data.isActivated,
			};
		},

		hydrateView({ viewData, entityData, relatedSchemas, relatedContainers }) {
			const firstTab = _.get(entityData, '[0]', {});
			const isPartitioned = _.get(entityData, '[0].partitioned');
			const ifNotExist = _.get(entityData, '[0].ifNotExist');

			return {
				...viewData,
				selectStatement: firstTab.selectStatement || '',
				viewAttrbute: firstTab.viewAttrbute || '',
				materialized: firstTab.materialized,
				withCheckOption: Boolean(firstTab.withCheckOption),
				partitioned: isPartitioned,
				ifNotExist,
				partitionedTables: isPartitioned
					? getPartitionedTables(
							_.get(entityData, '[0].partitionedTables', []),
							relatedSchemas,
							relatedContainers,
					  )
					: [],
			};
		},

		commentIfDeactivated(statement, data, isPartOfLine) {
			return commentIfDeactivated(statement, data, isPartOfLine);
		},
	};
};
