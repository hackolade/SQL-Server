'use strict';

module.exports = (app, options) => {
	const _ = app.require('lodash');
	const { createColumnDefinitionBySchema } = require('./columnHelpers/createColumnDefinition')(_);
	const { getTableName } = require('../general')(app);
	const { getFullTableName, getEntityName } = require('../../utils/general')(app.require('lodash'));
	const ddlProvider = require('../../ddlProvider')(null, options, app);
	const { generateIdToNameHashTable, generateIdToActivatedHashTable } = app.require('@hackolade/ddl-fe-utils');
	const { setIndexKeys, modifyGroupItems } = require('./common')(app);
	const { getRenameColumnScriptsDto } = require('./columnHelpers/renameColumnHelpers')(app, ddlProvider);
	const { getChangedComputedColumnsScriptsDto } = require('./columnHelpers/alterComputedColumnHelpr')(
		app,
		ddlProvider,
	);
	const { getChangeTypeScriptsDto } = require('./columnHelpers/alterTypeHelper')(app, ddlProvider);
	const { AlterScriptDto } = require('./types/AlterScriptDto');
	const { getModifyCheckConstraintScriptDtos } = require('./entityHelpers/checkConstraintHelper');
	const { getModifyPkConstraintsScriptDtos } = require('./entityHelpers/primaryKeyHelper');
	const { getModifyNonNullColumnsScriptDtos } = require('./columnHelpers/notNullConstraintsHelper');

	/**
	 * @param {Collection} collection
	 * @return Array<AlterScriptDto>
	 * */
	const getAddCollectionScriptDto = collection => {
		//done but need clean up
		const schemaName = collection.compMod.keyspaceName;
		const schemaData = { schemaName };
		const jsonSchema = { ...collection, ...(collection?.role || {}) };
		const tableName = getEntityName(jsonSchema);
		const idToNameHashTable = generateIdToNameHashTable(jsonSchema);
		const idToActivatedHashTable = generateIdToActivatedHashTable(jsonSchema);
		const columnDefinitions = _.toPairs(jsonSchema.properties).map(([name, column]) =>
			createColumnDefinitionBySchema({
				name,
				jsonSchema: column,
				parentJsonSchema: jsonSchema,
				ddlProvider,
				schemaData,
			}),
		);
		const checkConstraints = (jsonSchema.chkConstr || []).map(check =>
			ddlProvider.createCheckConstraint(ddlProvider.hydrateCheckConstraint(check)),
		);
		const tableData = {
			name: tableName,
			columns: columnDefinitions.map(ddlProvider.convertColumnDefinition),
			checkConstraints: checkConstraints,
			foreignKeyConstraints: [],
			schemaData,
			columnDefinitions,
		};

		const indexesScripts = (jsonSchema.Indxs || [])
			.map(hydrateIndex({ idToNameHashTable, idToActivatedHashTable, ddlProvider, tableData, schemaData }))
			.map(index => _.trim(ddlProvider.createIndex(tableName, index, null, jsonSchema.isActivated)));

		const hydratedTable = ddlProvider.hydrateTable({
			tableData,
			entityData: [jsonSchema],
			jsonSchema,
			idToNameHashTable,
		});
		const tableScriptDto = AlterScriptDto.getInstance(
			[ddlProvider.createTable(hydratedTable, jsonSchema.isActivated)],
			true,
			false,
		);
		const indexesScriptsDto = indexesScripts
			.map(indexScript => AlterScriptDto.getInstance([indexScript], true, false))
			.filter(Boolean);

		return [tableScriptDto, ...indexesScriptsDto].filter(Boolean);
	};

	/**
	 * @param {Collection} collection
	 * @return {AlterScriptDto}
	 * */
	const getDeleteCollectionScriptDto = collection => {
		const fullName = getFullTableName(collection);
		const script = ddlProvider.dropTable(fullName);

		return AlterScriptDto.getInstance([script], true, true);
	};

	/**
	 * @param {Collection} collection
	 * @return {Array<AlterScriptDto>}
	 * */
	const getModifyCollectionScriptDto = collection => {
		const jsonSchema = { ...collection, ...(collection?.role || {}) };
		const schemaName = collection.compMod?.keyspaceName;
		const schemaData = { schemaName };
		const idToNameHashTable = generateIdToNameHashTable(jsonSchema);
		const idToActivatedHashTable = generateIdToActivatedHashTable(jsonSchema);
		const modifyCheckConstraintScriptDtos = getModifyCheckConstraintScriptDtos(_, ddlProvider)(collection);
		const modifyPKConstraintDtos = getModifyPkConstraintsScriptDtos(app, _, ddlProvider)(collection);
		const indexesScriptsDtos = modifyGroupItems({
			data: jsonSchema,
			key: 'Indxs',
			hydrate: hydrateIndex({
				idToNameHashTable,
				idToActivatedHashTable,
				ddlProvider,
				schemaData,
				tableData: [jsonSchema],
			}),
			create: (tableName, index) =>
				index.orReplace
					? [
							AlterScriptDto.getInstance([ddlProvider.dropIndex(tableName, index)], true, true),
							AlterScriptDto.getInstance([ddlProvider.createIndex(tableName, index, null)], true, false),
						]
					: AlterScriptDto.getInstance([ddlProvider.createIndex(tableName, index, schemaData)], true, false),
			drop: (tableName, index) =>
				AlterScriptDto.getInstance([ddlProvider.dropIndex(tableName, index)], true, true),
		}).flat();

		return [...modifyCheckConstraintScriptDtos, ...modifyPKConstraintDtos, ...indexesScriptsDtos].filter(Boolean);
	};

	/**
	 * @param {Collection} collection
	 * @return {Array<AlterScriptDto> | undefined}
	 * */
	const getAddColumnScriptDto = collection => {
		const collectionSchema = { ...collection, ...(_.omit(collection?.role, 'properties') || {}) };
		const tableName = collectionSchema?.code || collectionSchema?.collectionName || collectionSchema?.name;
		const schemaName = collectionSchema.compMod?.keyspaceName;
		const fullName = getTableName(tableName, schemaName);
		const schemaData = { schemaName };

		return _.toPairs(collection.properties)
			.filter(([name, jsonSchema]) => !jsonSchema.compMod)
			.map(([name, jsonSchema]) => {
				const columnDefinition = createColumnDefinitionBySchema({
					name,
					jsonSchema,
					parentJsonSchema: collectionSchema,
					ddlProvider,
					schemaData,
				});

				const columnDefinitionScript = ddlProvider.convertColumnDefinition(columnDefinition);
				const script = ddlProvider.addColumn(fullName, columnDefinitionScript);

				return AlterScriptDto.getInstance([script], true, false);
			});
	};

	/**
	 * @param {Collection} collection
	 * @return {Array<AlterScriptDto> | undefined}
	 * */
	const getDeleteColumnScriptDto = collection => {
		const collectionSchema = { ...collection, ...(_.omit(collection?.role, 'properties') || {}) };
		const tableName = collectionSchema?.code || collectionSchema?.collectionName || collectionSchema?.name;
		const schemaName = collectionSchema.compMod?.keyspaceName;
		const fullName = getTableName(tableName, schemaName);

		return _.toPairs(collection.properties)
			.filter(([name, jsonSchema]) => !jsonSchema.compMod)
			.map(([name]) => {
				const script = ddlProvider.dropColumn(fullName, name);

				return AlterScriptDto.getInstance([script], true, true);
			})
			.filter(Boolean);
	};

	/**
	 * @param {Collection} collection
	 * @return {Array<AlterScriptDto> | undefined}
	 * */
	const getModifyColumnScriptDto = collection => {
		const collectionSchema = { ...collection, ...(_.omit(collection?.role, 'properties') || {}) };
		const tableName = collectionSchema?.code || collectionSchema?.collectionName || collectionSchema?.name;
		const schemaName = collectionSchema.compMod?.keyspaceName;
		const fullName = getTableName(tableName, schemaName);

		const renameColumnScriptsDtos = getRenameColumnScriptsDto(collection.properties, fullName);
		const changeTypeScriptsDtos = getChangeTypeScriptsDto(
			collection.properties,
			fullName,
			collectionSchema,
			schemaName,
		);
		const modifyNotNullScriptDtos = getModifyNonNullColumnsScriptDtos(_, ddlProvider)(
			collection,
			collectionSchema,
			schemaName,
		);
		const changedComputedScriptsDtos = getChangedComputedColumnsScriptsDto(
			collection,
			fullName,
			collectionSchema,
			schemaName,
		);

		return [
			...renameColumnScriptsDtos,
			...changeTypeScriptsDtos,
			...modifyNotNullScriptDtos,
			...changedComputedScriptsDtos,
		].filter(Boolean);
	};

	const hydrateIndex =
		({ idToNameHashTable, idToActivatedHashTable, ddlProvider, tableData, schemaData }) =>
		index => {
			index = setIndexKeys(idToNameHashTable, idToActivatedHashTable, index);

			return ddlProvider.hydrateIndex(index, tableData, schemaData);
		};

	const getTableUpdateCommentScript = ({ schemaName, tableName, comment }) =>
		ddlProvider.updateTableComment({ schemaName, tableName, comment });
	const getTableDropCommentScript = ({ schemaName, tableName }) =>
		ddlProvider.dropTableComment({
			schemaName,
			tableName,
		});

	const getTablesDropCommentAlterScriptsDto = tables => {
		return Object.keys(tables)
			.map(tableName => {
				const table = tables[tableName];

				if (!table?.compMod?.deleted || !table?.role?.description) {
					return undefined;
				}

				const schemaName = table.role?.compMod.keyspaceName;
				const script = getTableDropCommentScript({ schemaName, tableName });

				return AlterScriptDto.getInstance([script], true, true);
			})
			.filter(Boolean);
	};

	const getTablesModifyCommentsAlterScriptsDto = tables => {
		return Object.keys(tables)
			.map(tableName => {
				let script = '';

				const tableComparison = tables[tableName].role?.compMod;
				const schemaName = tableComparison.keyspaceName;

				const newComment = tableComparison?.description?.new;
				const oldComment = tableComparison?.description?.old;

				const isCommentRemoved = oldComment && !newComment;

				if (isCommentRemoved) {
					script = getTableDropCommentScript({ schemaName, tableName });

					return AlterScriptDto.getInstance([script], true, true);
				}

				if (!newComment || newComment === oldComment) {
					return undefined;
				}

				script = getTableUpdateCommentScript({ schemaName, tableName, comment: newComment });

				return AlterScriptDto.getInstance([script], true, false);
			})
			.filter(Boolean);
	};

	const getColumnCreateCommentScript = ({ schemaName, tableName, columnName, comment }) =>
		ddlProvider.createColumnComment({
			schemaName,
			tableName,
			columnName,
			comment,
		});
	const getColumnUpdateCommentScript = ({ schemaName, tableName, columnName, comment }) =>
		ddlProvider.updateColumnComment({
			schemaName,
			tableName,
			columnName,
			comment,
		});
	const getColumnDropCommentScript = ({ schemaName, tableName, columnName }) =>
		ddlProvider.dropColumnComment({ schemaName, tableName, columnName });

	const getColumnsCreateCommentAlterScriptsDto = tables => {
		return Object.keys(tables)
			.map(tableName => {
				const columns = tables[tableName].properties;
				if (!columns) {
					return [];
				}
				const schemaName = tables[tableName].role?.compMod.keyspaceName;
				return Object.keys(columns).map(columnName => {
					const column = columns[columnName];
					const isColumnRenamed = column?.compMod?.oldField?.name !== column?.compMod?.newField?.name;
					const columnNameToSearchComment = isColumnRenamed ? column?.compMod?.oldField?.name : columnName;
					const comment = column.description;
					const oldComment = tables[tableName].role?.properties[columnNameToSearchComment]?.description;

					if (comment || !oldComment) {
						return undefined;
					}

					const script = getColumnCreateCommentScript({ schemaName, tableName, columnName, comment });

					return AlterScriptDto.getInstance([script], true, false);
				});
			})
			.flat()
			.filter(Boolean);
	};

	const getColumnsDropCommentAlterScriptsDto = tables => {
		return Object.keys(tables)
			.map(tableName => {
				const columns = tables[tableName].properties;

				if (!columns) {
					return [];
				}

				const schemaName = tables[tableName].role?.compMod.keyspaceName;

				return Object.keys(columns)
					.filter(columnName => Boolean(columns[columnName].description))
					.map(columnName => {
						const script = getColumnDropCommentScript({ schemaName, tableName, columnName });

						return AlterScriptDto.getInstance([script], true, true);
					});
			})
			.flat()
			.filter(Boolean);
	};

	const getColumnsModifyCommentAlterScriptsDto = tables => {
		return Object.keys(tables)
			.map(tableName => {
				const columns = tables[tableName].properties;
				if (!columns) {
					return undefined;
				}
				const schemaName = tables[tableName].role?.compMod.keyspaceName;
				return Object.keys(columns).map(columnName => {
					let script = '';
					const newComment = columns[columnName]?.description;
					const oldComment = tables[tableName].role?.properties[columnName]?.description;
					const isCommentRemoved = oldComment && !newComment;

					if (isCommentRemoved) {
						script = getColumnDropCommentScript({ schemaName, tableName, columnName });

						return AlterScriptDto.getInstance([script], true, true);
					}

					if (!newComment || !oldComment || newComment === oldComment) {
						return undefined;
					}

					script = getColumnUpdateCommentScript({ schemaName, tableName, columnName, comment: newComment });
					return AlterScriptDto.getInstance([script], true, false);
				});
			})
			.flat()
			.filter(Boolean);
	};

	return {
		getAddCollectionScriptDto,
		getDeleteCollectionScriptDto,
		getModifyCollectionScriptDto,
		getAddColumnScriptDto,
		getDeleteColumnScriptDto,
		getModifyColumnScriptDto,
		getTablesDropCommentAlterScriptsDto,
		getTablesModifyCommentsAlterScriptsDto,
		getColumnsCreateCommentAlterScriptsDto,
		getColumnsDropCommentAlterScriptsDto,
		getColumnsModifyCommentAlterScriptsDto,
	};
};
