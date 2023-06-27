import './types/typedefs';

module.exports = (app, options) => {
	const _ = app.require('lodash');
	const { getEntityName } = app.require('@hackolade/ddl-fe-utils').general;
	const { createColumnDefinitionBySchema } = require('./createColumnDefinition')(_);
	const { getTableName } = require('../general')(app);
	const ddlProvider = require('../../ddlProvider')(null, options, app);
	const { generateIdToNameHashTable, generateIdToActivatedHashTable } = app.require('@hackolade/ddl-fe-utils');
	const { modifyGroupItems, setIndexKeys } = require('./common')(app);
	const { getRenameColumnScriptsDto } = require('./columnHelpers/renameColumnHelpers')(app, ddlProvider);
	const { getChangeTypeScriptsDto } = require('./columnHelpers/alterTypeHelper')(app, ddlProvider);
	const { AlterScriptDto } = require('./types/AlterScriptDto');

	/**
	 * @param {Collection} collection
	 * @return Array<AlterScriptDto>
	 * */
	const getAddCollectionScriptDto = collection => { //done but need clean up
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
		const tableScriptDto = AlterScriptDto.getInstance([ddlProvider.createTable(hydratedTable, jsonSchema.isActivated)], true, false);
		const indexesScriptsDto = AlterScriptDto.getInstances(indexesScripts, true, false);

		return [tableScriptDto, ...indexesScriptsDto].filter(Boolean);
	};

	/**
	 * @param {Collection} collection
	 * @return {AlterScriptDto}
	 * */
	const getDeleteCollectionScriptDto = collection => {
		const jsonSchema = { ...collection, ...(collection?.role || {}) };
		const tableName = getEntityName(jsonSchema);
		const schemaName = collection.compMod.keyspaceName;
		const fullName = getTableName(tableName, schemaName);

		return AlterScriptDto.getInstance([ddlProvider.dropTable(fullName)], true, true);
	};

	/**
	 * @param {Collection} collection
	 * @return {Array<AlterScriptDto>}
	 * */
	const getModifyCollectionScript = collection => {
		const jsonSchema = { ...collection, ...(collection?.role || {}) };
		const schemaName = collection.compMod.keyspaceName;
		const schemaData = { schemaName };
		const idToNameHashTable = generateIdToNameHashTable(jsonSchema);
		const idToActivatedHashTable = generateIdToActivatedHashTable(jsonSchema);
		const tableName = getEntityName(jsonSchema);
		const fullTableName = getTableName(tableName, schemaName);

		const indexesScripts = modifyGroupItems({
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
					? AlterScriptDto.getInstances([
						ddlProvider.dropIndex(tableName, index),
						ddlProvider.createIndex(tableName, index, null)
					], true, true)
					: AlterScriptDto.getInstance([ddlProvider.createIndex(tableName, index, schemaData)], true, false),
			drop: (tableName, index) => AlterScriptDto.getInstance([ddlProvider.dropIndex(tableName, index)], true, true),
		});

		const checkConstraintsScripts = modifyGroupItems({
			data: jsonSchema,
			key: 'chkConstr',
			hydrate: ddlProvider.hydrateCheckConstraint,
			create: (tableName, checkConstraint) =>
				checkConstraint.orReplace
					? AlterScriptDto.getInstances([
						ddlProvider.dropConstraint(fullTableName, checkConstraint.name),
						ddlProvider.alterTableAddCheckConstraint(fullTableName, checkConstraint)
					], true, false)
					: AlterScriptDto.getInstance([ddlProvider.alterTableAddCheckConstraint(fullTableName, checkConstraint)], true, false),
			drop: (tableName, checkConstraint) => AlterScriptDto.getInstance([ddlProvider.dropConstraint(fullTableName, checkConstraint.name)], true, true),
		});

		const modifyTableOptionsScriptDto = AlterScriptDto.getInstance([ddlProvider.alterTableOptions(jsonSchema, schemaData, idToNameHashTable)], true, false);

		return [modifyTableOptionsScriptDto, ...checkConstraintsScripts, ...indexesScripts].filter(Boolean);
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

				const script = ddlProvider.convertColumnDefinition(columnDefinition);

				return AlterScriptDto.getInstance([ddlProvider.addColumn(fullName, script)], true, false);
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
			.map(
				([name]) => AlterScriptDto.getInstance([ddlProvider.dropColumn(fullName, name)], true, true)
			).filter(Boolean);
	};

	/**
	 * @param {Collection} collection
	 * @return {Array<AlterScriptDto> | undefined}
	 * */
	const getModifyColumnScript = collection => {
		const collectionSchema = { ...collection, ...(_.omit(collection?.role, 'properties') || {}) };
		const tableName = collectionSchema?.code || collectionSchema?.collectionName || collectionSchema?.name;
		const schemaName = collectionSchema.compMod?.keyspaceName;
		const fullName = getTableName(tableName, schemaName);

		const renameColumnScriptsDto = getRenameColumnScriptsDto(collection.properties, fullName);
		const changeTypeScriptsDto = getChangeTypeScriptsDto(collection.properties, fullName, collectionSchema, schemaName);

		return [...renameColumnScriptsDto, ...changeTypeScriptsDto].filter(Boolean);
	};

	const hydrateIndex =
		({ idToNameHashTable, idToActivatedHashTable, ddlProvider, tableData, schemaData }) =>
			index => {
				index = setIndexKeys(idToNameHashTable, idToActivatedHashTable, index);

				return ddlProvider.hydrateIndex(index, tableData, schemaData);
			};

	const getTableUpdateCommentScript = ({
											 schemaName,
											 tableName,
											 comment
										 }) => ddlProvider.updateTableComment({ schemaName, tableName, comment });
	const getTableDropCommentScript = ({ schemaName, tableName }) => ddlProvider.dropTableComment({
		schemaName,
		tableName
	});

	const getTablesDropCommentAlterScripts = (tables) => {
		return Object.keys(tables).map(tableName => {
			if (!tables[tableName]?.compMod?.deleted) {
				return '';
			}
			const schemaName = tables[tableName].role?.compMod.keyspaceName;
			return getTableDropCommentScript({ schemaName, tableName });
		});
	};

	const getTablesModifyCommentsAlterScripts = (tables) => {
		return Object.keys(tables).map(tableName => {

			const tableComparison = tables[tableName].role?.compMod;
			const schemaName = tableComparison.keyspaceName;

			const newComment = tableComparison?.description?.new;
			const oldComment = tableComparison?.description?.old;

			const isCommentRemoved = oldComment && !newComment;
			if (isCommentRemoved) {
				return getTableDropCommentScript({ schemaName, tableName });
			}

			return newComment ? getTableUpdateCommentScript({ schemaName, tableName, comment: newComment }) : '';

		});
	};

	const getColumnCreateCommentScript = ({
											  schemaName,
											  tableName,
											  columnName,
											  comment
										  }) => ddlProvider.createColumnComment({
		schemaName,
		tableName,
		columnName,
		comment
	});
	const getColumnUpdateCommentScript = ({
											  schemaName,
											  tableName,
											  columnName,
											  comment
										  }) => ddlProvider.updateColumnComment({
		schemaName,
		tableName,
		columnName,
		comment
	});
	const getColumnDropCommentScript = ({
											schemaName,
											tableName,
											columnName
										}) => ddlProvider.dropColumnComment({ schemaName, tableName, columnName });

	const getColumnsCreateCommentAlterScripts = (tables) => {
		return Object.keys(tables).map(tableName => {
			const columns = tables[tableName].properties;
			if (!columns) {
				return [];
			}
			const schemaName = tables[tableName].role?.compMod.keyspaceName;
			return Object.keys(columns).map(columnName => {
				const comment = columns[columnName].description;
				const oldComment = tables[tableName].role?.properties[columnName]?.description;
				if (!comment || oldComment) {
					return '';
				}
				return getColumnCreateCommentScript({ schemaName, tableName, columnName, comment });
			});
		}).flat();
	};

	const getColumnsDropCommentAlterScripts = (tables) => {
		return Object.keys(tables).map(tableName => {
			const columns = tables[tableName].properties;
			if (!columns) {
				return [];
			}
			const schemaName = tables[tableName].role?.compMod.keyspaceName;
			return Object.keys(columns).map(columnName => getColumnDropCommentScript({
				schemaName,
				tableName,
				columnName
			}));
		}).flat();
	};

	const getColumnsModifyCommentAlterScripts = (tables) => {
		return Object.keys(tables).map(tableName => {
			const columns = tables[tableName].properties;
			if (!columns) {
				return [];
			}
			const schemaName = tables[tableName].role?.compMod.keyspaceName;
			return Object.keys(columns).map(columnName => {
				const newComment = columns[columnName]?.description;
				const oldComment = tables[tableName].role?.properties[columnName]?.description;

				const isCommentRemoved = oldComment && !newComment;
				if (isCommentRemoved) {
					return getColumnDropCommentScript({ schemaName, tableName, columnName });
				}

				return newComment ? getColumnUpdateCommentScript({
					schemaName,
					tableName,
					columnName,
					comment: newComment
				}) : '';
			});
		}).flat();
	};

	return {
		getAddCollectionScriptDto,
		getDeleteCollectionScriptDto,
		getModifyCollectionScriptDto: getModifyCollectionScript,
		getAddColumnScriptDto,
		getDeleteColumnScriptDto,
		getModifyColumnScriptDto: getModifyColumnScript,
		getTablesDropCommentAlterScripts,
		getTablesModifyCommentsAlterScripts,
		getColumnsCreateCommentAlterScripts,
		getColumnsDropCommentAlterScripts,
		getColumnsModifyCommentAlterScripts,
	};
};
