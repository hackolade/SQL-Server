const { buildScript, commentDeactivatedStatements } = require('../utils/generalUtils');

/**
 * @typedef {import('./alterScriptHelpers/types/AlterScriptDto').AlterScriptDto} AlterScriptDto
 * */

/**
 * @param scripts {Array<string>}
 * @return {Array<string>}
 * */
const assertNoEmptyStatements = (scripts) => {
	return scripts
		.filter(Boolean)
		.map(script => script.trim())
		.filter(Boolean);
};

const getComparisonModelCollection = collections => {
	return collections
		.map(collection => JSON.parse(collection))
		.find(collection => collection.collectionName === 'comparisonModelCollection');
};

/**
 * @return Array<AlterScriptDto>
 * */
const getAlterContainersScriptsDtos = (collection, app, options) => {
	const { getAddContainerScript, getDeleteContainerScript } = require('./alterScriptHelpers/alterContainerHelper')(
		app,
		options,
	);

	const addedContainers = collection.properties?.containers?.properties?.added?.items;
	const deletedContainers = collection.properties?.containers?.properties?.deleted?.items;

	const addContainersScriptsDtos = []
		.concat(addedContainers)
		.filter(Boolean)
		.map(container => ({ ...Object.values(container.properties)[0], name: Object.keys(container.properties)[0] }))
		.map(getAddContainerScript);
	const deleteContainersScriptsDtos = []
		.concat(deletedContainers)
		.filter(Boolean)
		.map(container => getDeleteContainerScript(Object.keys(container.properties)[0]));

	return { addContainersScriptsDtos, deleteContainersScriptsDtos };
};

/**
 * @return Array<AlterScriptDto>
 * */
const getAlterCollectionsScriptsDtos = (collection, app, options) => {
	const {
		getAddCollectionScriptDto,
		getDeleteCollectionScriptDto,
		getAddColumnScriptDto,
		getDeleteColumnScriptDto,
		getModifyColumnScriptDto,
		getModifyCollectionScriptDto,
	} = require('./alterScriptHelpers/alterEntityHelper')(app, options);

	const createCollectionsScriptsDtos = []
		.concat(collection.properties?.entities?.properties?.added?.items)
		.filter(Boolean)
		.map(item => Object.values(item.properties)[0])
		.filter(collection => collection.compMod?.created)
		.flatMap(getAddCollectionScriptDto);
	const deleteCollectionScriptsDtos = []
		.concat(collection.properties?.entities?.properties?.deleted?.items)
		.filter(Boolean)
		.map(item => Object.values(item.properties)[0])
		.filter(collection => collection.compMod?.deleted)
		.map(getDeleteCollectionScriptDto);
	const modifyCollectionScriptsDtos = []
		.concat(collection.properties?.entities?.properties?.modified?.items)
		.filter(Boolean)
		.map(item => Object.values(item.properties)[0])
		.filter(collection => collection.compMod?.modified)
		.flatMap(getModifyCollectionScriptDto);
	const addColumnScriptsDtos = []
		.concat(collection.properties?.entities?.properties?.added?.items)
		.filter(Boolean)
		.map(item => Object.values(item.properties)[0])
		.filter(collection => !collection.compMod?.created)
		.flatMap(getAddColumnScriptDto);
	const deleteColumnScriptsDtos = []
		.concat(collection.properties?.entities?.properties?.deleted?.items)
		.filter(Boolean)
		.map(item => Object.values(item.properties)[0])
		.filter(collection => !collection.compMod?.deleted)
		.flatMap(getDeleteColumnScriptDto);
	const modifyColumnScriptDtos = []
		.concat(collection.properties?.entities?.properties?.modified?.items)
		.filter(Boolean)
		.map(item => Object.values(item.properties)[0])
		.flatMap(getModifyColumnScriptDto);

	return {
		createCollectionsScriptsDtos,
		deleteCollectionScriptsDtos,
		modifyCollectionScriptsDtos,
		addColumnScriptsDtos,
		deleteColumnScriptsDtos,
		modifyColumnScriptDtos
	};
};

/**
 * @return Array<AlterScriptDto>
 * */
const getAlterViewScriptsDtos = (collection, app, options) => {
	const { getAddViewScript, getDeleteViewScript, getModifiedViewScript } =
		require('./alterScriptHelpers/alterViewHelper')(app, options);

	const createViewsScriptsDtos = []
		.concat(collection.properties?.views?.properties?.added?.items)
		.filter(Boolean)
		.map(item => Object.values(item.properties)[0])
		.map(view => ({ ...view, ...(view.role || {}) }))
		.filter(view => view.compMod?.created)
		.map(getAddViewScript);

	const deleteViewsScriptsDtos = []
		.concat(collection.properties?.views?.properties?.deleted?.items)
		.filter(Boolean)
		.map(item => Object.values(item.properties)[0])
		.map(view => ({ ...view, ...(view.role || {}) }))
		.filter(view => view.compMod?.deleted)
		.map(getDeleteViewScript);

	const modifiedViewsScriptsDtos = []
		.concat(collection.properties?.views?.properties?.modified?.items)
		.filter(Boolean)
		.map(item => Object.values(item.properties)[0])
		.map(view => ({ ...view, ...(view.role || {}) }))
		.filter(view => !view.compMod?.created && !view.compMod?.deleted)
		.flatMap(getModifiedViewScript);

	return { deleteViewsScriptsDtos, createViewsScriptsDtos, modifiedViewsScriptsDtos };
};

/**
 * @return Array<AlterScriptDto>
 * */
const getAlterModelDefinitionsScripts = (collection, app, options) => {
	const { getCreateUdtScript, getDeleteUdtScript } = require('./alterScriptHelpers/alterUdtHelper')(app, options);

	const createUdtScriptsDtos = []
		.concat(collection.properties?.modelDefinitions?.properties?.added?.items)
		.filter(Boolean)
		.map(item => Object.values(item.properties)[0])
		.map(item => ({ ...item, ...(app.require('lodash').omit(item.role, 'properties') || {}) }))
		.filter(item => item.compMod?.created)
		.flatMap(getCreateUdtScript);
	const deleteUdtScriptsDtos = []
		.concat(collection.properties?.modelDefinitions?.properties?.deleted?.items)
		.filter(Boolean)
		.map(item => Object.values(item.properties)[0])
		.map(item => ({ ...item, ...(app.require('lodash').omit(item.role, 'properties') || {}) }))
		.filter(collection => collection.compMod?.deleted)
		.flatMap(getDeleteUdtScript);

	return { deleteUdtScriptsDtos, createUdtScriptsDtos };
};

const getContainersCommentsAlterScripts = (collection, app, options) => {
	const { 
		getSchemasDropCommentsAlterScripts, 
		getSchemasModifyCommentsAlterScripts 
	} = require('./alterScriptHelpers/alterContainerHelper')(
		app,
		options,
	);
	const modifiedSchemas = collection.properties?.containers?.properties?.modified?.items;
	const deletedSchemas = collection.properties?.containers?.properties?.deleted?.items;

	//There is no need for separate added schemas comments creation because it is already done in generation of ddl (just like in FE) and this method is called
	let addSchemasModifyCommentsScripts = []
	let addSchemasDropCommentsScripts = []

	if (modifiedSchemas) {
		addSchemasModifyCommentsScripts = Array.isArray(modifiedSchemas) ? modifiedSchemas.map(schema => getSchemasModifyCommentsAlterScripts(schema?.properties)).flat() : getSchemasModifyCommentsAlterScripts(modifiedSchemas?.properties)
	}

	if (deletedSchemas) {
		addSchemasDropCommentsScripts = Array.isArray(deletedSchemas) ? deletedSchemas.map(schema => getSchemasDropCommentsAlterScripts(schema?.properties)).flat() : getSchemasDropCommentsAlterScripts(deletedSchemas?.properties)
	}
	

	return { 
		addSchemasModifyCommentsScripts, 
		addSchemasDropCommentsScripts
	}
}

const getCollectionsCommentsAlterScripts = (collection, app, options) => {
	const { 
		getTablesDropCommentAlterScripts, 
		getTablesModifyCommentsAlterScripts,
		getColumnsCreateCommentAlterScripts,
		getColumnsDropCommentAlterScripts, 
		getColumnsModifyCommentAlterScripts 
	} = require('./alterScriptHelpers/alterEntityHelper')(
		app,
		options,
	);
	const modifiedTables = collection.properties?.entities?.properties?.modified?.items;
	const deletedTables = collection.properties?.entities?.properties?.deleted?.items;

	//Added tables comments creation is already done in generation of ddl
	let addTablesModifyCommentsScripts = []
	let addTablesDropCommentsScripts = []

	// Columns create scripts added for case with modification of tables with new fields with comments
	let addColumnCreateCommentsScrips = []
	let addColumnModifyCommentsScripts = []
	let addColumnDropCommentsScripts = []

	if (modifiedTables) {
		addColumnCreateCommentsScrips = Array.isArray(modifiedTables) ? modifiedTables.map(schema => getColumnsCreateCommentAlterScripts(schema?.properties)).flat() : getColumnsCreateCommentAlterScripts(modifiedTables?.properties)
		addTablesModifyCommentsScripts = Array.isArray(modifiedTables) ? modifiedTables.map(schema => getTablesModifyCommentsAlterScripts(schema?.properties)).flat() : getTablesModifyCommentsAlterScripts(modifiedTables?.properties)
		addColumnModifyCommentsScripts = Array.isArray(modifiedTables) ? modifiedTables.map(schema => getColumnsModifyCommentAlterScripts(schema?.properties)).flat() : getColumnsModifyCommentAlterScripts(modifiedTables?.properties)
	}

	if (deletedTables) {
		addTablesDropCommentsScripts = Array.isArray(deletedTables) ? deletedTables.map(schema => getTablesDropCommentAlterScripts(schema?.properties)).flat() : getTablesDropCommentAlterScripts(deletedTables?.properties)
		addColumnDropCommentsScripts = Array.isArray(deletedTables) ? deletedTables.map(schema => getColumnsDropCommentAlterScripts(schema?.properties)).flat() : getColumnsDropCommentAlterScripts(deletedTables?.properties)
	}

	return { 
		addTablesModifyCommentsScripts, 
		addTablesDropCommentsScripts,
		addColumnCreateCommentsScrips,
		addColumnModifyCommentsScripts, 
		addColumnDropCommentsScripts 
	}
}

const getViewsCommentsAlterScripts = (collection, app, options) => {
	const { 
		getViewsDropCommentAlterScripts, 
		getViewsModifyCommentsAlterScripts, 
	} = require('./alterScriptHelpers/alterViewHelper')(
		app,
		options,
	);

	//Added views comments creation is already done in generation of ddl
	const modifiedViews = collection.properties?.views?.properties?.modified?.items;
	const deletedViews = collection.properties?.views?.properties?.deleted?.items;

	let addViewsModifyCommentsScripts = []
	let addViewsDropCommentsScripts = []

	if (modifiedViews) {
		addViewsModifyCommentsScripts = Array.isArray(modifiedViews) ? modifiedViews.map(schema => getViewsModifyCommentsAlterScripts(schema?.properties)).flat() : getViewsModifyCommentsAlterScripts(modifiedViews?.properties)
	}

	if (deletedViews) {
		addViewsDropCommentsScripts = Array.isArray(deletedViews) ? deletedViews.map(schema => getViewsDropCommentAlterScripts(schema?.properties)).flat() : getViewsDropCommentAlterScripts(deletedViews?.properties)
	}

	return { 
		addViewsModifyCommentsScripts, 
		addViewsDropCommentsScripts
	}
}

/**
 * @param scriptDtos {Array<AlterScriptDto>},
 * @param data {{
 *     options: {
 *         id: string,
 *         value: any,
 *     },
 * }}
 * @return {Array<string>}
 * */
const getAlterStatementsWithCommentedUnwantedDDL = (scriptDtos, data) => {
	const { additionalOptions = [] } = data.options || {};
	const applyDropStatements = (additionalOptions.find(option => option.id === 'applyDropStatements') || {}).value;

	const scripts = scriptDtos.map((dto) => {
		if (dto.isActivated === false) {
			return dto.scripts
				.map((scriptDto) => commentDeactivatedStatements(scriptDto.script, false));
		}

		if (!applyDropStatements) {
			return dto.scripts
				.map((scriptDto) => commentDeactivatedStatements(scriptDto.script, !scriptDto.isDropScript));
		}

		return dto.scripts.map((scriptDto) => scriptDto.script);
	})
		.flat();
	return assertNoEmptyStatements(scripts);
};

/**
 * @return Array<AlterScriptDto>
 * */
const getAlterScriptDtos = (collection, app, options) => {
	const script = {
		...getAlterCollectionsScriptsDtos(collection, app, options),
		...getAlterContainersScriptsDtos(collection, app, options),
		...getAlterViewScriptsDtos(collection, app, options),
		...getAlterModelDefinitionsScripts(collection, app, options),
		...getContainersCommentsAlterScripts(collection, app, options),
		...getCollectionsCommentsAlterScripts(collection, app, options),
		...getViewsCommentsAlterScripts(collection, app, options)
	};

	return [
		'addContainersScripts',
		'addViewsDropCommentsScripts',
		'deleteViewsScripts',
		'addTablesDropCommentsScripts',
		'deleteCollectionScripts',
		'addColumnDropCommentsScripts',
		'deleteColumnScripts',
		'deleteUdtScripts',
		'createUdtScripts',
		'createCollectionsScripts',
		'modifyCollectionScripts',
		'addColumnScriptsDtos',
		'modifyColumnScript',
		'createViewsScripts',
		'modifiedViewsScripts',
		'addSchemasDropCommentsScripts',
		'deleteContainersScripts',
		'addColumnCreateCommentsScrips',
		'addColumnModifyCommentsScripts',
		'addSchemasModifyCommentsScripts',
		'addTablesModifyCommentsScripts',
		'addViewsModifyCommentsScripts',
	].flatMap(name => script[name]).filter(Boolean);
};

/**
 * @param alterScriptDtos {Array<AlterScriptDto>}
 * @param data {{
 *     options: {
 *         id: string,
 *         value: any,
 *     },
 * }}
 * @return {string}
 * */
const joinAlterScriptDtosIntoAlterScript = (alterScriptDtos, data) => {
	const scriptAsStringsWithCommentedUnwantedDDL = getAlterStatementsWithCommentedUnwantedDDL(alterScriptDtos, data);

	return buildScript(scriptAsStringsWithCommentedUnwantedDDL);
};

module.exports = {
	getAlterScriptDtos,
	getComparisonModelCollection,
	getAlterContainersScripts: getAlterContainersScriptsDtos,
	getAlterCollectionsScripts: getAlterCollectionsScriptsDtos,
	getAlterViewScripts: getAlterViewScriptsDtos,
	getAlterModelDefinitionsScripts,
	joinAlterScriptDtosIntoAlterScript
};
