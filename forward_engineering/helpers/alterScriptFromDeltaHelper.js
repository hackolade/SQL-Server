const getComparisonModelCollection = collections => {
	return collections
		.map(collection => JSON.parse(collection))
		.find(collection => collection.collectionName === 'comparisonModelCollection');
};

const getAlterContainersScripts = (collection, app, options) => {
	const { getAddContainerScript, getDeleteContainerScript } = require('./alterScriptHelpers/alterContainerHelper')(
		app,
		options,
	);

	const addedContainers = collection.properties?.containers?.properties?.added?.items;
	const deletedContainers = collection.properties?.containers?.properties?.deleted?.items;

	const addContainersScripts = []
		.concat(addedContainers)
		.filter(Boolean)
		.map(container => ({ ...Object.values(container.properties)[0], name: Object.keys(container.properties)[0] }))
		.map(getAddContainerScript);
	const deleteContainersScripts = []
		.concat(deletedContainers)
		.filter(Boolean)
		.map(container => getDeleteContainerScript(Object.keys(container.properties)[0]));
	
	return { addContainersScripts, deleteContainersScripts };
};

const getAlterCollectionsScripts = (collection, app, options) => {
	const {
		getAddCollectionScript,
		getDeleteCollectionScript,
		getAddColumnScript,
		getDeleteColumnScript,
		getModifyColumnScript,
		getModifyCollectionScript,
	} = require('./alterScriptHelpers/alterEntityHelper')(app, options);

	const createCollectionsScripts = []
		.concat(collection.properties?.entities?.properties?.added?.items)
		.filter(Boolean)
		.map(item => Object.values(item.properties)[0])
		.filter(collection => collection.compMod?.created)
		.map(getAddCollectionScript);
	const deleteCollectionScripts = []
		.concat(collection.properties?.entities?.properties?.deleted?.items)
		.filter(Boolean)
		.map(item => Object.values(item.properties)[0])
		.filter(collection => collection.compMod?.deleted)
		.map(getDeleteCollectionScript);
	const modifyCollectionScripts = []
		.concat(collection.properties?.entities?.properties?.modified?.items)
		.filter(Boolean)
		.map(item => Object.values(item.properties)[0])
		.filter(collection => collection.compMod?.modified)
		.map(getModifyCollectionScript);
	const addColumnScripts = []
		.concat(collection.properties?.entities?.properties?.added?.items)
		.filter(Boolean)
		.map(item => Object.values(item.properties)[0])
		.filter(collection => !collection.compMod?.created)
		.flatMap(getAddColumnScript);
	const deleteColumnScripts = []
		.concat(collection.properties?.entities?.properties?.deleted?.items)
		.filter(Boolean)
		.map(item => Object.values(item.properties)[0])
		.filter(collection => !collection.compMod?.deleted)
		.flatMap(getDeleteColumnScript);
	const modifyColumnScript = []
		.concat(collection.properties?.entities?.properties?.modified?.items)
		.filter(Boolean)
		.map(item => Object.values(item.properties)[0])
		.flatMap(getModifyColumnScript);

	return {
		createCollectionsScripts,
		deleteCollectionScripts,
		modifyCollectionScripts,
		addColumnScripts,
		deleteColumnScripts,
		modifyColumnScript
	};
};

const getAlterViewScripts = (collection, app, options) => {
	const { getAddViewScript, getDeleteViewScript, getModifiedViewScript } =
		require('./alterScriptHelpers/alterViewHelper')(app, options);

	const createViewsScripts = []
		.concat(collection.properties?.views?.properties?.added?.items)
		.filter(Boolean)
		.map(item => Object.values(item.properties)[0])
		.map(view => ({ ...view, ...(view.role || {}) }))
		.filter(view => view.compMod?.created)
		.map(getAddViewScript);

	const deleteViewsScripts = []
		.concat(collection.properties?.views?.properties?.deleted?.items)
		.filter(Boolean)
		.map(item => Object.values(item.properties)[0])
		.map(view => ({ ...view, ...(view.role || {}) }))
		.filter(view => view.compMod?.deleted)
		.map(getDeleteViewScript);

	const modifiedViewsScripts = []
		.concat(collection.properties?.views?.properties?.modified?.items)
		.filter(Boolean)
		.map(item => Object.values(item.properties)[0])
		.map(view => ({ ...view, ...(view.role || {}) }))
		.filter(view => !view.compMod?.created && !view.compMod?.deleted)
		.map(getModifiedViewScript);

	return { deleteViewsScripts, createViewsScripts, modifiedViewsScripts };
};

const getAlterModelDefinitionsScripts = (collection, app, options) => {
	const { getCreateUdtScript, getDeleteUdtScript } = require('./alterScriptHelpers/alterUdtHelper')(app, options);

	const createUdtScripts = []
		.concat(collection.properties?.modelDefinitions?.properties?.added?.items)
		.filter(Boolean)
		.map(item => Object.values(item.properties)[0])
		.map(item => ({ ...item, ...(app.require('lodash').omit(item.role, 'properties') || {}) }))
		.filter(item => item.compMod?.created)
		.flatMap(getCreateUdtScript);
	const deleteUdtScripts = []
		.concat(collection.properties?.modelDefinitions?.properties?.deleted?.items)
		.filter(Boolean)
		.map(item => Object.values(item.properties)[0])
		.map(item => ({ ...item, ...(app.require('lodash').omit(item.role, 'properties') || {}) }))
		.filter(collection => collection.compMod?.deleted)
		.flatMap(getDeleteUdtScript);

	return { deleteUdtScripts, createUdtScripts };
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
		addSchemasModifyCommentsScripts = Array.isArray(modifiedSchemas) ? modifiedSchemas.map(schema => getSchemasModifyCommentsAlterScripts(schema?.properties)).flatMap(schema => schema) : getSchemasModifyCommentsAlterScripts(modifiedSchemas?.properties)
	}

	if (deletedSchemas) {
		addSchemasDropCommentsScripts = Array.isArray(deletedSchemas) ? deletedSchemas.map(schema => getSchemasDropCommentsAlterScripts(schema?.properties)).flatMap(schema => schema) : getSchemasDropCommentsAlterScripts(deletedSchemas?.properties)
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
		addColumnCreateCommentsScrips = Array.isArray(modifiedTables) ? modifiedTables.map(schema => getColumnsCreateCommentAlterScripts(schema?.properties)).flatMap(schema => schema) : getColumnsCreateCommentAlterScripts(modifiedTables?.properties)
		addTablesModifyCommentsScripts = Array.isArray(modifiedTables) ? modifiedTables.map(schema => getTablesModifyCommentsAlterScripts(schema?.properties)).flatMap(schema => schema) : getTablesModifyCommentsAlterScripts(modifiedTables?.properties)
		addColumnModifyCommentsScripts = Array.isArray(modifiedTables) ? modifiedTables.map(schema => getColumnsModifyCommentAlterScripts(schema?.properties)).flatMap(schema => schema) : getColumnsModifyCommentAlterScripts(modifiedTables?.properties)
	}

	if (deletedTables) {
		addTablesDropCommentsScripts = Array.isArray(deletedTables) ? deletedTables.map(schema => getTablesDropCommentAlterScripts(schema?.properties)).flatMap(schema => schema) : getTablesDropCommentAlterScripts(deletedTables?.properties)
		addColumnDropCommentsScripts = Array.isArray(deletedTables) ? deletedTables.map(schema => getColumnsDropCommentAlterScripts(schema?.properties)).flatMap(schema => schema) : getColumnsDropCommentAlterScripts(deletedTables?.properties)
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
		addViewsModifyCommentsScripts = Array.isArray(modifiedViews) ? modifiedViews.map(schema => getViewsModifyCommentsAlterScripts(schema?.properties)).flatMap(schema => schema) : getViewsModifyCommentsAlterScripts(modifiedViews?.properties)
	}

	if (deletedViews) {
		addViewsDropCommentsScripts = Array.isArray(deletedViews) ? deletedViews.map(schema => getViewsDropCommentAlterScripts(schema?.properties)).flatMap(schema => schema) : getViewsDropCommentAlterScripts(deletedViews?.properties)
	}

	return { 
		addViewsModifyCommentsScripts, 
		addViewsDropCommentsScripts
	}
}

const getAlterScript = (collection, app, options) => {
	const script = {
		...getAlterCollectionsScripts(collection, app, options),
		...getAlterContainersScripts(collection, app, options),
		...getAlterViewScripts(collection, app, options),
		...getAlterModelDefinitionsScripts(collection, app, options),
		...getContainersCommentsAlterScripts(collection, app, options),
		...getCollectionsCommentsAlterScripts(collection, app, options),
		...getViewsCommentsAlterScripts(collection, app, options)
	}
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
		'addColumnScripts',
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
	].flatMap(name => script[name] || []).map(script => script.trim()).filter(Boolean).join('\n\n');
};

module.exports = {
	getAlterScript,
	getComparisonModelCollection,
	getAlterContainersScripts,
	getAlterCollectionsScripts,
	getAlterViewScripts,
	getAlterModelDefinitionsScripts,
};
