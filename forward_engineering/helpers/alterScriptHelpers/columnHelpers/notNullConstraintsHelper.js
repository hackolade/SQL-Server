'use strict';

const { AlterScriptDto } = require('../types/AlterScriptDto');

/**
 * @return {(collection: Collection) => AlterScriptDto[]}
 * */
const getModifyNonNullColumnsScriptDtos = (_, ddlProvider) => (collection, collectionSchema, schemaName) => {
	const { getFullTableName, wrapInBrackets } = require('../../../utils/general')(_);
	const { createColumnDefinitionBySchema } = require('./createColumnDefinition')(_);
	const fullTableName = getFullTableName(collection);
	const schemaData = { schemaName };

	const currentRequiredColumnNames = collection.required || [];
	const previousRequiredColumnNames = collection.role.required || [];

	const columnNamesToAddNotNullConstraint = _.difference(currentRequiredColumnNames, previousRequiredColumnNames);
	const columnNamesToRemoveNotNullConstraint = _.difference(previousRequiredColumnNames, currentRequiredColumnNames);

	const addNotNullConstraintsScript = _.toPairs(collection.properties)
		.filter(([name, jsonSchema]) => {
			const oldName = jsonSchema.compMod.oldField.name;
			const shouldRemoveForOldName = columnNamesToRemoveNotNullConstraint.includes(oldName);
			const shouldAddForNewName = columnNamesToAddNotNullConstraint.includes(name);
			return shouldAddForNewName && !shouldRemoveForOldName;
		})
		.map(([columnName, jsonSchema]) => {
			const columnDefinition = createColumnDefinitionBySchema({
				name,
				jsonSchema,
				parentJsonSchema: collectionSchema,
				ddlProvider,
				schemaData,
			});

			return ddlProvider.setNotNullConstraint(fullTableName, wrapInBrackets(columnName), columnDefinition);
		})
		.map(script => AlterScriptDto.getInstance([script], true, false));

	const removeNotNullConstraint = _.toPairs(collection.properties)
		.filter(([name, jsonSchema]) => {
			const oldName = jsonSchema.compMod.oldField.name;
			const shouldRemoveForOldName = columnNamesToRemoveNotNullConstraint.includes(oldName);
			const shouldAddForNewName = columnNamesToAddNotNullConstraint.includes(name);
			return shouldRemoveForOldName && !shouldAddForNewName;
		})
		.map(([name, jsonSchema]) => {
			const columnDefinition = createColumnDefinitionBySchema({
				name,
				jsonSchema,
				parentJsonSchema: collectionSchema,
				ddlProvider,
				schemaData,
			});

			return ddlProvider.dropNotNullConstraint(fullTableName, wrapInBrackets(name), columnDefinition);
		})
		.map(script => AlterScriptDto.getInstance([script], true, true));

	return [...addNotNullConstraintsScript, ...removeNotNullConstraint];
};

module.exports = {
	getModifyNonNullColumnsScriptDtos
};
