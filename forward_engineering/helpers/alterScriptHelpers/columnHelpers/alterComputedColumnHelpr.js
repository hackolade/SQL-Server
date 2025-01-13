'use strict';

module.exports = (app, ddlProvider) => {
	const _ = app.require('lodash');
	const { checkFieldPropertiesChanged } = require('../common')(app);
	const { createColumnDefinitionBySchema } = require('./createColumnDefinition')(_);
	const { AlterScriptDto } = require('../types/AlterScriptDto');

	function generateSqlAlterScript({
		collectionSchema,
		prevJsonSchema,
		jsonSchema,
		fullName,
		columnName,
		schemaName,
	}) {
		const schemaData = { schemaName };
		const sqlScript = [];

		const columnDefinition = createColumnDefinitionBySchema({
			name: fullName,
			jsonSchema,
			parentJsonSchema: collectionSchema,
			ddlProvider,
			schemaData,
		});

		const isComputedRemoved = prevJsonSchema.computed && !jsonSchema.computed;
		const isComputedEnabled = !prevJsonSchema.computed && jsonSchema.computed;
		const isComputedModified =
			prevJsonSchema.computed &&
			jsonSchema.computed &&
			(prevJsonSchema.computedExpression !== jsonSchema.computedExpression ||
				prevJsonSchema.persisted !== jsonSchema.persisted);

		if (isComputedRemoved) {
			sqlScript.push(
				AlterScriptDto.getInstance([ddlProvider.dropColumn(fullName, columnName)], true, true),
				AlterScriptDto.getInstance([ddlProvider.alterColumn(fullName, columnDefinition)], true, false),
			);
		}

		if (isComputedEnabled && jsonSchema.computedExpression) {
			sqlScript.push(
				AlterScriptDto.getInstance([ddlProvider.dropColumn(fullName, columnName)], true, true),
				AlterScriptDto.getInstance(
					[ddlProvider.alterComputedColumn(fullName, columnName, columnDefinition)],
					true,
					false,
				),
			);
		}

		if (isComputedModified) {
			if (jsonSchema.computedExpression) {
				sqlScript.push(
					AlterScriptDto.getInstance([ddlProvider.dropColumn(fullName, columnName)], true, true),
					AlterScriptDto.getInstance(
						[ddlProvider.alterComputedColumn(fullName, columnName, columnDefinition)],
						true,
						false,
					),
				);
			} else {
				sqlScript.push(
					AlterScriptDto.getInstance([ddlProvider.dropColumn(fullName, columnName)], true, true),
					AlterScriptDto.getInstance([ddlProvider.alterColumn(fullName, columnDefinition)], true, false),
				);
			}
		}

		return sqlScript;
	}

	const getChangedComputedColumnsScriptsDto = (collection, fullName, collectionSchema, schemaName) => {
		return _.flatten(
			_.toPairs(collection.properties).reduce((result, [columnName, jsonSchema]) => {
				const oldJsonSchema = _.omit(collection.role?.properties?.[columnName], ['compMod']);

				result.push(
					generateSqlAlterScript({
						collectionSchema,
						prevJsonSchema: oldJsonSchema,
						jsonSchema,
						fullName,
						columnName,
						schemaName,
					}),
				);

				return result;
			}, []),
		);
	};

	return {
		getChangedComputedColumnsScriptsDto,
	};
};
