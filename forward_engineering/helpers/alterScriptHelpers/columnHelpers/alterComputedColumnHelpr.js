'use strict';

module.exports = (app, ddlProvider) => {
	const _ = app.require('lodash');
	const { createColumnDefinitionBySchema } = require('./createColumnDefinition')(_);
	const { AlterScriptDto } = require('../types/AlterScriptDto');

	const changeToComputed = (fullName, columnName, columnDefinition) => {
		return [
			AlterScriptDto.getInstance([ddlProvider.dropColumn(fullName, columnName)], true, true),
			AlterScriptDto.getInstance(
				[ddlProvider.alterComputedColumn(fullName, columnName, columnDefinition)],
				true,
				false,
			),
		];
	};

	const changeToNonComputed = (fullName, columnName, columnDefinition) => {
		return [
			AlterScriptDto.getInstance([ddlProvider.dropColumn(fullName, columnName)], true, true),
			AlterScriptDto.getInstance([ddlProvider.alterColumn(fullName, columnDefinition)], true, false),
		];
	};

	const generateSqlAlterScript = ({
		collectionSchema,
		prevJsonSchema,
		jsonSchema,
		fullName,
		columnName,
		schemaName,
	}) => {
		const schemaData = { schemaName };
		const columnDefinition = createColumnDefinitionBySchema({
			name: columnName,
			jsonSchema,
			parentJsonSchema: collectionSchema,
			ddlProvider,
			schemaData,
		});

		let sqlScripts = [];

		const isComputedRemoved = prevJsonSchema.computed && !jsonSchema.computed;
		const isComputedEnabled = !prevJsonSchema.computed && jsonSchema.computed;
		const isComputedModified =
			prevJsonSchema.computed &&
			jsonSchema.computed &&
			(prevJsonSchema.computedExpression !== jsonSchema.computedExpression ||
				prevJsonSchema.persisted !== jsonSchema.persisted);

		if ((isComputedRemoved || isComputedModified) && !jsonSchema.computedExpression) {
			sqlScripts = changeToNonComputed(fullName, columnName, columnDefinition);
		}

		if ((isComputedEnabled || isComputedModified) && jsonSchema.computedExpression) {
			sqlScripts = changeToComputed(fullName, columnName, columnDefinition);
		}

		return sqlScripts;
	};

	const getChangedComputedColumnsScriptsDto = ({ collection, fullName, collectionSchema, schemaName }) => {
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
