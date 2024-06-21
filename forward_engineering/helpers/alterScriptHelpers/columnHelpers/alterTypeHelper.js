'use strict';

module.exports = (app, ddlProvider) => {
	const _ = app.require('lodash');
	const { checkFieldPropertiesChanged } = require('../common')(app);
	const { createColumnDefinitionBySchema } = require('./createColumnDefinition')(_);
	const { AlterScriptDto } = require('../types/AlterScriptDto');

	const getChangeTypeScriptsDto = (collectionProperties, fullName, collectionSchema, schemaName) => {
		const schemaData = { schemaName };

		return _.toPairs(collectionProperties)
			.filter(([name, jsonSchema]) => checkFieldPropertiesChanged(jsonSchema.compMod, ['type', 'mode']))
			.map(([name, jsonSchema]) => {
				const columnDefinition = createColumnDefinitionBySchema({
					name,
					jsonSchema,
					parentJsonSchema: collectionSchema,
					ddlProvider,
					schemaData,
				});

				return AlterScriptDto.getInstance([ddlProvider.alterColumn(fullName, columnDefinition)], true, false);
			});
	};

	return {
		getChangeTypeScriptsDto,
	};
};
