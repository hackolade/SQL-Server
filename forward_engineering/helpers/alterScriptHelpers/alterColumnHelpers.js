'use strict';

module.exports = (app, ddlProvider) => {
	const _ = app.require('lodash');
	const { checkFieldPropertiesChanged, modifyGroupItems, setIndexKeys } = require('./common')(app);
	const { createColumnDefinitionBySchema } = require('./createColumnDefinition')(_);
	const { AlterScriptDto } = require('./types/AlterScriptDto');

	const getRenameColumnScripts = (collectionProperties, fullName) => {
		return _.values(collectionProperties)
			.filter(jsonSchema => checkFieldPropertiesChanged(jsonSchema.compMod, ['name']))
			.map(jsonSchema => {
				const script = ddlProvider.renameColumn(fullName, jsonSchema.compMod.oldField.name, jsonSchema.compMod.newField.name);

				return AlterScriptDto.getInstance([script], true, false);
			});
	};

	const getChangeTypeScripts = (collectionProperties, fullName, collectionSchema, schemaName) => {
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
		getRenameColumnScripts,
		getChangeTypeScripts
	};
};