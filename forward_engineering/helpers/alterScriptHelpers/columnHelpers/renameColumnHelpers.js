'use strict';

module.exports = (app, ddlProvider) => {
	const _ = app.require('lodash');
	const { checkFieldPropertiesChanged } = require('../common')(app);
	const { AlterScriptDto } = require('../types/AlterScriptDto');

	const getRenameColumnScriptsDto = (collectionProperties, fullName) => {
		return _.values(collectionProperties)
			.filter(jsonSchema => checkFieldPropertiesChanged(jsonSchema.compMod, ['name']))
			.map(jsonSchema => {
				const script = ddlProvider.renameColumn(fullName, jsonSchema.compMod.oldField.name, jsonSchema.compMod.newField.name);

				return AlterScriptDto.getInstance([script], true, false);
			});
	};

	return {
		getRenameColumnScriptsDto,
	};
};