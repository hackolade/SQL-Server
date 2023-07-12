module.exports = (app, options) => {
	const _ = app.require('lodash');
	const { createColumnDefinitionBySchema } = require('./columnHelpers/createColumnDefinition')(_);
	const ddlProvider = require('../../ddlProvider')(null, options, app);
	const { AlterScriptDto } = require('./types/AlterScriptDto');

	const DEFAULT_KEY_SPACE = { 'Default_Keyspace': [] };

	const getSchemaNames = udt => {
		const schemaNames = udt.compMod?.bucketsWithCurrentDefinition;
		return !_.isEmpty(schemaNames) ? schemaNames : DEFAULT_KEY_SPACE;
	};

	const getCreateUdtScriptDto = jsonSchema => {
		const schemaNames = getSchemaNames(jsonSchema);

		return Object.keys(schemaNames).map(schemaName => {
			const schemaData = { schemaName };

			const udt = createColumnDefinitionBySchema({
				name: jsonSchema.code || jsonSchema.name,
				jsonSchema: jsonSchema,
				parentJsonSchema: { required: [] },
				ddlProvider,
				schemaData,
			});

			return AlterScriptDto.getInstance([ddlProvider.createUdt({ ...udt, schemaName })], true, false);
		});
	};

	const getDeleteUdtScriptDto = udt => {
		const schemaNames = getSchemaNames(udt);
		return Object.keys(schemaNames).map(schemaName => {
			const name = udt.code || udt.name || '';

			return AlterScriptDto.getInstance([ddlProvider.dropUdt({ name, schemaName })], true, true);
		});
	};

	return {
		getCreateUdtScriptDto,
		getDeleteUdtScriptDto,
	};
};
