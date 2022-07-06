module.exports = (app, options) => {
	const _ = app.require('lodash');
	const { createColumnDefinitionBySchema } = require('./createColumnDefinition')(_);
	const ddlProvider = require('../../ddlProvider')(null, options, app);

	const DEFAULT_KEY_SPACE = { 'Default_Keyspace': [] };

	const getSchemaNames = udt => {
		const schemaNames = udt.compMod?.bucketsWithCurrentDefinition;
		return !_.isEmpty(schemaNames) ? schemaNames : DEFAULT_KEY_SPACE;
	};

	const getCreateUdtScript = jsonSchema => {
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

			return ddlProvider.createUdt({ ...udt, schemaName });
		});

		return ddlProvider.createUdt(udt);
	};

	const getDeleteUdtScript = udt => {
		const schemaNames = getSchemaNames(udt);
		return Object.keys(schemaNames).map(schemaName => {
			const name = udt.code || udt.name || '';
			return ddlProvider.dropUdt({ name, schemaName });
		});
	};

	return {
		getCreateUdtScript,
		getDeleteUdtScript,
	};
};
