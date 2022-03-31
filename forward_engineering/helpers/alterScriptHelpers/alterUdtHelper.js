module.exports = (app, options) => {
	const _ = app.require('lodash');
	const { createColumnDefinitionBySchema } = require('./createColumnDefinition')(_);
	const ddlProvider = require('../../ddlProvider')(null, options, app);

	const getCreateUdtScript = jsonSchema => {
		const schemaData = { schemaName: '' };

		const udt = createColumnDefinitionBySchema({
			name: jsonSchema.code || jsonSchema.name,
			jsonSchema: jsonSchema,
			parentJsonSchema: { required: [] },
			ddlProvider,
			schemaData,
		});

		return ddlProvider.createUdt(udt);
	};

	const getDeleteUdtScript = udt => {
		return ddlProvider.dropUdt(udt.code || udt.name);
	};

	return {
		getCreateUdtScript,
		getDeleteUdtScript,
	};
};
