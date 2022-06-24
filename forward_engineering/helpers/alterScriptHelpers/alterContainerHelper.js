module.exports = (app, options) => {
	const _ = app.require('lodash');
	const ddlProvider = require('../../ddlProvider')(null, options, app);
	const { getDbData } = app.require('@hackolade/ddl-fe-utils').general;

	const getAddContainerScript = containerData => {
		const constructedDbData = getDbData([containerData]);
		const schemaData = ddlProvider.hydrateSchema(constructedDbData, {
			udfs: containerData.role?.UDFs,
			procedures: containerData.role?.Procedures,
			useDb: false,
		});

		return _.trim(ddlProvider.createSchema(schemaData));
	};

	const getDeleteContainerScript = containerName => {
		return ddlProvider.dropSchema(containerName);
	};

	return {
		getAddContainerScript,
		getDeleteContainerScript,
	};
};
