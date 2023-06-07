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

	const getUpdateSchemaCommentScript = ({schemaName, comment}) => ddlProvider.updateSchemaComment({schemaName, comment})
	const getDropSchemaCommentScript = ({schemaName}) => ddlProvider.dropSchemaComment({schemaName})

	const getSchemasDropCommentsAlterScripts = (schemas) => Object.keys(schemas).map(schemaName => getDropSchemaCommentScript({schemaName}))

	const getSchemasModifyCommentsAlterScripts = (schemas) => {
		return Object.keys(schemas).map(schemaName => {
			const schemaComparison = schemas[schemaName].role?.compMod

			const newComment = schemaComparison?.description?.new
			const oldComment = schemaComparison?.description?.old

			const isCommentRemoved = oldComment && !newComment
			if (isCommentRemoved) {
				return getDropSchemaCommentScript({schemaName})
			}

			return newComment ? getUpdateSchemaCommentScript({schemaName, comment: newComment}) : ''
			
		})
	}

	return {
		getAddContainerScript,
		getDeleteContainerScript,
		getSchemasDropCommentsAlterScripts,
		getSchemasModifyCommentsAlterScripts,
	};
};
