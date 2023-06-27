module.exports = (app, options) => {
	const _ = app.require('lodash');
	const ddlProvider = require('../../ddlProvider')(null, options, app);
	const { getDbData } = app.require('@hackolade/ddl-fe-utils').general;
	const { AlterScriptDto } = require('./types/AlterScriptDto');

	const getAddContainerScript = containerData => {
		const constructedDbData = getDbData([containerData]);
		const schemaData = ddlProvider.hydrateSchema(constructedDbData, {
			udfs: containerData.role?.UDFs,
			procedures: containerData.role?.Procedures,
			useDb: false,
		});

		return AlterScriptDto.getInstance([_.trim(ddlProvider.createSchema(schemaData))], true, false);
	};

	const getDeleteContainerScript = containerName => {
		return AlterScriptDto.getInstance([ddlProvider.dropSchema(containerName)], true, true);
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
