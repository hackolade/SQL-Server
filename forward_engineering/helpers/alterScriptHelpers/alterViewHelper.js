module.exports = (app, options) => {
	const _ = app.require('lodash');
	const { mapProperties } = app.require('@hackolade/ddl-fe-utils');
	const { checkCompModEqual, setIndexKeys, modifyGroupItems } = require('./common')(app);
	const { generateIdToNameHashTable, generateIdToActivatedHashTable } = app.require('@hackolade/ddl-fe-utils');
	const ddlProvider = require('../../ddlProvider')(null, options, app);
	const { getTableName } = require('../general')(app);
	const { AlterScriptDto } = require('./types/AlterScriptDto');

	const getAddViewScript = view => {
		const viewSchema = { ...view, ...(view.role ?? {}) };

		const viewData = {
			name: viewSchema.code || viewSchema.name,
			keys: getKeys(viewSchema, viewSchema.compMod?.collectionData?.collectionRefsDefinitionsMap ?? {}),
			schemaData: { schemaName: viewSchema.compMod.keyspaceName },
		};
		const hydratedView = ddlProvider.hydrateView({ viewData, entityData: [view] });

		return AlterScriptDto.getInstance([ddlProvider.createView(hydratedView, {}, view.isActivated)], true, false);
	};

	const getDeleteViewScript = view => {
		const viewName = getTableName(view.code || view.name, view?.role?.compMod?.keyspaceName);

		return AlterScriptDto.getInstance([ddlProvider.dropView(viewName)], true, true);
	};

	const getModifiedViewScript = view => {
		const viewSchema = { ...view, ...(view.role ?? {}) };
		const idToNameHashTable = generateIdToNameHashTable(viewSchema);
		const idToActivatedHashTable = generateIdToActivatedHashTable(viewSchema);
		const schemaData = { schemaName: viewSchema.compMod.keyspaceName };
		const viewData = {
			name: viewSchema.code || viewSchema.name,
			keys: getKeys(viewSchema, viewSchema.compMod?.collectionData?.collectionRefsDefinitionsMap ?? {}),
			schemaData,
		};

		const isViewAttributeModified = !checkCompModEqual(viewSchema.compMod?.viewAttrbute);
		const isSelectStatementModified = !checkCompModEqual(viewSchema.compMod?.selectStatement);
		const isCheckOptionModified = !checkCompModEqual(viewSchema.compMod?.withCheckOption);
		const isFieldsModifiedWithNoSelectStatement =
			!_.trim(viewSchema.selectStatement) && !_.isEmpty(view.properties);

		let alterView = AlterScriptDto.getInstance([], true, false);

		if (
			isViewAttributeModified ||
			isSelectStatementModified ||
			isCheckOptionModified ||
			isFieldsModifiedWithNoSelectStatement
		) {
			const hydratedView = ddlProvider.hydrateView({ viewData, entityData: [viewSchema] });
			alterView = AlterScriptDto.getInstance([ddlProvider.alterView(hydratedView, null, viewSchema.isActivated)], true, false);
		}

		const alterIndexesScripts = modifyGroupItems({
			data: viewSchema,
			key: 'Indxs',
			hydrate: hydrateIndex({ idToNameHashTable, idToActivatedHashTable, schemaData }),
			create: (viewName, index) =>
				index.orReplace
					? AlterScriptDto.getInstances([ddlProvider.dropIndex(viewName, index), ddlProvider.createViewIndex(viewName, index)], true, true)
					: AlterScriptDto.getInstance([ddlProvider.createViewIndex(viewName, index)], true, false),
			drop: (viewName, index) => AlterScriptDto.getInstance([ddlProvider.dropIndex(viewName, index)], true, true),
		});

		return [alterView, ...alterIndexesScripts];
	};

	const getKeys = (viewSchema, collectionRefsDefinitionsMap) => {
		return mapProperties(viewSchema, (propertyName, schema) => {
			const definition = collectionRefsDefinitionsMap[schema.refId];

			if (!definition) {
				return ddlProvider.hydrateViewColumn({
					name: propertyName,
					isActivated: schema.isActivated,
				});
			}

			const entityName =
				_.get(definition.collection, '[0].code', '') ||
				_.get(definition.collection, '[0].collectionName', '') ||
				'';
			const dbName = _.get(definition.bucket, '[0].code') || _.get(definition.bucket, '[0].name', '');
			const name = definition.name;

			if (name === propertyName) {
				return ddlProvider.hydrateViewColumn({
					containerData: definition.bucket,
					entityData: definition.collection,
					isActivated: schema.isActivated,
					definition: definition.definition,
					entityName,
					name,
					dbName,
				});
			}

			return ddlProvider.hydrateViewColumn({
				containerData: definition.bucket,
				entityData: definition.collection,
				isActivated: schema.isActivated,
				definition: definition.definition,
				alias: propertyName,
				entityName,
				name,
				dbName,
			});
		});
	};

	const hydrateIndex =
		({ idToNameHashTable, idToActivatedHashTable, schemaData }) =>
			index => {
				index = setIndexKeys(idToNameHashTable, idToActivatedHashTable, index);

				return ddlProvider.hydrateViewIndex(index, schemaData);
			};

		const getViewUpdateCommentScript = ({schemaName, viewName, comment}) => ddlProvider.updateViewComment({schemaName, viewName, comment})
		const getViewDropCommentScript = ({schemaName, viewName}) => ddlProvider.dropViewComment({schemaName, viewName})

		const getViewsDropCommentAlterScripts = (views) => {	
			return Object.keys(views).map(viewName => {
				const schemaName = views[viewName].role?.compMod?.bucketProperties?.name
				return getViewDropCommentScript({schemaName, viewName})
			})
		}
	
		const getViewsModifyCommentsAlterScripts = (views) => {
			return Object.keys(views).map(viewName => {
				const viewComparison = views[viewName].role?.compMod
				const schemaName = viewComparison.keyspaceName
	
				const newComment = viewComparison?.description?.new
				const oldComment = viewComparison?.description?.old
	
				const isCommentRemoved = oldComment && !newComment
				if (isCommentRemoved) {
					return getViewDropCommentScript({schemaName, viewName})
				}
	
				return newComment ? getViewUpdateCommentScript({schemaName, viewName, comment: newComment}) : ''
				
			})
		}

	return {
		getAddViewScript,
		getDeleteViewScript,
		getModifiedViewScript,
		getViewsDropCommentAlterScripts,
		getViewsModifyCommentsAlterScripts
	};
};
