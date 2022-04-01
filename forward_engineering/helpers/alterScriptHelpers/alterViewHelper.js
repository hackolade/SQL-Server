module.exports = (app, options) => {
	const _ = app.require('lodash');
	const { mapProperties } = app.require('@hackolade/ddl-fe-utils');
	const { checkCompModEqual, setIndexKeys, modifyGroupItems } = require('./common')(app);
	const { generateIdToNameHashTable, generateIdToActivatedHashTable } = app.require('@hackolade/ddl-fe-utils');
	const ddlProvider = require('../../ddlProvider')(null, options, app);
	const { getTableName } = require('../general')(app);

	const getAddViewScript = view => {
		const viewSchema = { ...view, ...(view.role ?? {}) };

		const viewData = {
			name: viewSchema.code || viewSchema.name,
			keys: getKeys(viewSchema, viewSchema.compMod?.collectionData?.collectionRefsDefinitionsMap ?? {}),
			schemaData: { schemaName: viewSchema.compMod.keyspaceName },
		};
		const hydratedView = ddlProvider.hydrateView({ viewData, entityData: [view] });

		return ddlProvider.createView(hydratedView, {}, view.isActivated);
	};

	const getDeleteViewScript = view => {
		const viewName = getTableName(view.code || view.name, view?.role?.compMod?.keyspaceName);

		return ddlProvider.dropView(viewName);
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

		let alterView = '';

		if (
			isViewAttributeModified ||
			isSelectStatementModified ||
			isCheckOptionModified ||
			isFieldsModifiedWithNoSelectStatement
		) {
			const hydratedView = ddlProvider.hydrateView({ viewData, entityData: [viewSchema] });
			alterView = ddlProvider.alterView(hydratedView, null, viewSchema.isActivated);
		}

		const alterIndexesScripts = modifyGroupItems({
			data: viewSchema,
			key: 'Indxs',
			hydrate: hydrateIndex({ idToNameHashTable, idToActivatedHashTable, schemaData }),
			create: (viewName, index) =>
				index.orReplace
					? `${ddlProvider.dropIndex(viewName, index)}\n\n${ddlProvider.createViewIndex(viewName, index)}`
					: ddlProvider.createViewIndex(viewName, index),
			drop: (viewName, index) => ddlProvider.dropIndex(viewName, index),
		});

		return [alterView, alterIndexesScripts].filter(Boolean).join('\n\n');
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

	return {
		getAddViewScript,
		getDeleteViewScript,
		getModifiedViewScript,
	};
};
