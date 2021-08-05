const getObjectsFromDatabase = async client => {
	const { recordset } = await client.query`SELECT * from INFORMATION_SCHEMA.TABLES`;
	const schemaObjects = recordset.reduce((schemas, { TABLE_NAME, TABLE_TYPE, TABLE_SCHEMA }) => {
		const schema = schemas[TABLE_SCHEMA] || { dbName: TABLE_SCHEMA, dbCollections: [], views: [] };
		if (TABLE_TYPE === 'VIEW') {
			return {
				...schemas,
				[TABLE_SCHEMA]: {
					...schema,
					views: [...schema.views, TABLE_NAME],
				}
			};
		}

		return {
			...schemas,
			[TABLE_SCHEMA]: {
				...schema,
				dbCollections: [...schema.dbCollections, TABLE_NAME],
			}
		};
	}, {});
	return Object.values(schemaObjects);
};

module.exports = getObjectsFromDatabase;
