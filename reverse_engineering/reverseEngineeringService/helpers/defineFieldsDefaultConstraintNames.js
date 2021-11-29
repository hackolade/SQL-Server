const defineFieldsDefaultConstraintNames = defaultConstraintsInfo => jsonSchema =>
defaultConstraintsInfo.reduce((jsonSchemaAcc, column) => ({
		...jsonSchemaAcc,
		...(jsonSchemaAcc.properties[column.columnName] && {
			properties: {
				...jsonSchemaAcc.properties,
				[column.columnName]: {
					...jsonSchemaAcc.properties[column.columnName],
					defaultConstraintName: getValidName(column.name),
				},
			}
		}),
	}), jsonSchema);

const getValidName = (name = '') => {
	const isDbGenerated  = name.startsWith('DF__');
	return !isDbGenerated ? name : '';
}

module.exports = defineFieldsDefaultConstraintNames;
