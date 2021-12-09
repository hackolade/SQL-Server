const defineFieldsDescription = columnsInfo => jsonSchema =>
	columnsInfo.reduce((jsonSchemaAcc, column) => ({
		...jsonSchemaAcc,
		...(jsonSchemaAcc.properties[column.Column] && {
			properties: {
				...jsonSchemaAcc.properties,
				[column.Column]: {
					...jsonSchemaAcc.properties[column.Column],
					description: column.Description || '',
					generatedAlwaysType: column.GeneratedAlwaysType || '',
					isHidden: column.IsHidden || false
				},
			}
		}),
	}), jsonSchema);

module.exports = defineFieldsDescription;
