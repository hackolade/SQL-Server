const defineFieldsDescription = columnsInfo => jsonSchema =>
	columnsInfo.reduce(
		(jsonSchemaAcc, column) => ({
			...jsonSchemaAcc,
			...(jsonSchemaAcc.properties[column.Column] && {
				properties: {
					...jsonSchemaAcc.properties,
					[column.Column]: {
						...jsonSchemaAcc.properties[column.Column],
						description: column.Description || '',
						computedColumn: column.Computed,
						computedColumnExpression: column.Computed_Expression,
						persisted: column.Persisted,
					},
				},
			}),
		}),
		jsonSchema,
	);

module.exports = defineFieldsDescription;
