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
						computed: column.Computed,
						computedExpression: column.Computed_Expression,
						persisted: column.Persisted,
					},
				},
			}),
		}),
		jsonSchema,
	);

module.exports = defineFieldsDescription;
