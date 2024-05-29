const prepareDeleteAndUpdate = value => {
	switch (value) {
		case 'NO_ACTION':
			return 'NO ACTION';
		case 'CASCADE':
			return 'CASCADE';
		case 'SET_NULL':
			return 'SET NULL';
		case 'SET_DEFAULT':
			return 'SET DEFAULT';
		default:
			return '';
	}
};

const reverseTableForeignKeys = tableForeignKeys => {
	const tableForeignKeysObject = tableForeignKeys.reduce((data, foreignKey) => {
		const foreignKeyName = foreignKey.FK_NAME;
		const existedForeignKey = data[foreignKeyName];
		const relationshipOnDelete = prepareDeleteAndUpdate(foreignKey.on_delete);
		const relationshipOnUpdate = prepareDeleteAndUpdate(foreignKey.on_update);
		const getForeignKey = existedForeignKey => {
			if (existedForeignKey) {
				return {
					...existedForeignKey,
					parentField: [...existedForeignKey.parentField, foreignKey.referenced_column],
					childField: [...existedForeignKey.childField, foreignKey.column],
				};
			} else {
				return {
					relationshipName: foreignKeyName,
					dbName: foreignKey.schema_name,
					parentCollection: foreignKey.referenced_table,
					parentField: [foreignKey.referenced_column],
					childDbName: foreignKey.schema_name,
					childCollection: foreignKey.table,
					childField: [foreignKey.column],
					relationshipType: 'Foreign Key',
					relationshipInfo: {
						relationshipOnDelete,
						relationshipOnUpdate,
					},
				};
			}
		};

		return {
			...data,
			[foreignKeyName]: getForeignKey(existedForeignKey),
		};
	}, {});

	return Object.keys(tableForeignKeysObject).map(key => tableForeignKeysObject[key]);
};

module.exports = reverseTableForeignKeys;
