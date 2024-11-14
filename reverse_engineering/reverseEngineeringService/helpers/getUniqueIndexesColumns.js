const getColumnUniqueKey = ({ IndexName, TableName, schemaName, columnName }) =>
	`${schemaName}${IndexName}${TableName}${columnName}`;

const getUniqueIndexesColumns = ({ indexesColumns }) => {
	const uniqueKeysToColumns = {};

	for (const indexesColumn of indexesColumns) {
		const columnKey = getColumnUniqueKey(indexesColumn);
		const isColumnUnique = !Boolean(uniqueKeysToColumns[columnKey]);

		if (!isColumnUnique) {
			continue;
		}

		uniqueKeysToColumns[columnKey] = indexesColumn;
	}

	return Object.values(uniqueKeysToColumns);
};

module.exports = {
	getUniqueIndexesColumns,
};
