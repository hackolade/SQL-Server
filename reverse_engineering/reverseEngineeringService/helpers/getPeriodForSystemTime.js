const { getTableSystemTime } = require('../../databaseService/databaseService');

const getPeriodForSystemTime = async ({ client, dbName, tableName, schemaName, logger }) => {
	const tableSystemTime = await getTableSystemTime({ client, dbName, tableName, tableSchema: schemaName, logger });

	if (!tableSystemTime[0]) {
		return;
	}

	const periodForSystemTime = tableSystemTime[0];
	return [
		{
			startTime: [
				{
					name: periodForSystemTime.startColumn,
					type: periodForSystemTime.startColumnIsHidden === 1 ? 'hidden' : '',
				},
			],
			endTime: [
				{
					name: periodForSystemTime.endColumn,
					type: periodForSystemTime.endColumnIsHidden === 1 ? 'hidden' : '',
				},
			],
		},
	];
};

module.exports = getPeriodForSystemTime;
