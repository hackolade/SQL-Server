const { getTableSystemTime } = require('../../databaseService/databaseService');

const getPeriodForSystemTime = async (dbConnectionClient, dbName, tableName, schemaName, logger) => {
	const tableSystemTime = await getTableSystemTime(dbConnectionClient, dbName, tableName, schemaName, logger);
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
