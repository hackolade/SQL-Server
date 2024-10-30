const { URL } = require('url');
const { ConnectionPool } = require('mssql');

const mssqlPrefix = 'mssql://';

// example: mssql://username:password@host:1433/DatabaseName
const parseMssqlUrl = ({ url = '' }) => {
	const parsed = new URL(url);
	return {
		database: parsed.pathname.slice(1),
		host: parsed.hostname,
		port: parsed.port ? Number(parsed.port) : null,
		userName: parsed.username,
		userPassword: parsed.password,
	};
};

// Default connection string example:
// Server=host,1433;Database=DatabaseName;User Id=username;Password=password;
const parseConnectionString = ({ string = '' }) => {
	const params = string.startsWith(mssqlPrefix)
		? parseMssqlUrl({ url: string })
		: ConnectionPool.parseConnectionString(string);

	return {
		databaseName: params.database,
		host: params.server,
		port: params.port,
		userName: params.user,
		userPassword: params.password,
	};
};

module.exports = {
	parseConnectionString,
};
