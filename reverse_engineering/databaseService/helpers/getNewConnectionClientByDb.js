const sql = require('mssql');

const getNewConnectionClientByDb = async (connectionClient, currentDbName) => {
	if (!connectionClient) {
		throw new Error('Connection client is missing');
	}

	const { database, user, password, port, server } = connectionClient.config;
	if (database === currentDbName) {
		return connectionClient;
	}

	return await sql.connect({
		user,
		password,
		server,
		port,
		connectionTimeout: 90000,
		requestTimeout: 90000,
		options: {
			encrypt: true,
		},
		database: currentDbName,
	});
};

module.exports = getNewConnectionClientByDb;
