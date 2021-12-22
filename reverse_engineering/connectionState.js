const { getConnectionClient } = require('./databaseService/databaseService');

const stateInstance = {
	_client: null,
	getClient: () => this._client,
	setClient: async (connectionInfo, attempts = 0) => {
		try {
			this._client = await getConnectionClient(connectionInfo)
		} catch (error) {
			const isEncryptedConnectionToLocalInstance = error.message.includes('self signed certificate') && connectionInfo.encryptConnection;

			if (isEncryptedConnectionToLocalInstance && attempts <= 0) {
				return stateInstance.setClient({
					...connectionInfo,
					encryptConnection: false,
				});
			}
			
			throw error;
		}
	},
	clearClient: () => this._client = null,
}

module.exports = stateInstance;