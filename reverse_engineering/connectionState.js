const { getConnectionClient } = require('./databaseService/databaseService');
const sshHelper = require('./helpers/sshHelper');

const stateInstance = {
	_client: null,
	_sshTunnel: null,
	getClient: () => this._client,
	setClient: async (connectionInfo, attempts = 0, logger) => {
		if (connectionInfo.ssh && !this._sshTunnel) {
			const sshData = await sshHelper.connectViaSsh(connectionInfo);
			connectionInfo = sshData.info;
			this._sshTunnel = sshData.tunnel;
		}

		try {
			this._client = await getConnectionClient(connectionInfo, logger);
		} catch (error) {
			const encryptConnection =
				connectionInfo.encryptConnection === undefined || Boolean(connectionInfo.encryptConnection);
			const isEncryptedConnectionToLocalInstance =
				error.message.includes('self signed certificate') && encryptConnection;

			if (isEncryptedConnectionToLocalInstance && attempts <= 0) {
				return stateInstance.setClient(
					{
						...connectionInfo,
						encryptConnection: false,
					},
					attempts + 1,
					logger,
				);
			}

			throw error;
		}
	},
	clearClient: () => {
		this._client = null;

		if (this._sshTunnel) {
			this._sshTunnel.close();
			this._sshTunnel = null;
		}
	},
};

module.exports = stateInstance;
