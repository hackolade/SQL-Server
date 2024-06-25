const { getConnectionClient } = require('./databaseService/databaseService');

const stateInstance = {
	_client: null,
	_isSshTunnel: false,
	getClient: () => this._client,
	setClient: async (connectionInfo, sshService, attempts = 0, logger) => {
		if (connectionInfo.ssh && !this._isSshTunnel) {
			const { options } = await sshService.openTunnel({
				sshAuthMethod: connectionInfo.ssh_method === 'privateKey' ? 'IDENTITY_FILE' : 'USER_PASSWORD',
				sshTunnelHostname: connectionInfo.ssh_host,
				sshTunnelPort: connectionInfo.ssh_port,
				sshTunnelUsername: connectionInfo.ssh_user,
				sshTunnelPassword: connectionInfo.ssh_password,
				sshTunnelIdentityFile: connectionInfo.ssh_key_file,
				sshTunnelPassphrase: connectionInfo.ssh_key_passphrase,
				host: connectionInfo.host,
				port: connectionInfo.port,
			});

			this._isSshTunnel = true;
			connectionInfo = {
				...connectionInfo,
				...options,
			};
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
					sshService,
					attempts + 1,
					logger,
				);
			}

			throw error;
		}
	},
	clearClient: async sshService => {
		this._client = null;

		if (this._isSshTunnel) {
			await sshService.closeConsumer();
			this._isSshTunnel = false;
		}
	},
};

module.exports = stateInstance;
