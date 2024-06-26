const fs = require('fs');

const getSshConfig = info => {
	const config = {
		username: info.ssh_user,
		host: info.ssh_host,
		port: info.ssh_port,
		dstHost: info.host,
		dstPort: info.port,
		localHost: '127.0.0.1',
		localPort: info.port,
		keepAlive: true,
	};

	if (info.ssh_method === 'privateKey') {
		return Object.assign({}, config, {
			privateKey: fs.readFileSync(info.ssh_key_file),
			passphrase: info.ssh_key_passphrase,
		});
	} else {
		return Object.assign({}, config, {
			password: info.ssh_password,
		});
	}
};

const connectViaSsh = info =>
	new Promise((resolve, reject) => {
		ssh(getSshConfig(info), (err, tunnel) => {
			if (err) {
				reject(err);
			} else {
				resolve({
					tunnel,
					info: Object.assign({}, info, {
						host: '127.0.0.1',
					}),
				});
			}
		});
	});

module.exports = {
	connectViaSsh,
};
