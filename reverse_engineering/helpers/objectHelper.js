/**
 * 
 * @param {object} data 
 * @param {string} keyChain 
 * @param {any} value 
 * @returns {object}
 */
const setProperty = (data, key, value) => {
	return { ...data, [key]: value };
};

const set = (data, key, value) => {
	const keys = key.split('.');

	if (keys.length === 1) {
		return setProperty(data, keys[0], value);
	}

	return setProperty(data, keys[0], set(data[keys[0]] || {}, keys.slice(1).join('.'), value));
};

module.exports = {
    set
}