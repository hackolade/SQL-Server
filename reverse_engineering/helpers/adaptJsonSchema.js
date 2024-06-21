const mapJsonSchema = require('./mapJsonSchema');

const handleDate = field => {
	return Object.assign({}, field, {
		type: 'datetime',
		mode: 'date',
	});
};

const handleTime = field => {
	return Object.assign({}, field, {
		type: 'datetime',
		mode: 'time',
	});
};

const handleDateTime = field => {
	return Object.assign({}, field, {
		type: 'datetime',
		mode: 'datetime',
	});
};

const handleStringFormat = field => {
	const { format, ...fieldData } = field;

	switch (format) {
		case 'date':
			return handleDate(fieldData);
		case 'time':
			return handleTime(fieldData);
		case 'date-time':
			return handleDateTime(fieldData);
		default:
			return field;
	}
};

const adaptType = field => {
	const type = field.type;

	if (type === 'string') {
		return handleStringFormat(field);
	}

	return field;
};

const adaptSchema = (_, jsonSchema) => {
	return mapJsonSchema(_)(jsonSchema, adaptType);
};

const adaptJsonSchema = (data, logger, callback, app) => {
	const formatError = error => {
		return Object.assign(
			{ title: 'Adapt JSON Schema' },
			Object.getOwnPropertyNames(error).reduce((accumulator, key) => {
				return Object.assign(accumulator, {
					[key]: error[key],
				});
			}, {}),
		);
	};
	logger.log('info', 'Adaptation of JSON Schema started...', 'Adapt JSON Schema');
	try {
		const jsonSchema = JSON.parse(data.jsonSchema);
		const adaptedJsonSchema = adaptSchema(app.require('lodash'), jsonSchema);

		logger.log('info', 'Adaptation of JSON Schema finished.', 'Adapt JSON Schema');

		callback(null, {
			jsonSchema: JSON.stringify(adaptedJsonSchema),
		});
	} catch (error) {
		const formattedError = formatError(error);
		callback(formattedError);
	}
};

module.exports = { adaptJsonSchema };
