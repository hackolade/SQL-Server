const mapJsonSchema = require('./mapJsonSchema');

const handleDate = field => {
	return Object.assign({}, field, {
		type: 'datetime',
        mode: 'datetime',
	});
};

const handleTime = field => {
	return Object.assign({}, field, {
		type: 'datetime',
        mode: 'datetime',
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

	switch(format) {
		case 'date':
			return handleDate(fieldData);
		case 'time':
			return handleTime(fieldData);
		case 'date-time':
			return handleDateTime(fieldData);
		default:
			return field;
	};
};


const adaptType = field => {
	const type = field.type;

	if (type === 'string') {
		return handleStringFormat(field);
	}

	return field;
};


const adaptJsonSchema = (_, jsonSchema) => {
	return mapJsonSchema(_)(jsonSchema, adaptType);
};

module.exports = { adaptJsonSchema };
