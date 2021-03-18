
const add = (obj, properties) => Object.assign({}, obj, properties);

const mapJsonSchema = (_) => (jsonSchema, callback) => {
	const mapProperties = (properties, mapper) => Object.keys(properties).reduce((newProperties, propertyName) => {
		return add(newProperties, {
			[propertyName]: mapper(properties[propertyName])
		});
	}, {});
	const mapItems = (items, mapper) => {
		if (Array.isArray(items)) {
			return items.map(jsonSchema => mapper(jsonSchema));
		} else if (_.isPlainObject(items)) {
			return mapper(items);
		} else {
			return items;
		}
	};
	const applyTo = (properties, jsonSchema, mapper) => {
		return properties.reduce((jsonSchema, propertyName) => {
			if (!jsonSchema[propertyName]) {
				return jsonSchema;
			}
	
			return Object.assign({}, jsonSchema, {
				[propertyName]: mapper(jsonSchema[propertyName])
			});
		}, jsonSchema);
	};
	if (!_.isPlainObject(jsonSchema)) {
		return jsonSchema;
	}
	const mapper = _.partial(mapJsonSchema(_), _.partial.placeholder, callback);
	const propertiesLike = [ 'properties', 'definitions', 'patternProperties' ];
	const itemsLike = [ 'items', 'oneOf', 'allOf', 'anyOf', 'not' ];
	
	const copyJsonSchema = Object.assign({}, jsonSchema);
	const jsonSchemaWithNewProperties = applyTo(propertiesLike, copyJsonSchema, _.partial(mapProperties, _.partial.placeholder, mapper));
	const newJsonSchema = applyTo(itemsLike, jsonSchemaWithNewProperties, _.partial(mapItems, _.partial.placeholder, mapper));

	return callback(newJsonSchema);
};

module.exports = mapJsonSchema;
