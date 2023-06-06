const { set: setObjectProperty } = require('./objectHelper')
const {getDescriptionComments } = require('../databaseService/databaseService')

const objtypesToDescriptionCommentsInjectors = {
    SCHEMA: ({schemas, objname, value}) => schemas.map(collection => collection.dbName === objname ? setObjectProperty(collection, 'bucketInfo.description', value): collection ),
    TABLE: ({schemas, objname, value}) => schemas.map(collection => collection.collectionName === objname ? setObjectProperty(collection, 'entityLevel.description', value): collection ),
    VIEW: ({schemas, objname, value}) => schemas.map(view => view.collectionName === objname ? setObjectProperty(view, 'data.description', value) : view ),
    COLUMN: ({schemas, objname, value}) => schemas.map(collection => 
        !collection?.data 
        && collection?.validation?.jsonSchema?.properties
        && Object.keys(collection?.validation?.jsonSchema?.properties).includes(objname) ? setObjectProperty(collection, `collection.validation.jsonSchema.properties.${objname}.description`, value): collection )
}

const getJsonSchemasWithInjectedDescriptionComments = async (jsonSchemas) => {
    // Getting unique schemas names
    const schemas = [...new Set(jsonSchemas.map(({dbName}) => dbName))].map(dbName => ({schema: dbName}))
    let schemasWithTables = schemas.map(({schema}) => ({schema, tablesCount: 0}));
    let schemasWithViews = schemas.map(({schema}) => ({schema, viewsCount: 0}));

    const entities = jsonSchemas.map(({collectionName: name, dbName, data}) => {
        const type = data && Object.keys(data).includes('viewAttrbute') ? 'view' : 'table'
        if (type === 'view') {
            schemasWithViews = schemasWithViews.map((schema) => schema.schema === dbName ? ({schema: schema.schema, viewsCount: schema.viewsCount + 1}) : schema)
        }
        if (type === 'table') {
            schemasWithTables = schemasWithTables.map((schema) => schema.schema === dbName ? ({schema: schema.schema, tablesCount: schema.tablesCount + 1}) : schema)
        }
        return {
            schema: dbName, 
            entity: {type, name}
        }
    })

    schemasWithViews = schemasWithViews.filter(schema => schema.viewsCount > 0).map(({schema}) => ({schema, entity: {type: 'view'}}))
    schemasWithTables = schemasWithTables.filter(schema => schema.tablesCount > 0).map(({schema}) => ({schema, entity: {type: 'table'}}))

    const descriptionComments = (await Promise
        .all([...schemas, ...schemasWithViews, ...schemasWithTables, ...entities]
        .map(commentParams => getDescriptionComments(client, dbName, commentParams, logger)))).flat();

    let jsonSchemasWithDescriptionComments = [...jsonSchemas]

    descriptionComments.forEach(({objtype, objname, value}) => {
        const getSchemasWithInjectedDescriptionComments = objtypesToDescriptionCommentsInjectors[objtype]
        if (!getSchemasWithInjectedDescriptionComments) {
            return
        }
        jsonSchemasWithDescriptionComments = getSchemasWithInjectedDescriptionComments({schemas: jsonSchemasWithDescriptionComments, objname, value})
    })

    return jsonSchemasWithDescriptionComments
}

module.exports = {
    getJsonSchemasWithInjectedDescriptionComments,
}