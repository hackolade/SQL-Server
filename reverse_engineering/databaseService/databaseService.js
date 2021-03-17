const sql = require('mssql');
const { getObjectsFromDatabase, getNewConnectionClientByDb } = require('./helpers');

const getConnectionClient = async connectionInfo => {
	if (connectionInfo.authMethod === 'Username / Password') {
		return await sql.connect({
			user: connectionInfo.userName,
			password: connectionInfo.userPassword,
			server: connectionInfo.host,
			port: +connectionInfo.port,
			database: connectionInfo.databaseName,
			options: {
				encrypt: true,
			},
		});
	} else if (connectionInfo.authMethod === 'Username / Password (Windows)') {
		return await sql.connect({
			user: connectionInfo.userName,
			password: connectionInfo.userPassword,
			server: connectionInfo.host,
			port: +connectionInfo.port,
			database: connectionInfo.databaseName,
			domain: connectionInfo.userDomain,
			options: {
				encrypt: false
			},
		});
	} else if (connectionInfo.authMethod === 'Azure Active Directory (Username / Password)') {
		return await sql.connect({
			user: connectionInfo.userName,
			password: connectionInfo.userPassword,
			server: connectionInfo.host,
			port: +connectionInfo.port,
			database: connectionInfo.databaseName,
			options: {
				encrypt: true
			},
			authentication: {
				type: 'azure-active-directory-password',
			}
		});
	}

	return await sql.connect(connectionInfo.connectionString);
};

const PERMISSION_DENIED_CODE = 297;

const addPermissionDeniedMetaData = (error, meta) => {
	error.message = 'The user does not have permission to perform ' + meta.action +
		'. Please, check the access to the next objects: ' + meta.objects.join(', ');

	return error;
};

const getClient = async (connectionClient, dbName, meta, logger) => {
	let currentDbConnectionClient = await getNewConnectionClientByDb(connectionClient, dbName);

	const _inst = {
		request(...args) {
			currentDbConnectionClient = currentDbConnectionClient.request(...args);
			return _inst;
		},
		input(...args) {
			currentDbConnectionClient = currentDbConnectionClient.input(...args);
			return _inst;
		},
		async query(...queryParams) {
			try {
				return await currentDbConnectionClient.query(...queryParams);
			} catch (error) {
				if (meta) {
					if (error.number === PERMISSION_DENIED_CODE) {
						error = addPermissionDeniedMetaData(error, meta);
					}

					if (meta.skip) {
						logger.log('error', { message: error.message, stack: error.stack, error }, 'Perform ' + meta.action);
						logger.progress({ message: 'Failed: ' + meta.action, containerName: dbName, entityName: ''});

						return [];
					}
				}

				throw error;
			}
		}
	};

	return _inst;
};

const getTableInfo = async (connectionClient, dbName, tableName, tableSchema, logger) => {
	const currentDbConnectionClient = await getClient(connectionClient, dbName, {
		action: 'table information query',
		objects: [
			'information_schema.columns',
			'sys.identity_columns',
			'sys.objects',
		]
	}, logger);
	const objectId = `${tableSchema}.${tableName}`;
	return mapResponse(await currentDbConnectionClient.query`
		SELECT c.*,
				ic.SEED_VALUE,
				ic.INCREMENT_VALUE,
				COLUMNPROPERTY(object_id(${objectId}), c.column_name, 'IsSparse') AS IS_SPARSE,
				COLUMNPROPERTY(object_id(${objectId}), c.column_name, 'IsIdentity') AS IS_IDENTITY,
				o.type AS TABLE_TYPE
		FROM information_schema.columns as c
		LEFT JOIN SYS.IDENTITY_COLUMNS ic ON ic.object_id=object_id(${objectId})
		LEFT JOIN sys.objects o ON o.object_id=object_id(${objectId})
		WHERE c.table_name = ${tableName}
		AND c.table_schema = ${tableSchema}
	;`);
};

const getTableRow = async (connectionClient, dbName, tableName, tableSchema, reverseEngineeringOptions, logger) => {
	const currentDbConnectionClient = await getClient(connectionClient, dbName, {
		action: 'getting data query',
		objects: [
			`[${tableSchema}].[${tableName}]`
		],
		skip: true
	}, logger);
	const percentageWord = reverseEngineeringOptions.isAbsoluteValue ? '' : 'PERCENT';

	return mapResponse(await currentDbConnectionClient
			.request()
			.input('tableName', sql.VarChar, tableName)
			.input('tableSchema', sql.VarChar, tableSchema)
			.input('amount', sql.Int, reverseEngineeringOptions.value)
			.input('percent', sql.VarChar, percentageWord)
			.query`EXEC('SELECT TOP '+ @Amount +' '+ @Percent +' * FROM [' + @TableSchema + '].[' + @TableName + '];');`);
};

const getTableForeignKeys = async (connectionClient, dbName, logger) => {
	const currentDbConnectionClient = await getClient(connectionClient, dbName, {
		action: 'getting foreign keys query',
		objects: [
			'sys.foreign_key_columns',
			'sys.objects',
			'sys.tables',
			'sys.schemas',
			'sys.columns',
		],
		skip: true
	}, logger);
	return mapResponse(await currentDbConnectionClient.query`
		SELECT obj.name AS FK_NAME,
				sch.name AS [schema_name],
				tab1.name AS [table],
				col1.name AS [column],
				tab2.name AS [referenced_table],
				col2.name AS [referenced_column]
		FROM sys.foreign_key_columns fkc
		INNER JOIN sys.objects obj
			ON obj.object_id = fkc.constraint_object_id
		INNER JOIN sys.tables tab1
			ON tab1.object_id = fkc.parent_object_id
		INNER JOIN sys.schemas sch
			ON tab1.schema_id = sch.schema_id
		INNER JOIN sys.columns col1
			ON col1.column_id = parent_column_id AND col1.object_id = tab1.object_id
		INNER JOIN sys.tables tab2
			ON tab2.object_id = fkc.referenced_object_id
		INNER JOIN sys.columns col2
			ON col2.column_id = referenced_column_id AND col2.object_id = tab2.object_id
		`);
};

const getDatabaseIndexes = async (connectionClient, dbName, logger) => {
	const currentDbConnectionClient = await getClient(connectionClient, dbName, {
		action: 'getting indexes query',
		objects: [
			'sys.indexes',
			'sys.tables',
			'sys.index_columns',
			'sys.partitions',
		],
		skip: true
	}, logger);
	return mapResponse(await currentDbConnectionClient.query`
		SELECT
			TableName = t.name,
			IndexName = ind.name,
			ic.is_descending_key,
			ic.is_included_column,
			COL_NAME(t.object_id, ic.column_id) as columnName,
			OBJECT_SCHEMA_NAME(t.object_id) as schemaName,
			p.data_compression_desc as dataCompression,
			ind.*
		FROM sys.indexes ind
		LEFT JOIN sys.tables t
			ON ind.object_id = t.object_id
		INNER JOIN sys.index_columns ic
			ON ind.object_id = ic.object_id AND ind.index_id = ic.index_id
		INNER JOIN sys.partitions p
			ON p.object_id = t.object_id AND ind.index_id = p.index_id
		WHERE
			ind.is_primary_key = 0
			AND ind.is_unique_constraint = 0
			AND t.is_ms_shipped = 0
		`);
};

const getIndexesBucketCount = async (connectionClient, dbName, indexesId, logger) => {
	const currentDbConnectionClient = await getClient(connectionClient, dbName, {
		action: 'getting total buckets of indexes',
		objects: [
			'sys.dm_db_xtp_hash_index_stats',
		],
		skip: true
	}, logger);
	return mapResponse(await currentDbConnectionClient
	.request()
	.input('idList', sql.VarChar, indexesId.join(', '))
	.query`
		SELECT hs.total_bucket_count, hs.index_id
		FROM sys.dm_db_xtp_hash_index_stats hs
		WHERE hs.index_id IN (SELECT value FROM string_split(@idList, ','))`);
};

const getSpatialIndexes = async (connectionClient, dbName, logger) => {
	const currentDbConnectionClient = await getClient(connectionClient, dbName, {
		action: 'getting spatial indexes',
		objects: [
			'sys.spatial_indexes',
			'sys.tables',
			'sys.index_columns',
			'sys.partitions',
			'sys.spatial_index_tessellations',
		],
		skip: true
	}, logger);
	return mapResponse(await currentDbConnectionClient.query`
		SELECT
			TableName = t.name,
			IndexName = ind.name,
			COL_NAME(t.object_id, ic.column_id) as columnName,
			OBJECT_SCHEMA_NAME(t.object_id) as schemaName,
			sit.bounding_box_xmin AS XMIN,
			sit.bounding_box_ymin AS YMIN,
			sit.bounding_box_xmax AS XMAX,
			sit.bounding_box_ymax AS YMAX,
			sit.level_1_grid_desc AS LEVEL_1,
			sit.level_2_grid_desc AS LEVEL_2,
			sit.level_3_grid_desc AS LEVEL_3,
			sit.level_4_grid_desc AS LEVEL_4,
			sit.cells_per_object AS CELLS_PER_OBJECT,
			p.data_compression_desc as dataCompression,
			ind.*
		FROM sys.spatial_indexes ind
		LEFT JOIN sys.tables t
			ON ind.object_id = t.object_id
		INNER JOIN sys.index_columns ic
			ON ind.object_id = ic.object_id AND ind.index_id = ic.index_id
		LEFT JOIN sys.spatial_index_tessellations sit
			ON ind.object_id = sit.object_id AND ind.index_id = sit.index_id
		LEFT JOIN sys.partitions p
			ON p.object_id = t.object_id AND ind.index_id = p.index_id`);
};

const getFullTextIndexes = async (connectionClient, dbName, logger) => {
	const currentDbConnectionClient = await getClient(connectionClient, dbName,  {
		action: 'getting full text indexes',
		objects: [
			'sys.fulltext_indexes',
			'sys.fulltext_index_columns',
			'sys.indexes',
			'sys.fulltext_stoplists',
			'sys.registered_search_property_lists',
			'sys.filegroups',
			'sys.fulltext_catalogs',
		],
		skip: true
	}, logger);
	
	const result = await currentDbConnectionClient.query`
		SELECT
			OBJECT_SCHEMA_NAME(F.object_id) as schemaName,
			OBJECT_NAME(F.object_id) as TableName,
			COL_NAME(FC.object_id, FC.column_id) as columnName,
			COL_NAME(FC.object_id, FC.type_column_id) as columnTypeName,
			FC.statistical_semantics AS statistical_semantics,
			FC.language_id AS language,
			I.name AS indexKeyName,
			F.change_tracking_state_desc AS changeTracking,
			CASE WHEN F.stoplist_id IS NULL THEN 'OFF' WHEN F.stoplist_id = 0 THEN 'SYSTEM' ELSE SL.name END AS stopListName,
			SPL.name AS searchPropertyList,
			FG.name AS fileGroup,
			FCAT.name AS catalogName,
			type = 'FullText',
			IndexName = 'full_text_idx'
		FROM sys.fulltext_indexes F
		INNER JOIN sys.fulltext_index_columns FC ON FC.object_id = F.object_id
		LEFT JOIN sys.indexes I ON F.unique_index_id = I.index_id AND I.object_id = F.object_id
		LEFT JOIN sys.fulltext_stoplists SL ON SL.stoplist_id = F.stoplist_id
		LEFT JOIN sys.registered_search_property_lists SPL ON SPL.property_list_id = F.property_list_id
		LEFT JOIN sys.filegroups FG ON FG.data_space_id = F.data_space_id
		LEFT JOIN sys.fulltext_catalogs FCAT ON FCAT.fulltext_catalog_id = F.fulltext_catalog_id
		WHERE F.is_enabled = 1`;

	return mapResponse(result);
};

const getViewsIndexes = async (connectionClient, dbName, logger) => {
	const currentDbConnectionClient = await getClient(connectionClient, dbName, {
		action: 'getting view indexes query',
		objects: [
			'sys.indexes',
			'sys.views',
			'sys.index_columns',
			'sys.partitions',
		],
		skip: true
	}, logger);
	return mapResponse(await currentDbConnectionClient.query`
		SELECT
			TableName = t.name,
			IndexName = ind.name,
			ic.is_descending_key,
			ic.is_included_column,
			COL_NAME(t.object_id, ic.column_id) as columnName,
			OBJECT_SCHEMA_NAME(t.object_id) as schemaName,
			p.data_compression_desc as dataCompression,
			ind.*
		FROM sys.indexes ind
		LEFT JOIN sys.views t
			ON ind.object_id = t.object_id
		INNER JOIN sys.index_columns ic
			ON ind.object_id = ic.object_id AND ind.index_id = ic.index_id
		INNER JOIN sys.partitions p
			ON p.object_id = t.object_id AND ind.index_id = p.index_id
		WHERE
			ind.is_primary_key = 0
			AND ind.is_unique_constraint = 0
			AND t.is_ms_shipped = 0
		`);
};

const getTableColumnsDescription = async (connectionClient, dbName, tableName, schemaName, logger) => {
	const currentDbConnectionClient = await getClient(connectionClient, dbName, {
		action: 'getting table columns description',
		objects: [
			'sys.tables',
			'sys.columns',
			'sys.extended_properties',
		],
		skip: true
	}, logger);
	return mapResponse(currentDbConnectionClient.query`
		select
			st.name [Table],
			sc.name [Column],
			sep.value [Description]
		from sys.tables st
		inner join sys.columns sc on st.object_id = sc.object_id
		left join sys.extended_properties sep on st.object_id = sep.major_id
														and sc.column_id = sep.minor_id
														and sep.name = 'MS_Description'
		where st.name = ${tableName}
		and st.schema_id=schema_id(${schemaName})
	`);
};

const getDatabaseMemoryOptimizedTables = async (connectionClient, dbName, logger) => {
	const currentDbConnectionClient = await getClient(connectionClient, dbName, {
		action: 'getting memory optimized tables',
		objects: [
			'sys.tables',
			'sys.objects',
		],
		skip: true
	}, logger);

	return mapResponse(await currentDbConnectionClient.query`
		SELECT
			T.name,
			T.durability,
			T.durability_desc,
			OBJECT_NAME(T.history_table_id) AS history_table,
			SCHEMA_NAME(O.schema_id) AS history_schema,
			T.temporal_type_desc
		FROM sys.tables T LEFT JOIN sys.objects O ON T.history_table_id = O.object_id
		WHERE T.is_memory_optimized=1
	`);
};

const getDatabaseCheckConstraints = async (connectionClient, dbName, logger) => {
	const currentDbConnectionClient = await getClient(connectionClient, dbName, {
		action: 'getting check constraints',
		objects: [
			'sys.check_constraints',
			'sys.objects',
			'sys.all_columns',
		],
		skip: true
	}, logger);
	return mapResponse(currentDbConnectionClient.query`
		select con.[name],
			t.[name] as [table],
			col.[name] as column_name,
			con.[definition],
			con.[is_not_trusted],
			con.[is_disabled],
			con.[is_not_for_replication]
		from sys.check_constraints con
		left outer join sys.objects t
			on con.parent_object_id = t.object_id
		left outer join sys.all_columns col
			on con.parent_column_id = col.column_id
			and con.parent_object_id = col.object_id
	`);
};

const getViewTableInfo = async (connectionClient, dbName, viewName, schemaName, logger) => {
	const currentDbConnectionClient = await getClient(connectionClient, dbName, {
		action: 'information view query',
		objects: [
			'sys.sql_dependencies',
			'sys.objects',
			'sys.columns',
			'sys.types',
		],
		skip: true
	}, logger);
	const objectId = `${schemaName}.${viewName}`;
	return mapResponse(currentDbConnectionClient.query`
		SELECT
			ViewName = O.name,
			ColumnName = A.name,
			ReferencedSchemaName = SCHEMA_NAME(X.schema_id),
			ReferencedTableName = X.name,
			ReferencedColumnName = C.name,
			T.is_selected,
			T.is_updated,
			T.is_select_all,
			ColumnType = M.name,
			M.max_length,
			M.precision,
			M.scale
		FROM
			sys.sql_dependencies AS T
			INNER JOIN sys.objects AS O ON T.object_id = O.object_id
			INNER JOIN sys.objects AS X ON T.referenced_major_id = X.object_id
			INNER JOIN sys.columns AS C ON
				C.object_id = X.object_id AND
				C.column_id = T.referenced_minor_id
			INNER JOIN sys.types AS M ON
				M.system_type_id = C.system_type_id AND
				M.user_type_id = C.user_type_id
			INNER JOIN sys.columns AS A ON
				A.object_id = object_id(${objectId}) AND
				T.referenced_minor_id = A.column_id
		WHERE
			O.type = 'V'
		AND
			O.name = ${viewName}
		And O.schema_id=schema_id(${schemaName})
		ORDER BY
			O.name,
			X.name,
			C.name
	`);
};

const getViewColumnRelations = async (connectionClient, dbName, viewName, schemaName, logger) => {
	const currentDbConnectionClient = await getClient(connectionClient, dbName, {
		action: 'getting view column relations',
		objects: [
			'sys.dm_exec_describe_first_result_set',
		],
		skip: true
	}, logger);
	return mapResponse(currentDbConnectionClient
	.request()
	.input('tableName', sql.VarChar, viewName)
	.input('tableSchema', sql.VarChar, schemaName)
	.query`
			SELECT name, source_database, source_schema,
				source_table, source_column
				FROM sys.dm_exec_describe_first_result_set(N'SELECT TOP 1 * FROM [' + @TableSchema + '].[' + @TableName + ']', null, 1)
			WHERE is_hidden=0
	`);
};

const getViewStatement = async (connectionClient, dbName, viewName, schemaName, logger) => {
	const currentDbConnectionClient = await getClient(connectionClient, dbName, {
		action: 'getting view statements',
		objects: [
			'sys.sql_modules',
			'sys.views',
		],
		skip: true
	}, logger);
	const objectId = `${schemaName}.${viewName}`;
	return mapResponse(currentDbConnectionClient
		.query`SELECT M.*, V.with_check_option
			FROM sys.sql_modules M INNER JOIN sys.views V ON M.object_id=V.object_id
			WHERE M.object_id=object_id(${objectId})
		`);
};

const getTableKeyConstraints = async (connectionClient, dbName, tableName, schemaName, logger) => {
	const currentDbConnectionClient = await getClient(connectionClient, dbName, {
		action: 'getting constraints of keys',
		objects: [
			'information_schema.table_constraints',
			'information_schema.constraint_column_usage',
			'sys.indexes',
			'sys.stats',
			'sys.data_spaces',
			'sys.index_columns',
			'sys.partitions',
		],
		skip: true
	}, logger);
	const objectId = `${schemaName}.${tableName}`;
	return mapResponse(await currentDbConnectionClient.query`
		SELECT TC.TABLE_NAME as tableName, TC.Constraint_Name as constraintName,
		CC.Column_Name as columnName, TC.constraint_type as constraintType, ind.type_desc as typeDesc,
		p.data_compression_desc as dataCompression,
		ds.name as dataSpaceName,
		st.no_recompute as statisticNoRecompute, st.is_incremental as statisticsIncremental,
		ic.is_descending_key as isDescending,
		ind.*
		FROM information_schema.table_constraints TC
		INNER JOIN information_schema.constraint_column_usage CC on TC.Constraint_Name = CC.Constraint_Name
			AND TC.TABLE_NAME=${tableName} AND TC.TABLE_SCHEMA=${schemaName}
		INNER JOIN sys.indexes ind ON ind.name = TC.CONSTRAINT_NAME
		INNER JOIN sys.stats st ON st.name = TC.CONSTRAINT_NAME
		INNER JOIN sys.data_spaces ds ON ds.data_space_id = ind.data_space_id
		INNER JOIN sys.index_columns ic ON ic.object_id = object_id(${objectId})
			AND ind.index_id=ic.index_id
			AND ic.column_id=COLUMNPROPERTY(object_id(${objectId}), CC.column_name, 'ColumnId')
		INNER JOIN sys.partitions p ON p.object_id = object_id(${objectId}) AND p.index_id = ind.index_id
		ORDER BY TC.Constraint_Name
	`);
};

const getTableMaskedColumns = async (connectionClient, dbName, tableName, schemaName, logger) => {
	const currentDbConnectionClient = await getClient(connectionClient, dbName, {
		action: 'getting masked columns',
		objects: [
			'sys.masked_columns'
		],
		skip: true
	}, logger);
	const objectId = `${schemaName}.${tableName}`;
	return mapResponse(await currentDbConnectionClient.query`
		select name, masking_function from sys.masked_columns
		where object_id=object_id(${objectId})
	`);
};

const getDatabaseXmlSchemaCollection = async (connectionClient, dbName, logger) => {
	const currentDbConnectionClient = await getClient(connectionClient, dbName, {
		action: 'getting xml schema collections',
		objects: [
			'sys.column_xml_schema_collection_usages',
			'sys.xml_schema_collections'
		],
		skip: true
	}, logger);
	return mapResponse( await currentDbConnectionClient.query`
		SELECT xsc.name as collectionName,
				SCHEMA_NAME(xsc.schema_id) as schemaName,
				OBJECT_NAME(xcu.object_id) as tableName,
				COL_NAME(xcu.object_id, xcu.column_id) as columnName
		FROM sys.column_xml_schema_collection_usages xcu
		LEFT JOIN sys.xml_schema_collections xsc ON xsc.xml_collection_id=xcu.xml_collection_id
	`);
};

const getTableDefaultConstraintNames = async (connectionClient, dbName, tableName, schemaName, logger) => {
	const currentDbConnectionClient = await getClient(connectionClient, dbName, {
		action: 'getting default cosntraint names',
		objects: [
			'sys.all_columns',
			'sys.tables',
			'sys.schemas',
			'sys.default_constraints',
		],
		skip: true
	}, logger);
	return mapResponse(await currentDbConnectionClient.query`
	SELECT
		ac.name as columnName,
		dc.name
	FROM 
		sys.all_columns as ac
			INNER JOIN
		sys.tables
			ON ac.object_id = tables.object_id
			INNER JOIN 
		sys.schemas
			ON tables.schema_id = schemas.schema_id
			INNER JOIN
		sys.default_constraints as dc
			ON ac.default_object_id = dc.object_id
	WHERE 
			schemas.name = ${schemaName}
		AND tables.name = ${tableName}
	`);
};

const getDatabaseUserDefinedTypes = async (connectionClient, dbName, logger) => {
	const currentDbConnectionClient = await getClient(connectionClient, dbName, {
		action: 'getting user defined types',
		objects: [
			'sys.types',
		],
		skip: true
	}, logger);
	return mapResponse(currentDbConnectionClient.query`
		select * from sys.types
		where is_user_defined = 1
	`);
}

const mapResponse = async (response = {}) => {
	return (await response).recordset;
}

module.exports = {
	getConnectionClient,
	getObjectsFromDatabase,
	getTableInfo,
	getTableRow,
	getTableForeignKeys,
	getDatabaseIndexes,
	getTableColumnsDescription,
	getDatabaseMemoryOptimizedTables,
	getDatabaseCheckConstraints,
	getViewTableInfo,
	getTableKeyConstraints,
	getViewColumnRelations,
	getTableMaskedColumns,
	getDatabaseXmlSchemaCollection,
	getTableDefaultConstraintNames,
	getDatabaseUserDefinedTypes,
	getViewStatement,
	getViewsIndexes,
	getFullTextIndexes,
	getSpatialIndexes,
	getIndexesBucketCount,
}
