const sql = require('mssql');
const { getObjectsFromDatabase, getNewConnectionClientByDb } = require('./helpers');

const getConnectionClient = async connectionInfo => {
	if (connectionInfo.authMethod === 'Username / Password') {
		return await sql.connect({
			user: connectionInfo.userName,
			password: connectionInfo.userPassword,
			server: connectionInfo.host,
			port: connectionInfo.port,
			database: connectionInfo.databaseName,
			options: {
				encrypt: true,
			},
		});
	}

	return await sql.connect(connectionInfo.connectionString);
};

const getTableInfo = async (connectionClient, dbName, tableName, tableSchema) => {
	const currentDbConnectionClient = await getNewConnectionClientByDb(connectionClient, dbName);
	const objectId = `${tableSchema}.${tableName}`;
	return await currentDbConnectionClient.query`
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
	;`
};

const getTableRow = async (connectionClient, dbName, tableName, tableSchema, reverseEngineeringOptions) => {
	const currentDbConnectionClient = await getNewConnectionClientByDb(connectionClient, dbName);
	const percentageWord = reverseEngineeringOptions.isAbsoluteValue ? '' : 'PERCENT';
	try {
		return await currentDbConnectionClient
			.request()
			.input('tableName', sql.VarChar, tableName)
			.input('tableSchema', sql.VarChar, tableSchema)
			.input('amount', sql.Int, reverseEngineeringOptions.value)
			.input('percent', sql.VarChar, percentageWord)
			.query`EXEC('SELECT TOP '+ @Amount +' '+ @Percent +' * FROM [' + @TableSchema + '].[' + @TableName + '];');`;
	} catch (e) {
		return [];
	}
};

const getTableForeignKeys = async (connectionClient, dbName) => {
	const currentDbConnectionClient = await getNewConnectionClientByDb(connectionClient, dbName);
	return await currentDbConnectionClient.query`
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
		`
};

const getDatabaseIndexes = async (connectionClient, dbName) => {
	const currentDbConnectionClient = await getNewConnectionClientByDb(connectionClient, dbName);
	return await currentDbConnectionClient.query`
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
		`;
};

const getTableColumnsDescription = async (connectionClient, dbName, tableName, schemaName) => {
	const currentDbConnectionClient = await getNewConnectionClientByDb(connectionClient, dbName);
	return currentDbConnectionClient.query`
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
	`;
};

const getDatabaseMemoryOptimizedTables = async (connectionClient, dbName) => {
	const currentDbConnectionClient = await getNewConnectionClientByDb(connectionClient, dbName);
	return currentDbConnectionClient.query`
		SELECT o.name
		FROM sys.memory_optimized_tables_internal_attributes AS moa
		LEFT JOIN sys.objects o ON o.object_id=moa.object_id
		WHERE o.type='U'
	`;
};

const getDatabaseCheckConstraints = async (connectionClient, dbName) => {
	const currentDbConnectionClient = await getNewConnectionClientByDb(connectionClient, dbName);
	return currentDbConnectionClient.query`
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
	`;
};

const getViewTableInfo = async (connectionClient, dbName, viewName, schemaName) => {
	const currentDbConnectionClient = await getNewConnectionClientByDb(connectionClient, dbName);
	const objectId = `${schemaName}.${viewName}`;
	return currentDbConnectionClient.query`
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
	`;
};

const getViewColumnRelations = async (connectionClient, dbName, viewName, schemaName) => {
	const currentDbConnectionClient = await getNewConnectionClientByDb(connectionClient, dbName);
	return currentDbConnectionClient
		.request()
		.input('tableName', sql.VarChar, viewName)
		.input('tableSchema', sql.VarChar, schemaName)
		.query`
			SELECT name, source_database, source_schema,
				source_table, source_column
			FROM sys.dm_exec_describe_first_result_set(N'SELECT TOP 1 * FROM [' + @TableSchema + '].[' + @TableName + ']', null, 1)
			WHERE is_hidden=0
	`;
};

const getTableKeyConstraints = async (connectionClient, dbName, tableName, schemaName) => {
	const currentDbConnectionClient = await getNewConnectionClientByDb(connectionClient, dbName);
	const objectId = `${schemaName}.${tableName}`;
	return currentDbConnectionClient.query`
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
	`;
};

const getTableMaskedColumns = async (connectionClient, dbName, tableName, schemaName) => {
	const currentDbConnectionClient = await getNewConnectionClientByDb(connectionClient, dbName);
	const objectId = `${schemaName}.${tableName}`;
	return currentDbConnectionClient.query`
		select name, masking_function from sys.masked_columns
		where object_id=object_id(${objectId})
	`;
};

const getDatabaseXmlSchemaCollection = async (connectionClient, dbName) => {
	const currentDbConnectionClient = await getNewConnectionClientByDb(connectionClient, dbName);
	return currentDbConnectionClient.query`
		SELECT xsc.name as collectionName,
				SCHEMA_NAME(xsc.schema_id) as schemaName,
				OBJECT_NAME(xcu.object_id) as tableName,
				COL_NAME(xcu.object_id, xcu.column_id) as columnName
		FROM sys.column_xml_schema_collection_usages xcu
		LEFT JOIN sys.xml_schema_collections xsc ON xsc.xml_collection_id=xcu.xml_collection_id
	`
};

const getTableDefaultConstraintNames = async (connectionClient, dbName, tableName, schemaName) => {
	const currentDbConnectionClient = await getNewConnectionClientByDb(connectionClient, dbName);
	return currentDbConnectionClient.query`
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
	`
};

const getDatabaseUserDefinedTypes = async (connectionClient, dbName) => {
	const currentDbConnectionClient = await getNewConnectionClientByDb(connectionClient, dbName);
	return currentDbConnectionClient.query`
		select * from sys.types
		where is_user_defined = 1
	`;
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
}
