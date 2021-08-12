const getAllDatabasesTablesInfo = async (client, includeViews = true) => {
	const viewJoin = includeViews ? `LEFT JOIN '+ QUOTENAME(name) + '.sys.views v ON v.schema_id = t.schema_id` : '';
	const orderByView = includeViews ? `, view_name` : '';
	const viewName = includeViews ? `, v.name AS view_name` : '';
	return await client.request()
		.query(`
			DECLARE @sql nvarchar(max);
			SELECT @sql = 
				(SELECT ' UNION ALL
					SELECT ' +  + QUOTENAME(name,'''') + ' AS database_name,
								s.name AS schema_name,
								t.name COLLATE DATABASE_DEFAULT AS table_name
								${viewName}
								FROM '+ QUOTENAME(name) + '.sys.tables t
								LEFT JOIN '+ QUOTENAME(name) + '.sys.schemas s ON s.schema_id = t.schema_id
								${viewJoin}
								WHERE t.is_ms_shipped = 0
								'
					FROM sys.databases
					WHERE state=0
					ORDER BY [name] FOR XML PATH(''), type).value('.', 'nvarchar(max)');

			SET @sql = STUFF(@sql, 1, 12, '') + ' ORDER BY database_name,
															table_name
															${orderByView}';

			EXECUTE (@sql);
		`);
};

module.exports = getAllDatabasesTablesInfo;
