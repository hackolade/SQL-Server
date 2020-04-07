const COLUMNSTORE = 'Columnstore';
const INDEX = 'Index';

const handleDataCompression = index => {
	const compressionTypes = ['NONE', 'ROW', 'PAGE', 'COLUMNSTORE', 'COLUMNSTORE_ARCHIVE'];
	const type = compressionTypes.find(type => index.dataCompression.includes(type));
	return type || '';
};

const isClusteredIndex = index => !index.type_desc.includes('NONCLUSTERED');
const getIndexType = index => (index.type === 5 || index.type === 6) ? COLUMNSTORE : INDEX;

const reverseIndex = index => ({
	indxName: index.IndexName,
	ALLOW_ROW_LOCKS: index.allow_row_locks,
	ALLOW_PAGE_LOCKS: index.allow_page_locks,
	uniqueIndx: index.is_unique,
	clusteredIndx: isClusteredIndex(index),
	IGNORE_DUP_KEY: index.ignore_dup_key,
	indxType: getIndexType(index),
	COMPRESSION_DELAY: index.compression_delay,
	OPTIMIZE_FOR_SEQUENTIAL_KEY: Boolean(index.optimize_for_sequential_key),
	PAD_INDEX: Boolean(index.is_padded),
	FILLFACTOR: index.fill_factor,
	DATA_COMPRESSION: handleDataCompression(index),
});

const reverseIndexKey = index => {
	const indexType = getIndexType(index);
	if (index.is_included_column && indexType !== COLUMNSTORE) {
		return null;
	}

	return {
		name: index.columnName,
		type: index.is_descending_key ? 'descending' : 'ascending',
	};
};

const reverseIncludedKey = index => {
	const indexType = getIndexType(index);
	if (!index.is_included_column || indexType === COLUMNSTORE) {
		return null;
	}

	return {
		name: index.columnName,
		type: index.is_descending_key ? 'descending' : 'ascending',
	};
};

const reverseTableIndexes = tableIndexes =>
	Object.values(tableIndexes.reduce((indexList, index) => {
		const existedIndex = indexList[index.IndexName];
		if (existedIndex) {
			return {
				...indexList,
				[index.IndexName]: {
					...existedIndex,
					indxKey: [...existedIndex.indxKey, reverseIndexKey(index)].filter(Boolean),
					indxInclude: [...existedIndex.indxInclude, reverseIncludedKey(index)].filter(Boolean),
				}
			};
		}


		return {
			...indexList,
			[index.IndexName]: {
				...reverseIndex(index),
				indxKey: [reverseIndexKey(index)].filter(Boolean),
				indxInclude: [reverseIncludedKey(index)].filter(Boolean),
			}
		};
	}, {}));

module.exports = reverseTableIndexes;
