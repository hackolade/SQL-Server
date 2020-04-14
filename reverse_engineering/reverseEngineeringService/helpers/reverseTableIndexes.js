const COLUMNSTORE = 'Columnstore';
const INDEX = 'Index';
const FULL_TEXT = 'FullText';

const handleDataCompression = index => {
	const compressionTypes = ['NONE', 'ROW', 'PAGE', 'COLUMNSTORE', 'COLUMNSTORE_ARCHIVE'];
	const type = compressionTypes.find(type => index.dataCompression.includes(type));
	return type || '';
};

const isClusteredIndex = index => !index.type_desc.includes('NONCLUSTERED');
const getIndexType = index => {
	if (index.type === 5 || index.type === 6) {
		return COLUMNSTORE;
	} else if (index.type === 'FullText') {
		return FULL_TEXT;
	} else {
		return INDEX;
	}
};

const getIndexData = index => ({
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
	indxHash: index.type_desc === "NONCLUSTERED HASH",
	indxBucketCount: !isNaN(Number(index.total_bucket_count)) ? Number(index.total_bucket_count) : '',
	indxFilterExpression: index.has_filter ? index.filter_definition : '',
});

const getFullTextIndex = index => ({
	indxName: index.IndexName,
	indxType: FULL_TEXT,
	indxFullTextKeyIndex: index.indexKeyName,
	indxFullTextCatalogName: index.catalogName,
	indxFullTextFileGroup: index.fileGroup === 'PRIMARY' ? '' : index.fileGroup,
	indxFullTextChangeTracking: index.changeTracking,
	indxFullTextStopList: ['OFF', 'SYSTEM'].includes(index.stopListName) ? index.stopListName : 'Stoplist name',
	indxFullTextStopListName: !['OFF', 'SYSTEM'].includes(index.stopListName) ? index.stopListName : '',
	indxFullTextSearchPropertyList: index.searchPropertyList,
});

const reverseIndex = index => {
	if (getIndexType(index) === FULL_TEXT) {
		return getFullTextIndex(index);
	} else {
		return getIndexData(index);
	}
};

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

const getFullTextKeys = (index) => {
	const key = { name: index.columnName };

	if (!index.columnTypeName && !index.language) {
		return { key };
	}

	const properties = {
		columnType: index.columnTypeName,
		languageTerm: index.language,
		statisticalSemantics: Boolean(index.statistical_semantics),
	};

	return {
		key,
		properties
	};
};

const addKeys = (indexData, index) => {
	if (getIndexType(index) === FULL_TEXT) {
		const data = getFullTextKeys(index);
		return {
			...indexData,
			indxKey: [...(indexData.indxKey || []), data.key],
			indxFullTextKeysProperties: data.properties ? [...(indexData.indxFullTextKeysProperties || []), data.properties] : [],
		};
	} else {
		return {
			...indexData,
			indxKey: [...(indexData.indxKey || []), reverseIndexKey(index)].filter(Boolean),
			indxInclude: [...(indexData.indxInclude || []), reverseIncludedKey(index)].filter(Boolean),
		};
	}
};

const reverseTableIndexes = tableIndexes =>
	Object.values(tableIndexes.reduce((indexList, index) => {
		let existedIndex = indexList[index.IndexName];

		if (!existedIndex) {
			existedIndex = reverseIndex(index);
		}

		return {
			...indexList,
			[index.IndexName]: addKeys(existedIndex, index)
		};
	}, {}));

module.exports = reverseTableIndexes;
