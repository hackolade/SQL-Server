const BEFORE_DEACTIVATED_STATEMENT = '-- ';
const REG_FOR_MULTYLINE_COMMENT = /(\n\/\*\n[\s\S]*?\n\s\*\/\n)|((\n\/\*\n[\s\S]*?\n\s\*\/)$)/gi;

const commentIfDeactivated = (statement, data, isPartOfLine) => {
	if (data?.hasOwnProperty('isActivated') && !data.isActivated) {
		if (isPartOfLine) {
			return '/* ' + statement + ' */';
		} else if (statement.includes('\n')) {
			return '/*\n' + statement + ' */\n';
		} else {
			return BEFORE_DEACTIVATED_STATEMENT + statement;
		}
	}
	return statement;
};

const filterDeactivatedQuery = query => query.replace(REG_FOR_MULTYLINE_COMMENT, '');

const queryIsDeactivated = (query = '') => query.startsWith(BEFORE_DEACTIVATED_STATEMENT);

module.exports = {
	commentIfDeactivated,
	filterDeactivatedQuery,
	queryIsDeactivated,
};
