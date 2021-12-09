const getPeriodForSystemTime = (tableRows) => {
	const periodForSystemTime = Object.entries(tableRows).reduce((period, [colName, properties]) => {
		if(properties.generatedAlwaysType === 'AS_ROW_START'){
			return {...period, startTime: {name: colName, type: properties.isHidden ? 'hidden': ''}}
		}
		if(properties.generatedAlwaysType === 'AS_ROW_END'){
			return {...period, endTime: {name: colName, type: properties.isHidden ? 'hidden': ''}}
		}
		return period;
	 }, {})
	if(!periodForSystemTime.startTime || !periodForSystemTime.endTime){
		return;
	}
	return [{
		startTime: [periodForSystemTime.startTime],
		endTime: [periodForSystemTime.endTime],
	}]
};

module.exports = getPeriodForSystemTime;
