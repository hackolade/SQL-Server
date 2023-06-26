'use strict';

const { getAlterScriptDtos, joinAlterScriptDtosIntoAlterScript } = require('../alterScriptFromDeltaHelper');

/**
 * @typedef {import('./helpers/alterScriptHelpers/types/AlterScriptDto').AlterScriptDto} AlterScriptDto
 * @typedef {import('./types/coreApplicationTypes').App} App
 * @typedef {import('./types/coreApplicationTypes').CoreData} CoreData
 * */

/**
 * @typedef {{
 *     externalDefinitions: unknown,
 *     modelDefinitions: unknown,
 *     jsonSchema: unknown,
 *     internalDefinitions: unknown
 * }} EntityLevelAlterScriptData
 * */

/**
 * @typedef {{
 *     externalDefinitions: unknown,
 *     modelDefinitions: unknown,
 *     entitiesJsonSchema: unknown,
 *     internalDefinitions: unknown
 * }} ContainerLevelAlterScriptData
 * */

/**
 * @param data {CoreData}
 * @param app {App}
 * @return {(dto: EntityLevelAlterScriptData) => Array<AlterScriptDto>}
 * */
const getEntityLevelAlterScriptDtos = (data, app) => ({
														  externalDefinitions,
														  modelDefinitions,
														  jsonSchema,
														  internalDefinitions
													  }) => {
	const definitions = [modelDefinitions, internalDefinitions, externalDefinitions];

	return getAlterScriptDtos(jsonSchema, app, data.options);
};

/**
 * @param data {CoreData}
 * @param app {App}
 * @return {(dto: EntityLevelAlterScriptData) => string}
 * */
const buildEntityLevelAlterScript = (data, app) => (entityLevelAlterScriptDto) => {
	const alterScriptDtos = getEntityLevelAlterScriptDtos(data, app)(entityLevelAlterScriptDto);

	return joinAlterScriptDtosIntoAlterScript(alterScriptDtos, data);
};

module.exports = {
	buildEntityLevelAlterScript,
};
