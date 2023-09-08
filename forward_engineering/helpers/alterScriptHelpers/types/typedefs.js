/**
 * @typedef {Object} Collection
 * @property {string} type
 * @property {boolean} isActivated
 * @property {boolean} unique
 * @property {boolean} hasMaxLength
 * @property {string} subtype
 * @property {Object} properties
 * @property {Object} compMod
 * @property {Array<string>} compositeKey
 * @property {boolean} compositePrimaryKey
 * @property {boolean} compositeUniqueKey
 * @property {Object} role
 * @property {string} GUID
 */

/**
 * @typedef {Object} ColumnCompModField
 * @property {string} type
 * @property {string} name
 * @property {string} mode
 */

/**
 * @typedef {[
 *     "compositePartitionKey",
 *     "compositePrimaryKey",
 *     "compositeUniqueKey",
 *     "triggerUpdateColumns",
 * ]} CompositeKey
 */

/**
 * @typedef {Object} AlterCollectionColumnCompModDto
 * @property {ColumnCompModField} oldField
 * @property {ColumnCompModField} newField
 */

/**
 * @typedef {Object} AlterCollectionColumnPrimaryKeyOptionDto
 * @property {string} id
 * @property {string} constraintName
 * @property {string} indexStorageParameters
 * @property {string} indexTablespace
 * @property {Array<{ keyId: string, [type]: string}>} indexInclude
 */

/**
 * @typedef {Object} AlterCollectionColumnDto
 * @property {string} type
 * @property {boolean} isActivated
 * @property {boolean} primaryKey
 * @property {boolean} unique
 * @property {string} mode
 * @property {number} length
 * @property {CompositeKey} compositeKey
 * @property {boolean} compositePartitionKey
 * @property {boolean} compositePrimaryKey
 * @property {boolean} compositeUniqueKey
 * @property {Array<AlterCollectionColumnPrimaryKeyOptionDto> | AlterCollectionColumnPrimaryKeyOptionDto | undefined} primaryKeyOptions
 * @property {boolean} triggerUpdateColumns
 * @property {AlterCollectionColumnCompModDto} compMod
 * @property {string} GUID
 */

/**
 * @typedef {Object} AlterCollectionRoleDefinitionDto
 * @property {string} id
 * @property {string} type
 * @property {Array<unknown>} properties
 */

/**
 * @typedef {Object} AlterCollectionRoleCompModPKDto
 * @extends AlterCollectionColumnPrimaryKeyOptionDto
 * @property {Array<{ type: string, keyId: string }>} compositePrimaryKey
 */

/**
 * @typedef {Object} AlterCollectionRoleCompModPrimaryKey
 * @property {Array<AlterCollectionRoleCompModPKDto> | undefined} new
 * @property {Array<AlterCollectionRoleCompModPKDto> | undefined} old
 */

/**
 * @typedef {Object} AlterCollectionRoleCompModDto
 * @property {string} keyspaceName
 * @property {{ name: string, isActivated: boolean, ifNotExist: boolean }} bucketProperties
 * @property {{ new: string, old: string }} collectionName
 * @property {{ new: string, old: string }} collectionName
 * @property {{ new: boolean, old: boolean }} isActivated
 * @property {{ new: string, old: string }} bucketId
 * @property {{ new: boolean, old: boolean }} ifNotExist
 * @property {{ new: string, old: string }} on_commit
 * @property {AlterCollectionRoleCompModPrimaryKey} primaryKey
 * @property {{ new: string, old: string }} table_tablespace_name
 * @property {Array<{ [propertyName: string]: AlterCollectionColumnDto }>} newProperties
 */

/**
 * @typedef {Object} AlterCollectionRoleDto
 * @property {string} id
 * @property {string} type
 * @property {string} collectionName
 * @property {{ [propertyName: string]: AlterCollectionColumnDto }} properties
 * @property {AlterCollectionRoleDefinitionDto} definitions
 * @property {boolean} isActivated
 * @property {boolean} additionalProperties
 * @property {boolean} memory_optimized
 * @property {Array<unknown>} collectionUsers
 * @property {boolean} ifNotExist
 * @property {string} on_commit
 * @property {string} table_tablespace_name
 * @property {string} bucketId
 * @property {AlterCollectionRoleCompModDto} compMod
 * @property {string} name
 * @property {'entity'} roleType
 * @property {Array<unknown>} patternProperties
 */

/**
 * @typedef {Object} AlterCollectionDto
 * @property {'object'} type
 * @property {boolean} isActivated
 * @property {boolean} unique
 * @property {'object'} subtype
 * @property {{ [propertyName: string]: AlterCollectionColumnDto }} properties
 * @property {CompositeKey} compositeKey
 * @property {boolean} compositePartitionKey
 * @property {boolean} compositePrimaryKey
 * @property {boolean} compositeUniqueKey
 * @property {boolean} triggerUpdateColumns
 * @property {AlterCollectionRoleDto} role
 * @property {string} GUID
 */

/**
 * @typedef {Object} AlterRelationshipFKField
 * @property {boolean} isActivated
 * @property {string} name
 */

/**
 * @typedef {Object} AlterRelationshipParentDto
 * @property {{
 *     name: string,
 *     isActivated: boolean
 * }} bucket
 * @property {{
 *     name: string,
 *     isActivated: boolean,
 *     fkFields: Array<AlterRelationshipFKField>
 * }} collection
 */

/**
 * @typedef {Object} AlterRelationshipChildDto
 * @property {{
 *     name: string,
 *     isActivated: boolean
 * }} bucket
 * @property {{
 *     name: string,
 *     isActivated: boolean,
 *     fkFields: Array<AlterRelationshipFKField>
 * }} collection
 */

/**
 * @typedef {Object} AlterRelationshipCustomProperties
 * @property {string} relationshipOnDelete
 * @property {string} relationshipOnUpdate
 * @property {string} relationshipMatch
 */

/**
 * @typedef {Object} AlterRelationshipRoleCompModDto
 * @property {boolean} created
 * @property {boolean} deleted
 * @property {boolean | undefined} modified
 * @property {AlterRelationshipParentDto} parent
 * @property {AlterRelationshipParentDto} child
 * @property {{
 *     new: string,
 *     old: string,
 * } | undefined} name
 * @property {{
 *     new: string,
 *     old: string,
 * } | undefined} description
 * @property {{
 *     old?: AlterRelationshipCustomProperties,
 *     new?: AlterRelationshipCustomProperties
 * } | undefined} customProperties
 * @property {{
 *     new?: boolean,
 *     old?: boolean,
 * }} isActivated
 */

/**
 * @typedef {Object} AlterRelationshipRoleDto
 * @property {string} id
 * @property {string} name
 * @property {string} parentCardinality
 * @property {'Foreign Key'} relationshipType
 * @property {Array<Array<string>>} parentField
 * @property {Array<Array<string>>} childField
 * @property {boolean} isActivated
 * @property {string} childCardinality
 * @property {string} parentCollection
 * @property {string} childCollection
 * @property {Object} hackoladeStyles
 * @property {AlterRelationshipRoleCompModDto} compMod
 * @property {'relationship'} roleType
 */

/**
 * @typedef {Object} AlterRelationshipDto
 * @property {'object'} type
 * @property {'object'} subtype
 * @property {boolean} isActivated
 * @property {boolean} unique
 * @property {boolean} compositePartitionKey
 * @property {boolean} compositePrimaryKey
 * @property {boolean} compositeUniqueKey
 * @property {boolean} triggerUpdateColumns
 * @property {AlterRelationshipRoleDto} role
 * @property {[
 *     "compositePartitionKey",
 *     "compositePrimaryKey",
 *     "compositeUniqueKey",
 *     "triggerUpdateColumns",
 * ]} compositeKey
 * @property {string} GUID
 */
