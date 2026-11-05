import { createAdapterFactory } from 'better-auth/adapters';
import { AsyncLocalStorage } from 'node:async_hooks';
import { DateTime, QueryError, RecordId, } from 'surrealdb';
function isTableNotFoundError(err) {
    if (!err || typeof err !== 'object')
        return false;
    const e = err;
    const msg = typeof e.message === 'string' ? e.message : '';
    return msg.includes('does not exist') || e.kind === 'NotFound';
}
function deserializeValue(val) {
    if (val instanceof RecordId)
        return String(val.id);
    if (val instanceof DateTime)
        return val.toDate();
    return val;
}
function deserializeRecord(record) {
    if (!record || typeof record !== 'object')
        return record;
    const out = {};
    for (const [key, val] of Object.entries(record)) {
        out[key] = deserializeValue(val);
    }
    return out;
}
function buildObjectBindings(obj, prefix) {
    const bindings = {};
    const parts = [];
    let i = 0;
    for (const [key, val] of Object.entries(obj)) {
        const p = `${prefix}${i++}`;
        bindings[p] = val;
        parts.push(`${key}: $${p}`);
    }
    return { expr: `{ ${parts.join(', ')} }`, bindings };
}
function buildWhereClause(where, model) {
    if (!where.length)
        return { sql: '', bindings: {} };
    const parts = [];
    const bindings = {};
    let paramIdx = 0;
    for (let i = 0; i < where.length; i++) {
        const { field, value, operator, connector, mode } = where[i];
        const isInsensitive = mode === 'insensitive';
        const connector_ = i === 0 ? '' : `${connector} `;
        // NULL comparisons use IS NULL / IS NOT NULL
        if (value === null) {
            if (operator === 'ne') {
                parts.push(`${connector_}${field} IS NOT NULL`);
            }
            else {
                parts.push(`${connector_}${field} IS NULL`);
            }
            continue;
        }
        const param = `pw${paramIdx++}`;
        // ID fields must be compared as RecordId objects
        if (field === 'id') {
            if (operator === 'in' || operator === 'not_in') {
                bindings[param] = value.map((v) => new RecordId(model, v));
            }
            else {
                bindings[param] = new RecordId(model, String(value));
            }
        }
        else if (isInsensitive) {
            if (typeof value === 'string') {
                bindings[param] = value.toLowerCase();
            }
            else if (Array.isArray(value)) {
                bindings[param] = value.map((v) => typeof v === 'string' ? v.toLowerCase() : v);
            }
            else {
                bindings[param] = value;
            }
        }
        else {
            bindings[param] = value;
        }
        const fieldExpr = isInsensitive && field !== 'id'
            ? `string::lowercase(${field})`
            : field;
        const valueExpr = `$${param}`;
        let condition;
        switch (operator) {
            case 'ne':
                condition = `${fieldExpr} != ${valueExpr}`;
                break;
            case 'lt':
                condition = `${fieldExpr} < ${valueExpr}`;
                break;
            case 'lte':
                condition = `${fieldExpr} <= ${valueExpr}`;
                break;
            case 'gt':
                condition = `${fieldExpr} > ${valueExpr}`;
                break;
            case 'gte':
                condition = `${fieldExpr} >= ${valueExpr}`;
                break;
            case 'in':
                condition = `${fieldExpr} INSIDE ${valueExpr}`;
                break;
            case 'not_in':
                condition = `${fieldExpr} NOTINSIDE ${valueExpr}`;
                break;
            case 'contains':
                condition = isInsensitive
                    ? `string::lowercase(${field}) CONTAINS ${valueExpr}`
                    : `${field} CONTAINS ${valueExpr}`;
                break;
            case 'starts_with':
                condition = isInsensitive
                    ? `string::starts_with(string::lowercase(${field}), ${valueExpr})`
                    : `string::starts_with(${field}, ${valueExpr})`;
                break;
            case 'ends_with':
                condition = isInsensitive
                    ? `string::ends_with(string::lowercase(${field}), ${valueExpr})`
                    : `string::ends_with(${field}, ${valueExpr})`;
                break;
            default: // 'eq'
                condition = `${fieldExpr} = ${valueExpr}`;
        }
        parts.push(`${connector_}${condition}`);
    }
    return { sql: `WHERE ${parts.join(' ')}`, bindings };
}
function baseSurrealType(type) {
    switch (type) {
        case 'string':
            return 'string';
        case 'number':
            return 'number';
        case 'boolean':
            return 'bool';
        case 'date':
            return 'datetime';
        case 'json':
            return 'object';
        case 'string[]':
            return 'array<string>';
        case 'number[]':
            return 'array<number>';
        default:
            return 'any';
    }
}
function mapFieldTypeToSurreal(type, required) {
    const base = baseSurrealType(type);
    if (required)
        return base;
    // Optional fields are typed as `option<T | null>`, which resolves to
    // `T | NULL | NONE`. That covers the three states better-auth produces for a
    // nullable field: a typed value, a stored NULL (sent to clear the field),
    // and a missing value (NONE). This keeps the field typed instead of `any`.
    if (base === 'any')
        return 'option<any>';
    return `option<${base} | null>`;
}
export const surrealAdapter = (config) => {
    let lazyOptions = null;
    const txContext = new AsyncLocalStorage();
    const currentDb = () => txContext.getStore()?.db ?? config.db;
    const tbl = (model) => `\`${model}\``;
    const runQuery = async (sql, bindings) => {
        const result = await currentDb().query(sql, bindings);
        return result[0] ?? [];
    };
    const MAX_CONFLICT_ATTEMPTS = 5;
    /**
     * True for SurrealDB's retriable write-conflict error. 3.1.0+ sets the
     * structured flag; older servers report it only in the message.
     */
    const isRetriableConflict = (err) => {
        if (!(err instanceof QueryError))
            return false;
        return (err.message.includes('conflict') ||
            err.message.includes('can be retried'));
    };
    /**
     * Retries retriable transaction conflicts with a small linear backoff.
     * Only applied outside transactions (inside one, commit-time conflict
     * handling owns the outcome), and only safe because the retried work is
     * a single statement: selector and mutation re-evaluate together, so a
     * conflicted loser observes the winner's commit on the next attempt.
     */
    const withConflictRetry = async (run) => {
        for (let attempt = 1;; attempt += 1) {
            try {
                return await run();
            }
            catch (err) {
                if (attempt >= MAX_CONFLICT_ATTEMPTS ||
                    txContext.getStore() !== undefined ||
                    !isRetriableConflict(err))
                    throw err;
                await new Promise((resolve) => setTimeout(resolve, attempt * 10));
            }
        }
    };
    const findManyRaw = async (model, where, limit) => {
        try {
            const { sql: whereSql, bindings } = buildWhereClause(where, model);
            const rows = await runQuery(`SELECT * FROM ${tbl(model)} ${whereSql} LIMIT ${limit}`, bindings);
            return rows.map(deserializeRecord);
        }
        catch (err) {
            if (isTableNotFoundError(err))
                return [];
            throw err;
        }
    };
    const applyJoins = async (record, join) => {
        const result = { ...record };
        for (const [joinModel, joinAttr] of Object.entries(join)) {
            const fromValue = record[joinAttr.on.from];
            if (fromValue == null) {
                result[joinModel] =
                    joinAttr.relation === 'one-to-one' ? null : [];
                continue;
            }
            const joinWhere = [
                {
                    field: joinAttr.on.to,
                    value: fromValue,
                    operator: 'eq',
                    connector: 'AND',
                    mode: 'sensitive',
                },
            ];
            if (joinAttr.relation === 'one-to-one') {
                const rows = await findManyRaw(joinModel, joinWhere, 1);
                result[joinModel] = rows[0] ?? null;
            }
            else {
                const rows = await findManyRaw(joinModel, joinWhere, joinAttr.limit ?? 100);
                result[joinModel] = rows;
            }
        }
        return result;
    };
    const adapterCreator = createAdapterFactory({
        config: {
            adapterId: 'surrealdb',
            adapterName: 'SurrealDB Adapter',
            usePlural: config.usePlural ?? false,
            supportsJSON: true,
            supportsDates: true,
            supportsBooleans: true,
            supportsArrays: true,
            supportsNumericIds: false,
            supportsUUIDs: false,
            transaction: async (cb) => {
                const trx = await config.db.beginTransaction();
                const ctx = { db: trx, created: [] };
                try {
                    const result = await txContext.run(ctx, () => cb(adapterCreator(lazyOptions)));
                    await trx.commit();
                    return result;
                }
                catch (err) {
                    try {
                        await trx.cancel();
                    }
                    catch {
                        // cancel is best-effort; SurrealDB in-memory doesn't honour it
                    }
                    for (const { model, id } of ctx.created) {
                        try {
                            await config.db.query('DELETE $rid', {
                                rid: new RecordId(model, id),
                            });
                        }
                        catch {
                            // ignore cleanup errors
                        }
                    }
                    throw err;
                }
            },
        },
        adapter: ({ getFieldName }) => ({
            create: async ({ model, data, }) => {
                const insertData = { ...data };
                if (insertData.id && typeof insertData.id === 'string') {
                    insertData.id = new RecordId(model, insertData.id);
                }
                // SurrealDB v3 does not accept $obj in CONTENT clause; expand to field bindings
                const { expr, bindings } = buildObjectBindings(insertData, 'fi');
                const rows = await runQuery(`INSERT INTO ${tbl(model)} ${expr} RETURN AFTER`, bindings);
                const record = deserializeRecord(rows[0]);
                const ctx = txContext.getStore();
                if (ctx && record?.id) {
                    ctx.created.push({ model, id: record.id });
                }
                return record;
            },
            findOne: async ({ model, where, join, }) => {
                try {
                    const { sql: whereSql, bindings } = buildWhereClause(where, model);
                    const rows = await runQuery(`SELECT * FROM ${tbl(model)} ${whereSql} LIMIT 1`, bindings);
                    if (!rows.length)
                        return null;
                    const record = deserializeRecord(rows[0]);
                    if (!join)
                        return record;
                    return applyJoins(record, join);
                }
                catch (err) {
                    if (isTableNotFoundError(err))
                        return null;
                    throw err;
                }
            },
            findMany: async ({ model, where, limit, select, sortBy, offset, join, }) => {
                try {
                    const { sql: whereSql, bindings } = buildWhereClause(where ?? [], model);
                    const orderSql = sortBy
                        ? `ORDER BY ${sortBy.field} ${sortBy.direction.toUpperCase()}`
                        : '';
                    const startSql = offset != null ? `START AT ${offset}` : '';
                    const rows = await runQuery(`SELECT * FROM ${tbl(model)} ${whereSql} ${orderSql} LIMIT ${limit} ${startSql}`, bindings);
                    const records = rows.map(deserializeRecord);
                    const projected = select?.length
                        ? records.map((r) => {
                            const out = {};
                            for (const f of select) {
                                const dbField = getFieldName({
                                    model,
                                    field: f,
                                });
                                out[dbField] = r[dbField];
                            }
                            return out;
                        })
                        : records;
                    if (!join)
                        return projected;
                    return Promise.all(projected.map((r) => applyJoins(r, join)));
                }
                catch (err) {
                    if (isTableNotFoundError(err))
                        return [];
                    throw err;
                }
            },
            update: async ({ model, where, update, }) => {
                const { sql: whereSql, bindings: whereBindings } = buildWhereClause(where, model);
                // SurrealDB v3 does not accept $obj in MERGE clause; expand to SET bindings
                const { expr: setExpr, bindings: setBindings } = buildObjectBindings(update, 'fu');
                try {
                    const rows = await runQuery(`UPDATE ${tbl(model)} MERGE ${setExpr} ${whereSql} RETURN AFTER`, { ...setBindings, ...whereBindings });
                    return rows.length
                        ? deserializeRecord(rows[0])
                        : null;
                }
                catch (err) {
                    if (isTableNotFoundError(err))
                        return null;
                    throw err;
                }
            },
            updateMany: async ({ model, where, update, }) => {
                const { sql: whereSql, bindings: whereBindings } = buildWhereClause(where, model);
                const { expr: setExpr, bindings: setBindings } = buildObjectBindings(update, 'fu');
                try {
                    const rows = await runQuery(`UPDATE ${tbl(model)} MERGE ${setExpr} ${whereSql} RETURN AFTER`, { ...setBindings, ...whereBindings });
                    return rows.length;
                }
                catch (err) {
                    if (isTableNotFoundError(err))
                        return 0;
                    throw err;
                }
            },
            delete: async ({ model, where, }) => {
                const { sql: whereSql, bindings } = buildWhereClause(where, model);
                try {
                    await currentDb().query(`DELETE ${tbl(model)} ${whereSql}`, bindings);
                }
                catch (err) {
                    if (!isTableNotFoundError(err))
                        throw err;
                }
            },
            deleteMany: async ({ model, where, }) => {
                const { sql: whereSql, bindings } = buildWhereClause(where, model);
                try {
                    const rows = await runQuery(`DELETE ${tbl(model)} ${whereSql} RETURN BEFORE`, bindings);
                    return rows.length;
                }
                catch (err) {
                    if (isTableNotFoundError(err))
                        return 0;
                    throw err;
                }
            },
            consumeOne: async ({ model, where, }) => {
                const { sql: whereSql, bindings } = buildWhereClause(where, model);
                // SurrealDB v3 has no LIMIT on DELETE; the one-row bound lives
                // in an inner SELECT — the officially documented workaround
                // (https://surrealdb.com/docs/reference/query-language/statements/delete).
                // Selector and mutation evaluate in one storage-engine step, so
                // concurrent consumers cannot both win the same row and a guard
                // that matches nothing changes nothing.
                try {
                    const rows = await withConflictRetry(() => runQuery(`DELETE FROM (SELECT * FROM ${tbl(model)} ${whereSql} LIMIT 1) RETURN BEFORE`, bindings));
                    return rows.length
                        ? deserializeRecord(rows[0])
                        : null;
                }
                catch (err) {
                    if (isTableNotFoundError(err))
                        return null;
                    throw err;
                }
            },
            incrementOne: async ({ model, where, increment, set, }) => {
                const assignments = [];
                const assignBindings = {};
                let idx = 0;
                for (const [field, delta] of Object.entries(increment)) {
                    assignments.push(`${field} += $inc${idx}`);
                    assignBindings[`inc${idx}`] = delta;
                    idx += 1;
                }
                idx = 0;
                for (const [field, value] of Object.entries(set ?? {})) {
                    assignments.push(`${field} = $set${idx}`);
                    assignBindings[`set${idx}`] = value;
                    idx += 1;
                }
                if (assignments.length === 0) {
                    throw new Error('incrementOne requires at least one increment or set field');
                }
                const { sql: whereSql, bindings } = buildWhereClause(where, model);
                // A field present in both increment and set receives the
                // increment then the set (set wins) — no better-auth call site
                // mixes both on one field.
                try {
                    const rows = await withConflictRetry(() => runQuery(`UPDATE (SELECT * FROM ${tbl(model)} ${whereSql} LIMIT 1) SET ${assignments.join(', ')} RETURN AFTER`, { ...bindings, ...assignBindings }));
                    return rows.length
                        ? deserializeRecord(rows[0])
                        : null;
                }
                catch (err) {
                    if (isTableNotFoundError(err))
                        return null;
                    throw err;
                }
            },
            count: async ({ model, where, }) => {
                try {
                    const { sql: whereSql, bindings } = buildWhereClause(where ?? [], model);
                    const rows = await runQuery(`SELECT count() AS total FROM ${tbl(model)} ${whereSql} GROUP ALL`, bindings);
                    return rows[0]?.total ?? 0;
                }
                catch (err) {
                    if (isTableNotFoundError(err))
                        return 0;
                    throw err;
                }
            },
            createSchema: async ({ file, tables, }) => {
                const usePlural = config.usePlural ?? false;
                const tableMode = config.schemaMode === 'schemaless'
                    ? 'SCHEMALESS'
                    : 'SCHEMAFULL';
                const toTable = (modelName) => usePlural ? `${modelName}s` : modelName;
                const toField = (tableKey, fieldKey) => tables[tableKey]?.fields?.[fieldKey]?.fieldName ?? fieldKey;
                const lines = [
                    '-- Generated by @surrealdb/better-auth',
                    '-- Run this against your SurrealDB instance to define your schema',
                    '',
                ];
                for (const [, model] of Object.entries(tables)) {
                    const tableName = toTable(model.modelName);
                    lines.push(`DEFINE TABLE IF NOT EXISTS ${tableName} ${tableMode} COMMENT 'Better Auth ${model.modelName} table';`);
                    for (const [fieldName, field] of Object.entries(model.fields)) {
                        const dbField = field.fieldName ?? fieldName;
                        const fieldTypeStr = Array.isArray(field.type)
                            ? 'string'
                            : field.type;
                        const surrealType = mapFieldTypeToSurreal(fieldTypeStr, field.required !== false);
                        // Object (json) fields hold arbitrary nested keys, so they
                        // need FLEXIBLE for SurrealDB to accept undeclared subfields
                        // on a SCHEMAFULL table. FLEXIBLE is specified after TYPE.
                        const flexible = fieldTypeStr === 'json' ? ' FLEXIBLE' : '';
                        lines.push(`DEFINE FIELD IF NOT EXISTS ${dbField} ON TABLE ${tableName} TYPE ${surrealType}${flexible};`);
                        // A unique constraint is also an index, so only emit one.
                        if (field.unique) {
                            lines.push(`DEFINE INDEX IF NOT EXISTS idx_${tableName}_${dbField} ON TABLE ${tableName} FIELDS ${dbField} UNIQUE;`);
                        }
                        else if (field.index) {
                            lines.push(`DEFINE INDEX IF NOT EXISTS idx_${tableName}_${dbField} ON TABLE ${tableName} FIELDS ${dbField};`);
                        }
                    }
                    lines.push('');
                }
                if (tables.member) {
                    const mTbl = toTable(tables.member.modelName);
                    const mUserId = toField('member', 'userId');
                    const mOrgId = toField('member', 'organizationId');
                    const mRole = toField('member', 'role');
                    lines.push('-- fn::auth::organization::member_of($userId, $organizationId) -> bool');
                    lines.push('-- Returns true if the user is a member of the organization.');
                    lines.push(`DEFINE FUNCTION IF NOT EXISTS fn::auth::organization::member_of($userId: string, $organizationId: string) -> bool {`);
                    lines.push(`    RETURN count(SELECT id FROM ${mTbl} WHERE ${mUserId} = $userId AND ${mOrgId} = $organizationId LIMIT 1) > 0;`);
                    lines.push(`};`);
                    lines.push('');
                    lines.push('-- fn::auth::organization::get_role($userId, $organizationId) -> option<string>');
                    lines.push("-- Returns the user's role string (e.g. 'owner', 'admin', 'member') or NONE.");
                    lines.push(`DEFINE FUNCTION IF NOT EXISTS fn::auth::organization::get_role($userId: string, $organizationId: string) -> option<string> {`);
                    lines.push(`    RETURN array::first(SELECT VALUE ${mRole} FROM ${mTbl} WHERE ${mUserId} = $userId AND ${mOrgId} = $organizationId LIMIT 1);`);
                    lines.push(`};`);
                    lines.push('');
                    lines.push('-- fn::auth::organization::has_role($userId, $organizationId, $minRole) -> bool');
                    lines.push('-- Returns true if the user holds at least the given role.');
                    lines.push('-- Role hierarchy: owner (3) > admin (2) > member (1).');
                    lines.push(`DEFINE FUNCTION IF NOT EXISTS fn::auth::organization::has_role($userId: string, $organizationId: string, $minRole: string) -> bool {`);
                    lines.push(`    LET $role = fn::auth::organization::get_role($userId, $organizationId);`);
                    lines.push(`    IF $role IS NONE { RETURN false };`);
                    lines.push(`    LET $rank = { owner: 3, admin: 2, member: 1 };`);
                    lines.push(`    RETURN ($rank[$role] ?? 0) >= ($rank[$minRole] ?? 0);`);
                    lines.push(`};`);
                    lines.push('');
                    lines.push('-- fn::auth::organization::members($organizationId) -> array');
                    lines.push('-- Returns all member records for the organization.');
                    lines.push(`DEFINE FUNCTION IF NOT EXISTS fn::auth::organization::members($organizationId: string) -> array {`);
                    lines.push(`    RETURN SELECT * FROM ${mTbl} WHERE ${mOrgId} = $organizationId;`);
                    lines.push(`};`);
                    lines.push('');
                    // Organization teams helper, emitted only when team tables exist.
                    if (tables.team) {
                        const tTbl = toTable(tables.team.modelName);
                        const tOrgId = toField('team', 'organizationId');
                        lines.push('-- fn::auth::organization::teams($organizationId) -> array');
                        lines.push('-- Returns all team records that belong to the organization.');
                        lines.push(`DEFINE FUNCTION IF NOT EXISTS fn::auth::organization::teams($organizationId: string) -> array {`);
                        lines.push(`    RETURN SELECT * FROM ${tTbl} WHERE ${tOrgId} = $organizationId;`);
                        lines.push(`};`);
                        lines.push('');
                    }
                    // Dynamic permission helper, emitted only when the organizationRole table exists.
                    if (tables.organizationRole) {
                        const orTbl = toTable(tables.organizationRole.modelName);
                        const orOrgId = toField('organizationRole', 'organizationId');
                        const orRole = toField('organizationRole', 'role');
                        const orPerm = toField('organizationRole', 'permission');
                        lines.push('-- fn::auth::organization::has_permission($userId, $organizationId, $resource, $action) -> bool');
                        lines.push('-- Returns true if the user has the given resource+action permission.');
                        lines.push('-- Requires the organization plugin with dynamicAccessControl enabled.');
                        lines.push(`DEFINE FUNCTION IF NOT EXISTS fn::auth::organization::has_permission($userId: string, $organizationId: string, $resource: string, $action: string) -> bool {`);
                        lines.push(`    LET $role = fn::auth::organization::get_role($userId, $organizationId);`);
                        lines.push(`    IF $role IS NONE { RETURN false };`);
                        lines.push(`    LET $rows = (SELECT ${orPerm} FROM ${orTbl} WHERE ${orOrgId} = $organizationId AND ${orRole} = $role LIMIT 1);`);
                        lines.push(`    IF array::len($rows) == 0 { RETURN false };`);
                        lines.push(`    LET $raw = $rows[0].${orPerm};`);
                        lines.push(`    IF $raw = NONE OR $raw = "" { RETURN false };`);
                        lines.push(`    LET $perms = encoding::json::decode($raw);`);
                        lines.push(`    LET $actions = $perms[$resource];`);
                        lines.push(`    RETURN type::is_array($actions) AND $actions CONTAINS $action;`);
                        lines.push(`};`);
                        lines.push('');
                    }
                }
                if (tables.teamMember) {
                    const tmTbl = toTable(tables.teamMember.modelName);
                    const tmUserId = toField('teamMember', 'userId');
                    const tmTeamId = toField('teamMember', 'teamId');
                    lines.push('-- fn::auth::team::member_of($userId, $teamId) -> bool');
                    lines.push('-- Returns true if the user is a member of the team.');
                    lines.push(`DEFINE FUNCTION IF NOT EXISTS fn::auth::team::member_of($userId: string, $teamId: string) -> bool {`);
                    lines.push(`    RETURN count(SELECT id FROM ${tmTbl} WHERE ${tmUserId} = $userId AND ${tmTeamId} = $teamId LIMIT 1) > 0;`);
                    lines.push(`};`);
                    lines.push('');
                    lines.push('-- fn::auth::team::members($teamId) -> array');
                    lines.push('-- Returns all teamMember records for the team.');
                    lines.push(`DEFINE FUNCTION IF NOT EXISTS fn::auth::team::members($teamId: string) -> array {`);
                    lines.push(`    RETURN SELECT * FROM ${tmTbl} WHERE ${tmTeamId} = $teamId;`);
                    lines.push(`};`);
                    lines.push('');
                }
                const code = lines.join('\n');
                const path = file ?? 'schema.surql';
                return { code, path };
            },
        }),
    });
    return (options) => {
        lazyOptions = options;
        const a = adapterCreator(options);
        // Prevent the test framework from voiding the transaction property.
        const t = a.transaction;
        Object.defineProperty(a, 'transaction', {
            get: () => t,
            set: () => { },
            enumerable: true,
            configurable: true,
        });
        return a;
    };
};
