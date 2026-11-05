import type { BetterAuthOptions } from 'better-auth';
import { type Surreal } from 'surrealdb';
export interface SurrealDBAdapterConfig {
    db: Surreal;
    usePlural?: boolean;
    /**
     * Table definition mode for generated schema.
     *
     * - `schemafull` (default): every known field is typed and constrained.
     *   Writes to fields not in the generated schema are rejected, so re-run
     *   schema generation after adding a plugin or additional fields.
     * - `schemaless`: known fields are still typed and indexed, but writes to
     *   unknown fields are accepted. Use this when an app adds many dynamic
     *   plugin fields and does not want to regenerate the schema each time.
     */
    schemaMode?: 'schemafull' | 'schemaless';
}
export declare const surrealAdapter: (config: SurrealDBAdapterConfig) => (options: BetterAuthOptions) => import("better-auth").DBAdapter<BetterAuthOptions>;
//# sourceMappingURL=index.d.ts.map