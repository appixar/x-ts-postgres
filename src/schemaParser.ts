// ─────────────────────────────────────────────
// x-postgres — YAML schema parser
// ─────────────────────────────────────────────
// Parses the YAML field DSL into structured ParsedSchema.
// Port of PHP convertField().

import type { CustomFieldDef, FieldDefinition, ParsedSchema } from './types.js';

/**
 * Parse YAML table columns (the raw field map from a .yml table entry)
 * into a structured ParsedSchema with typed fields, indexes, and constraints.
 *
 * @param fields - Raw YAML fields: `{ column_name: "type modifier1 modifier2 ..." }`
 * @param customFields - Custom field type definitions from config
 */
export function parseSchema(
    fields: Record<string, string> | null | undefined,
    customFields: Record<string, CustomFieldDef>
): ParsedSchema {
    const result: ParsedSchema = {
        fields: {},
        individualIndexes: [],
        compositeIndexes: {},
        compositeUniqueIndexes: {},
    };

    if (!fields || typeof fields !== 'object') return result;

    for (const [fieldName, rawValue] of Object.entries(fields)) {
        // Skip meta keys like ~ignore
        if (fieldName.startsWith('~')) continue;
        if (typeof rawValue !== 'string') continue;

        const parts = rawValue.split(/\s+/);
        const typePart = parts[0];

        // Extract base type and optional length: "type/length" → type, length
        const [typeAlias, lengthStr] = typePart.split('/');

        // Resolve custom field type
        const customDef = customFields[typeAlias];
        let typeReal = customDef?.Type ?? typeAlias;

        // Apply explicit length override
        if (lengthStr) {
            // Remove any existing (N) from the type
            typeReal = typeReal.replace(/\(\d+(?:,\d+)?\)/, '');
            typeReal = `${typeReal}(${lengthStr})`;
        }

        // ─── Default ───
        let defaultRaw: string | null = null;
        for (const part of parts) {
            if (part.startsWith('default/')) {
                defaultRaw = part.substring('default/'.length);
                break;
            }
        }
        // Fallback: default from custom field definition
        if (defaultRaw === null && customDef?.Default) {
            defaultRaw = customDef.Default;
        }

        // ─── Nullable ───
        let nullable: string;
        if (typeAlias === 'id' || typeReal.toUpperCase().includes('SERIAL')) {
            nullable = ''; // SERIAL = implicit NOT NULL
        } else {
            nullable = parts.includes('required') ? 'NOT NULL' : 'NULL';
        }

        // ─── Key ───
        let key = '';
        const customKey = customDef?.Key;
        if (parts.includes('unique')) key = 'UNI'; // simple unique (no group)
        if (customKey) key = customKey;

        // ─── Extra ───
        const extra = (customDef?.Extra ?? '').toUpperCase();

        // ─── Build field definition ───
        result.fields[fieldName] = {
            field: fieldName,
            type: typeReal.toUpperCase(),
            nullable,
            key,
            defaultValue: defaultRaw,
            extra,
        };

        // ─── Indexes ───
        for (const part of parts) {
            // "index" or "index/group1,group2"
            if (part === 'index' || part.startsWith('index/')) {
                const indexParts = part.split('/');
                if (indexParts[1]) {
                    // Composite index
                    const groupNames = indexParts[1].split(',');
                    for (const groupName of groupNames) {
                        if (!result.compositeIndexes[groupName]) {
                            result.compositeIndexes[groupName] = [];
                        }
                        result.compositeIndexes[groupName].push(fieldName);
                    }
                } else {
                    // Individual index
                    if (!result.individualIndexes.includes(fieldName)) {
                        result.individualIndexes.push(fieldName);
                    }
                }
            }

            // "unique/group" = composite unique
            if (part.startsWith('unique/')) {
                const uniqueParts = part.split('/');
                if (uniqueParts[1]) {
                    const groupNames = uniqueParts[1].split(',');
                    for (const groupName of groupNames) {
                        if (!result.compositeUniqueIndexes[groupName]) {
                            result.compositeUniqueIndexes[groupName] = [];
                        }
                        result.compositeUniqueIndexes[groupName].push(fieldName);
                    }
                }
            }
        }
    }

    return result;
}
