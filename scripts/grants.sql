-- Least-privilege grants (run by an admin once the `data_science` group exists).
-- Data scientists read gold + ml, nothing else; engineers keep write access via ownership.
GRANT USE CATALOG ON CATALOG agripulse TO `data_science`;
GRANT USE SCHEMA, SELECT ON SCHEMA agripulse.gold TO `data_science`;
GRANT USE SCHEMA, SELECT ON SCHEMA agripulse.ml TO `data_science`;
-- silver / bronze / ops stay engineer-only by default (no grant).
