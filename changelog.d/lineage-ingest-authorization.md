### Changes

- The OpenLineage ingest endpoints now enforce authorization; previously they authenticated the
  caller but performed no privilege checks. Output datasets require `TABLE_INGEST_LINEAGE` or
  `VIEW_INGEST_LINEAGE`, or `TABLE_INGEST_LINEAGE` on the parent namespace when the output table
  does not exist yet, and input datasets require `TABLE_READ_PROPERTIES` or `VIEW_READ_PROPERTIES`.
  Datasets the caller is not authorized for, or that do not exist, are omitted rather than failing
  the request, so an event that authorizes nothing succeeds while recording nothing. Responses report
  the number of omitted datasets in `summary.omitted` for batches and in the
  `Polaris-Lineage-Omitted` header for single events. Principals that sent lineage before this
  change need these grants to keep recording it.
