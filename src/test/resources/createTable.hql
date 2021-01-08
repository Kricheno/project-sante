CREATE EXTERNAL TABLE IF NOT EXISTS `sante`.`aides_ext` (
    `key` string COMMENT 'from deserializer',
    `nombre_aides` double COMMENT 'from deserializer',
    `montant_total` double COMMENT 'from deserializer',
    `loc` string COMMENT 'from deserializer'
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.hbase.HBaseSerDe'
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
'hbase.columns.mapping'=':key,\nnombre_aides:nombre_aides#b,\nmontant_total:montant_total#b,\nloc:libelle_region','serialization.format'='1'
)
TBLPROPERTIES (
    'hbase.table.name'='sante:aides','hbase.mapred.output.outputtable'='sante:aides')