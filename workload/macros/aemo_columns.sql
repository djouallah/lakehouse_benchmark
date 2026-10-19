{#--
    THE AEMO CSV layout — the single source of truth for every engine.

    Before this file existed the same lists were written out four times (a Jinja loop in
    the iceberg repo, literal expansion in ducklake and delta, a Jinja list + cast_floats()
    in dwh) — about 1,200 lines that were verified byte-for-byte identical as data and had
    no business being copies. Every dialect's reader macro and every CAST select is now
    generated from here, so a layout change lands in one place for all five engines.

    Column ORDER is load-bearing: AEMO CSVs are headerless positional records, dwh's
    OPENROWSET binds by ORDINAL, and Spark's from_csv() binds by position too. Never
    reorder this list to make it tidier — append only.

    `aemo_spec(record)` returns, for record in price | scada | price_today | scada_today:
      columns     [[name, duckdb_type], ...]  in file order
      not_double  names NOT cast to DOUBLE in the select tail (identifiers, timestamps, flags)
      equals      [[column, literal], ...]     the record-selection predicate, as DATA —
                  each dialect renders it with its own quoting, but the RULE is here
      nonzero     a column that must be <> 0, or none
      source_type the stg_csv_archive_log.source_type this record is landed under
      unique_key  the natural key the incremental strategies dedupe on
--#}

{% macro aemo_spec(record) %}
  {% if record == 'price' %}
    {{ return({
      'columns': [
        ['I', 'VARCHAR'], ['UNIT', 'VARCHAR'], ['XX', 'VARCHAR'],
        ['VERSION', 'VARCHAR'], ['SETTLEMENTDATE', 'VARCHAR'], ['RUNNO', 'VARCHAR'],
        ['REGIONID', 'VARCHAR'], ['INTERVENTION', 'VARCHAR'], ['RRP', 'VARCHAR'],
        ['EEP', 'VARCHAR'], ['ROP', 'VARCHAR'], ['APCFLAG', 'VARCHAR'],
        ['MARKETSUSPENDEDFLAG', 'VARCHAR'], ['TOTALDEMAND', 'VARCHAR'], ['DEMANDFORECAST', 'VARCHAR'],
        ['DISPATCHABLEGENERATION', 'VARCHAR'], ['DISPATCHABLELOAD', 'VARCHAR'], ['NETINTERCHANGE', 'VARCHAR'],
        ['EXCESSGENERATION', 'VARCHAR'], ['LOWER5MINDISPATCH', 'VARCHAR'], ['LOWER5MINIMPORT', 'VARCHAR'],
        ['LOWER5MINLOCALDISPATCH', 'VARCHAR'], ['LOWER5MINLOCALPRICE', 'VARCHAR'], ['LOWER5MINLOCALREQ', 'VARCHAR'],
        ['LOWER5MINPRICE', 'VARCHAR'], ['LOWER5MINREQ', 'VARCHAR'], ['LOWER5MINSUPPLYPRICE', 'VARCHAR'],
        ['LOWER60SECDISPATCH', 'VARCHAR'], ['LOWER60SECIMPORT', 'VARCHAR'], ['LOWER60SECLOCALDISPATCH', 'VARCHAR'],
        ['LOWER60SECLOCALPRICE', 'VARCHAR'], ['LOWER60SECLOCALREQ', 'VARCHAR'], ['LOWER60SECPRICE', 'VARCHAR'],
        ['LOWER60SECREQ', 'VARCHAR'], ['LOWER60SECSUPPLYPRICE', 'VARCHAR'], ['LOWER6SECDISPATCH', 'VARCHAR'],
        ['LOWER6SECIMPORT', 'VARCHAR'], ['LOWER6SECLOCALDISPATCH', 'VARCHAR'], ['LOWER6SECLOCALPRICE', 'VARCHAR'],
        ['LOWER6SECLOCALREQ', 'VARCHAR'], ['LOWER6SECPRICE', 'VARCHAR'], ['LOWER6SECREQ', 'VARCHAR'],
        ['LOWER6SECSUPPLYPRICE', 'VARCHAR'], ['RAISE5MINDISPATCH', 'VARCHAR'], ['RAISE5MINIMPORT', 'VARCHAR'],
        ['RAISE5MINLOCALDISPATCH', 'VARCHAR'], ['RAISE5MINLOCALPRICE', 'VARCHAR'], ['RAISE5MINLOCALREQ', 'VARCHAR'],
        ['RAISE5MINPRICE', 'VARCHAR'], ['RAISE5MINREQ', 'VARCHAR'], ['RAISE5MINSUPPLYPRICE', 'VARCHAR'],
        ['RAISE60SECDISPATCH', 'VARCHAR'], ['RAISE60SECIMPORT', 'VARCHAR'], ['RAISE60SECLOCALDISPATCH', 'VARCHAR'],
        ['RAISE60SECLOCALPRICE', 'VARCHAR'], ['RAISE60SECLOCALREQ', 'VARCHAR'], ['RAISE60SECPRICE', 'VARCHAR'],
        ['RAISE60SECREQ', 'VARCHAR'], ['RAISE60SECSUPPLYPRICE', 'VARCHAR'], ['RAISE6SECDISPATCH', 'VARCHAR'],
        ['RAISE6SECIMPORT', 'VARCHAR'], ['RAISE6SECLOCALDISPATCH', 'VARCHAR'], ['RAISE6SECLOCALPRICE', 'VARCHAR'],
        ['RAISE6SECLOCALREQ', 'VARCHAR'], ['RAISE6SECPRICE', 'VARCHAR'], ['RAISE6SECREQ', 'VARCHAR'],
        ['RAISE6SECSUPPLYPRICE', 'VARCHAR'], ['AGGREGATEDISPATCHERROR', 'VARCHAR'], ['AVAILABLEGENERATION', 'VARCHAR'],
        ['AVAILABLELOAD', 'VARCHAR'], ['INITIALSUPPLY', 'VARCHAR'], ['CLEAREDSUPPLY', 'VARCHAR'],
        ['LOWERREGIMPORT', 'VARCHAR'], ['LOWERREGLOCALDISPATCH', 'VARCHAR'], ['LOWERREGLOCALREQ', 'VARCHAR'],
        ['LOWERREGREQ', 'VARCHAR'], ['RAISEREGIMPORT', 'VARCHAR'], ['RAISEREGLOCALDISPATCH', 'VARCHAR'],
        ['RAISEREGLOCALREQ', 'VARCHAR'], ['RAISEREGREQ', 'VARCHAR'], ['RAISE5MINLOCALVIOLATION', 'VARCHAR'],
        ['RAISEREGLOCALVIOLATION', 'VARCHAR'], ['RAISE60SECLOCALVIOLATION', 'VARCHAR'], ['RAISE6SECLOCALVIOLATION', 'VARCHAR'],
        ['LOWER5MINLOCALVIOLATION', 'VARCHAR'], ['LOWERREGLOCALVIOLATION', 'VARCHAR'], ['LOWER60SECLOCALVIOLATION', 'VARCHAR'],
        ['LOWER6SECLOCALVIOLATION', 'VARCHAR'], ['RAISE5MINVIOLATION', 'VARCHAR'], ['RAISEREGVIOLATION', 'VARCHAR'],
        ['RAISE60SECVIOLATION', 'VARCHAR'], ['RAISE6SECVIOLATION', 'VARCHAR'], ['LOWER5MINVIOLATION', 'VARCHAR'],
        ['LOWERREGVIOLATION', 'VARCHAR'], ['LOWER60SECVIOLATION', 'VARCHAR'], ['LOWER6SECVIOLATION', 'VARCHAR'],
        ['RAISE6SECRRP', 'VARCHAR'], ['RAISE6SECROP', 'VARCHAR'], ['RAISE6SECAPCFLAG', 'VARCHAR'],
        ['RAISE60SECRRP', 'VARCHAR'], ['RAISE60SECROP', 'VARCHAR'], ['RAISE60SECAPCFLAG', 'VARCHAR'],
        ['RAISE5MINRRP', 'VARCHAR'], ['RAISE5MINROP', 'VARCHAR'], ['RAISE5MINAPCFLAG', 'VARCHAR'],
        ['RAISEREGRRP', 'VARCHAR'], ['RAISEREGROP', 'VARCHAR'], ['RAISEREGAPCFLAG', 'VARCHAR'],
        ['LOWER6SECRRP', 'VARCHAR'], ['LOWER6SECROP', 'VARCHAR'], ['LOWER6SECAPCFLAG', 'VARCHAR'],
        ['LOWER60SECRRP', 'VARCHAR'], ['LOWER60SECROP', 'VARCHAR'], ['LOWER60SECAPCFLAG', 'VARCHAR'],
        ['LOWER5MINRRP', 'VARCHAR'], ['LOWER5MINROP', 'VARCHAR'], ['LOWER5MINAPCFLAG', 'VARCHAR'],
        ['LOWERREGRRP', 'VARCHAR'], ['LOWERREGROP', 'VARCHAR'], ['LOWERREGAPCFLAG', 'VARCHAR'],
        ['RAISE6SECACTUALAVAILABILITY', 'VARCHAR'], ['RAISE60SECACTUALAVAILABILITY', 'VARCHAR'], ['RAISE5MINACTUALAVAILABILITY', 'VARCHAR'],
        ['RAISEREGACTUALAVAILABILITY', 'VARCHAR'], ['LOWER6SECACTUALAVAILABILITY', 'VARCHAR'], ['LOWER60SECACTUALAVAILABILITY', 'VARCHAR'],
        ['LOWER5MINACTUALAVAILABILITY', 'VARCHAR'], ['LOWERREGACTUALAVAILABILITY', 'VARCHAR'], ['LORSURPLUS', 'VARCHAR'],
        ['LRCSURPLUS', 'VARCHAR']
      ],
      'not_double': ['I', 'UNIT', 'XX', 'SETTLEMENTDATE', 'REGIONID'],
      'equals': [['I', 'D'], ['UNIT', 'DREGION'], ['VERSION', '3']],
      'nonzero': none,
      'source_type': 'daily',
      'unique_key': ['file', 'REGIONID', 'SETTLEMENTDATE', 'INTERVENTION'],
    }) }}
  {% elif record == 'scada' %}
    {{ return({
      'columns': [
        ['I', 'VARCHAR'], ['UNIT', 'VARCHAR'], ['XX', 'VARCHAR'],
        ['VERSION', 'VARCHAR'], ['SETTLEMENTDATE', 'VARCHAR'], ['RUNNO', 'VARCHAR'],
        ['DUID', 'VARCHAR'], ['INTERVENTION', 'VARCHAR'], ['DISPATCHMODE', 'VARCHAR'],
        ['AGCSTATUS', 'VARCHAR'], ['INITIALMW', 'VARCHAR'], ['TOTALCLEARED', 'VARCHAR'],
        ['RAMPDOWNRATE', 'VARCHAR'], ['RAMPUPRATE', 'VARCHAR'], ['LOWER5MIN', 'VARCHAR'],
        ['LOWER60SEC', 'VARCHAR'], ['LOWER6SEC', 'VARCHAR'], ['RAISE5MIN', 'VARCHAR'],
        ['RAISE60SEC', 'VARCHAR'], ['RAISE6SEC', 'VARCHAR'], ['MARGINAL5MINVALUE', 'VARCHAR'],
        ['MARGINAL60SECVALUE', 'VARCHAR'], ['MARGINAL6SECVALUE', 'VARCHAR'], ['MARGINALVALUE', 'VARCHAR'],
        ['VIOLATION5MINDEGREE', 'VARCHAR'], ['VIOLATION60SECDEGREE', 'VARCHAR'], ['VIOLATION6SECDEGREE', 'VARCHAR'],
        ['VIOLATIONDEGREE', 'VARCHAR'], ['LOWERREG', 'VARCHAR'], ['RAISEREG', 'VARCHAR'],
        ['AVAILABILITY', 'VARCHAR'], ['RAISE6SECFLAGS', 'VARCHAR'], ['RAISE60SECFLAGS', 'VARCHAR'],
        ['RAISE5MINFLAGS', 'VARCHAR'], ['RAISEREGFLAGS', 'VARCHAR'], ['LOWER6SECFLAGS', 'VARCHAR'],
        ['LOWER60SECFLAGS', 'VARCHAR'], ['LOWER5MINFLAGS', 'VARCHAR'], ['LOWERREGFLAGS', 'VARCHAR'],
        ['RAISEREGAVAILABILITY', 'VARCHAR'], ['RAISEREGENABLEMENTMAX', 'VARCHAR'], ['RAISEREGENABLEMENTMIN', 'VARCHAR'],
        ['LOWERREGAVAILABILITY', 'VARCHAR'], ['LOWERREGENABLEMENTMAX', 'VARCHAR'], ['LOWERREGENABLEMENTMIN', 'VARCHAR'],
        ['RAISE6SECACTUALAVAILABILITY', 'VARCHAR'], ['RAISE60SECACTUALAVAILABILITY', 'VARCHAR'], ['RAISE5MINACTUALAVAILABILITY', 'VARCHAR'],
        ['RAISEREGACTUALAVAILABILITY', 'VARCHAR'], ['LOWER6SECACTUALAVAILABILITY', 'VARCHAR'], ['LOWER60SECACTUALAVAILABILITY', 'VARCHAR'],
        ['LOWER5MINACTUALAVAILABILITY', 'VARCHAR'], ['LOWERREGACTUALAVAILABILITY', 'VARCHAR']
      ],
      'not_double': ['I', 'UNIT', 'XX', 'SETTLEMENTDATE', 'DUID'],
      'equals': [['I', 'D'], ['UNIT', 'DUNIT'], ['VERSION', '3']],
      'nonzero': none,
      'source_type': 'daily',
      'unique_key': ['file', 'DUID', 'SETTLEMENTDATE', 'INTERVENTION'],
    }) }}
  {% elif record == 'price_today' %}
    {{ return({
      'columns': [
        ['I', 'VARCHAR'], ['DISPATCH', 'VARCHAR'], ['PRICE', 'VARCHAR'],
        ['xx', 'VARCHAR'], ['SETTLEMENTDATE', 'timestamp'], ['RUNNO', 'VARCHAR'],
        ['REGIONID', 'VARCHAR'], ['DISPATCHINTERVAL', 'VARCHAR'], ['INTERVENTION', 'VARCHAR'],
        ['RRP', 'VARCHAR'], ['EEP', 'VARCHAR'], ['ROP', 'VARCHAR'],
        ['APCFLAG', 'VARCHAR'], ['MARKETSUSPENDEDFLAG', 'VARCHAR'], ['LASTCHANGED', 'VARCHAR'],
        ['RAISE6SECRRP', 'VARCHAR'], ['RAISE6SECROP', 'VARCHAR'], ['RAISE6SECAPCFLAG', 'VARCHAR'],
        ['RAISE60SECRRP', 'VARCHAR'], ['RAISE60SECROP', 'VARCHAR'], ['RAISE60SECAPCFLAG', 'VARCHAR'],
        ['RAISE5MINRRP', 'VARCHAR'], ['RAISE5MINROP', 'VARCHAR'], ['RAISE5MINAPCFLAG', 'VARCHAR'],
        ['RAISEREGRRP', 'VARCHAR'], ['RAISEREGROP', 'VARCHAR'], ['RAISEREGAPCFLAG', 'VARCHAR'],
        ['LOWER6SECRRP', 'VARCHAR'], ['LOWER6SECROP', 'VARCHAR'], ['LOWER6SECAPCFLAG', 'VARCHAR'],
        ['LOWER60SECRRP', 'VARCHAR'], ['LOWER60SECROP', 'VARCHAR'], ['LOWER60SECAPCFLAG', 'VARCHAR'],
        ['LOWER5MINRRP', 'VARCHAR'], ['LOWER5MINROP', 'VARCHAR'], ['LOWER5MINAPCFLAG', 'VARCHAR'],
        ['LOWERREGRRP', 'VARCHAR'], ['LOWERREGROP', 'VARCHAR'], ['LOWERREGAPCFLAG', 'VARCHAR'],
        ['PRICE_STATUS', 'VARCHAR'], ['PRE_AP_ENERGY_PRICE', 'VARCHAR'], ['PRE_AP_RAISE6_PRICE', 'VARCHAR'],
        ['PRE_AP_RAISE60_PRICE', 'VARCHAR'], ['PRE_AP_RAISE5MIN_PRICE', 'VARCHAR'], ['PRE_AP_RAISEREG_PRICE', 'VARCHAR'],
        ['PRE_AP_LOWER6_PRICE', 'VARCHAR'], ['PRE_AP_LOWER60_PRICE', 'VARCHAR'], ['PRE_AP_LOWER5MIN_PRICE', 'VARCHAR'],
        ['PRE_AP_LOWERREG_PRICE', 'VARCHAR'], ['RAISE1SECRRP', 'VARCHAR'], ['RAISE1SECROP', 'VARCHAR'],
        ['RAISE1SECAPCFLAG', 'VARCHAR'], ['LOWER1SECRRP', 'VARCHAR'], ['LOWER1SECROP', 'VARCHAR'],
        ['LOWER1SECAPCFLAG', 'VARCHAR'], ['PRE_AP_RAISE1_PRICE', 'VARCHAR'], ['PRE_AP_LOWER1_PRICE', 'VARCHAR'],
        ['CUMUL_PRE_AP_ENERGY_PRICE', 'VARCHAR'], ['CUMUL_PRE_AP_RAISE6_PRICE', 'VARCHAR'], ['CUMUL_PRE_AP_RAISE60_PRICE', 'VARCHAR'],
        ['CUMUL_PRE_AP_RAISE5MIN_PRICE', 'VARCHAR'], ['CUMUL_PRE_AP_RAISEREG_PRICE', 'VARCHAR'], ['CUMUL_PRE_AP_LOWER6_PRICE', 'VARCHAR'],
        ['CUMUL_PRE_AP_LOWER60_PRICE', 'VARCHAR'], ['CUMUL_PRE_AP_LOWER5MIN_PRICE', 'VARCHAR'], ['CUMUL_PRE_AP_LOWERREG_PRICE', 'VARCHAR'],
        ['CUMUL_PRE_AP_RAISE1_PRICE', 'VARCHAR'], ['CUMUL_PRE_AP_LOWER1_PRICE', 'VARCHAR'], ['OCD_STATUS', 'VARCHAR'],
        ['MII_STATUS', 'VARCHAR']
      ],
      'not_double': ['I', 'DISPATCH', 'PRICE', 'xx', 'SETTLEMENTDATE', 'REGIONID', 'LASTCHANGED', 'PRICE_STATUS', 'OCD_STATUS', 'MII_STATUS'],
      'equals': [['I', 'D'], ['PRICE', 'PRICE']],
      'nonzero': none,
      'source_type': 'price_today',
      'unique_key': ['file', 'REGIONID', 'SETTLEMENTDATE', 'INTERVENTION'],
    }) }}
  {% elif record == 'scada_today' %}
    {{ return({
      'columns': [
        ['I', 'VARCHAR'], ['DISPATCH', 'VARCHAR'], ['UNIT_SCADA', 'VARCHAR'],
        ['xx', 'VARCHAR'], ['SETTLEMENTDATE', 'timestamp'], ['DUID', 'VARCHAR'],
        ['SCADAVALUE', 'double'], ['LASTCHANGED', 'timestamp']
      ],
      'not_double': [],
      'equals': [['I', 'D']],
      'nonzero': 'SCADAVALUE',
      'source_type': 'scada_today',
      'unique_key': ['file', 'DUID', 'SETTLEMENTDATE'],
    }) }}
  {% else %}
    {{ exceptions.raise_compiler_error("aemo_spec: unknown record '" ~ record ~ "' (expected price | scada | price_today | scada_today)") }}
  {% endif %}
{% endmacro %}

{#-- Column NAMES in file order. --#}
{% macro aemo_columns(record) %}
  {%- set spec = aemo_spec(record) -%}
  {{ return(spec['columns'] | map(attribute=0) | list) }}
{% endmacro %}


{#-- The subset cast to DOUBLE / FLOAT in the select tail: everything that is not an
     identifier, a timestamp or a status flag. --#}
{% macro aemo_cast_columns(record) %}
  {%- set spec = aemo_spec(record) -%}
  {{ return(spec['columns'] | map(attribute=0) | reject('in', spec['not_double']) | list) }}
{% endmacro %}

