EXTENSION = pg_fact_loader
DATA =  pg_fact_loader--1.4.sql pg_fact_loader--1.4--1.5.sql \
        pg_fact_loader--1.5.sql pg_fact_loader--1.5--1.6.sql \
        pg_fact_loader--1.6.sql pg_fact_loader--1.6--1.7.sql \
        pg_fact_loader--1.7.sql
MODULES = pg_fact_loader 

REGRESS := 01_create_ext 02_schema 03_audit \
        04_seeds 05_pgl_setup 06_basic_workers \
        07_launch_worker 08_fact_table_deps \
        09_purge 10_delete 11_more_data \
        12_no_proid 13_cutoff_no_dep_on_filter \
        14_null_key 15_source_change_date \
        16_1_2_features 17_1_3_features
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# Prevent unintentional inheritance of PGSERVICE while running regression suite
# with make installcheck.  We typically use PGSERVICE in our shell environment but
# not for dev. Require instead explicit PGPORT= or PGSERVICE= to do installcheck
unexport PGSERVICE
