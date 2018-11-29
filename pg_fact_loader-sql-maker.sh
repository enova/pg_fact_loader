#!/usr/bin/env bash

set -eu

last_version=1.4
new_version=1.5
last_version_file=pg_fact_loader--${last_version}.sql
new_version_file=pg_fact_loader--${new_version}.sql
update_file=pg_fact_loader--${last_version}--${new_version}.sql

rm -f $update_file
rm -f $new_version_file

create_update_file_with_header() {
cat << EOM > $update_file
/* $update_file */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_fact_loader" to load this file. \quit

EOM
}

add_sql_to_file() {
sql=$1
file=$2
echo "$sql" >> $file
}

add_file() {
s=$1
d=$2
(cat "${s}"; echo; echo) >> "$d"
}

create_update_file_with_header

# Drop dependencies to be re-added with changed def

# Now add new view and function defs

# Add comment files back for dropped and recreated views

# Only copy diff and new files after last version, and add the update script
touch $update_file
add_file functions/sql_builder.sql $update_file
cp $last_version_file $new_version_file
cat $update_file >> $new_version_file
