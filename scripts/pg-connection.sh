#!/usr/bin/env bash

pg_connection_target_for_db() {
    local db="$1"

    if [[ -n "${PG_EXPORTER_DSN:-}" ]]; then
        local dsn="${PG_EXPORTER_DSN}"
        local fragment=""
        local query=""

        if [[ "${dsn}" == *#* ]]; then
            fragment="#${dsn#*#}"
            dsn="${dsn%%#*}"
        fi

        if [[ "${dsn}" == *\?* ]]; then
            query="?${dsn#*\?}"
            dsn="${dsn%%\?*}"
        fi

        if [[ "${dsn}" == *://* ]]; then
            local scheme="${dsn%%://*}"
            local rest="${dsn#*://}"

            if [[ "${rest}" == */* ]]; then
                local authority="${rest%%/*}"
                printf '%s://%s/%s%s%s\n' "${scheme}" "${authority}" "${db}" "${query}" "${fragment}"
            else
                printf '%s/%s%s%s\n' "${dsn}" "${db}" "${query}" "${fragment}"
            fi
        else
            local replaced="${dsn}"

            if [[ "${replaced}" == *dbname=* ]]; then
                replaced="$(printf '%s\n' "${replaced}" | sed -E "s/(^|[[:space:]])dbname=([^[:space:]]*)/\1dbname=${db}/")"
            else
                replaced="${replaced} dbname=${db}"
            fi

            printf '%s\n' "${replaced}"
        fi

        return
    fi

    printf '%s\n' "${db}"
}

# Runs psql against `db`, non-interactively.
#
# `--pset=pager=off` is load-bearing: psql defaults to `pager 1`, so whenever result output
# is taller *or wider* than the terminal it pipes through $PAGER (less) and waits for the
# user to press `q`. That stalls any script that prints a result set — the setup script's
# "Top 5 queries" table is ~90 characters wide, so it hangs on any narrower terminal.
# `--no-psqlrc` does not cover this: the pager default is built into psql, not ~/.psqlrc.
# PAGER=cat is belt-and-braces for a psql built to consult it before the pset.
pg_connection_psql_cmd() {
    local db="$1"
    shift

    if [[ -n "${PG_EXPORTER_DSN:-}" ]]; then
        PGOPTIONS='--client-min-messages=warning' PAGER=cat psql \
            --no-psqlrc \
            --pset=pager=off \
            -d "$(pg_connection_target_for_db "${db}")" \
            "$@"
    else
        PGOPTIONS='--client-min-messages=warning' PAGER=cat psql \
            --no-psqlrc \
            --pset=pager=off \
            -h "${PG_HOST:-localhost}" \
            -p "${PG_PORT:-5432}" \
            -U "${PG_USER:-postgres}" \
            -d "${db}" \
            "$@"
    fi
}

pg_connection_pgbench_cmd() {
    local db="$1"
    shift

    if [[ -n "${PG_EXPORTER_DSN:-}" ]]; then
        pgbench "$@" "$(pg_connection_target_for_db "${db}")"
    else
        pgbench \
            -h "${PG_HOST:-localhost}" \
            -p "${PG_PORT:-5432}" \
            -U "${PG_USER:-postgres}" \
            "$@" \
            "${db}"
    fi
}

pg_connection_description() {
    local db="$1"

    if [[ -n "${PG_EXPORTER_DSN:-}" ]]; then
        local target
        target="$(pg_connection_target_for_db "${db}")"

        if [[ "${target}" == *://*@* ]]; then
            local scheme="${target%%://*}"
            local rest="${target#*://}"
            local userinfo="${rest%%@*}"
            local after_userinfo="${rest#*@}"

            if [[ "${userinfo}" == *:* ]]; then
                target="${scheme}://${userinfo%%:*}:****@${after_userinfo}"
            fi
        fi

        printf '%s\n' "${target}"
    else
        printf '%s@%s:%s/%s\n' \
            "${PG_USER:-postgres}" \
            "${PG_HOST:-localhost}" \
            "${PG_PORT:-5432}" \
            "${db}"
    fi
}
