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

pg_connection_psql_cmd() {
    local db="$1"
    shift

    if [[ -n "${PG_EXPORTER_DSN:-}" ]]; then
        PGOPTIONS='--client-min-messages=warning' psql --no-psqlrc -d "$(pg_connection_target_for_db "${db}")" "$@"
    else
        PGOPTIONS='--client-min-messages=warning' psql \
            --no-psqlrc \
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
