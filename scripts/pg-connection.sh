#!/usr/bin/env bash

# Prints where libpq would really connect for the current settings, as two lines: the
# host and the hostaddr (either may be empty). `PG_EXPORTER_DSN` wins when set, as it
# does for every command below; otherwise the host is `$1`. `PGHOSTADDR` is the
# fallback address in both cases, because libpq reads it whatever the host says.
pg_connection_effective_target() {
    local host="$1" hostaddr="${PGHOSTADDR:-}"
    local dsn="${PG_EXPORTER_DSN:-}"

    if [[ -n "${dsn}" ]]; then
        dsn="${dsn%%#*}"
        if [[ "${dsn}" == *://* ]]; then
            local rest="${dsn#*://}" query="" pair key value
            local authority="${rest%%[/?]*}"
            local hostport="${authority##*@}"
            if [[ "${hostport}" == \[* ]]; then
                host="${hostport#\[}"
                host="${host%%\]*}"
            else
                host="${hostport%%:*}"
            fi
            [[ "${dsn}" == *\?* ]] && query="${dsn#*\?}"
            local -a pairs=()
            IFS='&' read -ra pairs <<<"${query}"
            for pair in "${pairs[@]}"; do
                key="${pair%%=*}"
                value="${pair#*=}"
                value="${value//%2F//}"
                value="${value//%2f//}"
                case "${key}" in
                    host) host="${value}" ;;
                    hostaddr) hostaddr="${value}" ;;
                esac
            done
        else
            local token
            host=""
            for token in ${dsn}; do
                case "${token}" in
                    host=*) host="${token#host=}" ;;
                    hostaddr=*) hostaddr="${token#hostaddr=}" ;;
                esac
            done
        fi
        # No host in the DSN: libpq falls back to PGHOST, then to the local socket.
        [[ -n "${host}" ]] || host="${PGHOST:-/}"
    fi

    printf '%s\n%s\n' "${host}" "${hostaddr}"
}

# Succeeds when the settings connect to this machine: a socket, a loopback address, or
# a loopback host name with no address overriding it. With
# PG_EXPORTER_TEST_ALLOW_NONLOCAL=1 the devcontainer's compose service, host `postgres`
# with no address override, is admitted too. `$1` is the host used without a DSN.
pg_connection_is_local() {
    local host hostaddr
    {
        IFS= read -r host
        IFS= read -r hostaddr
    } < <(pg_connection_effective_target "$1")

    if [[ -n "${hostaddr}" ]]; then
        [[ "${hostaddr}" == 127.* || "${hostaddr}" == ::1 ]]
        return
    fi
    case "${host}" in
        /* | localhost | LOCALHOST | 127.* | ::1) return 0 ;;
    esac
    case "${PG_EXPORTER_TEST_ALLOW_NONLOCAL:-}" in
        1 | true | TRUE) [[ "${host}" == postgres ]] && return 0 ;;
    esac
    return 1
}

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
