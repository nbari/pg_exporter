#!/usr/bin/env bash

# Whether `$1` is a loopback host or address: `localhost`, a 127.0.0.0/8 literal, or ::1.
# A name that merely starts with "127." is a DNS name, not an address.
pg_connection_is_loopback() {
    local value="${1,,}"
    [[ "${value}" == localhost || "${value}" == ::1 ]] && return 0
    [[ "${value}" =~ ^127\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}$ ]]
}

# Prints where libpq would connect for the current settings, as two lines: the host and
# the hostaddr (either may be empty), or fails when the settings are in a form this
# cannot read with certainty. `PG_EXPORTER_DSN` wins when set, as it does for every
# command below; otherwise the host is `$1`. `PGHOSTADDR` is the fallback address in both
# cases, because libpq reads it whatever the host says.
#
# This is deliberately narrower than libpq. It refuses, instead of guessing at, a service
# file entry (`service=`, PGSERVICE), a list of hosts, quoting or backslashes in a
# keyword string, and any percent-encoding other than %2F (the slash of a socket path):
# each of those can name a host this function would not see.
pg_connection_effective_target() {
    local host="$1" hostaddr="${PGHOSTADDR:-}"
    local dsn="${PG_EXPORTER_DSN:-}"

    [[ -z "${PGSERVICE:-}" && -z "${PGSERVICEFILE:-}" ]] || return 1

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
                # An encoded key could spell `host` without looking like it.
                [[ "${key}" != *%* ]] || return 1
                case "${key,,}" in
                    host | hostaddr | service) [[ "${key}" == "${key,,}" ]] || return 1 ;;
                esac
                case "${key}" in
                    host) host="${value}" ;;
                    hostaddr) hostaddr="${value}" ;;
                    service) return 1 ;;
                esac
            done
        else
            local token keyword
            [[ "${dsn}" != *[\'\\]* ]] || return 1
            host=""
            for token in ${dsn}; do
                keyword="${token%%=*}"
                case "${keyword,,}" in
                    host | hostaddr | service) [[ "${keyword}" == "${keyword,,}" ]] || return 1 ;;
                esac
                case "${token}" in
                    host=*) host="${token#host=}" ;;
                    hostaddr=*) hostaddr="${token#hostaddr=}" ;;
                    service=*) return 1 ;;
                esac
            done
        fi
        # No host in the DSN: libpq falls back to PGHOST, then to the local socket.
        [[ -n "${host}" ]] || host="${PGHOST:-/}"
    fi

    host="${host//%2F//}"
    host="${host//%2f//}"
    [[ "${host}" != *%* && "${hostaddr}" != *%* ]] || return 1
    [[ "${host}" != *,* && "${hostaddr}" != *,* ]] || return 1

    printf '%s\n%s\n' "${host}" "${hostaddr}"
}

# Succeeds when the settings connect to this machine: a socket, a loopback address, or
# a loopback host with no address overriding it. With PG_EXPORTER_TEST_ALLOW_NONLOCAL=1
# the devcontainer's compose service, host `postgres` with no address override, is
# admitted too. Settings that cannot be read with certainty are not local. `$1` is the
# host used without a DSN.
pg_connection_is_local() {
    local target host hostaddr
    target="$(pg_connection_effective_target "$1")" || return 1
    {
        IFS= read -r host
        IFS= read -r hostaddr
    } <<<"${target}"

    if [[ -n "${hostaddr}" ]]; then
        pg_connection_is_loopback "${hostaddr}" && [[ "${hostaddr,,}" != localhost ]]
        return
    fi
    [[ "${host}" == /* ]] && return 0
    pg_connection_is_loopback "${host}" && return 0
    case "${PG_EXPORTER_TEST_ALLOW_NONLOCAL:-}" in
        1 | true | TRUE) [[ "${host}" == postgres ]] && return 0 ;;
    esac
    return 1
}

# Prints the host and, when there is one, the address a refusal should name. Never the
# DSN: it can carry a password in its user info, its query string or a keyword.
pg_connection_target_summary() {
    local target host hostaddr
    if ! target="$(pg_connection_effective_target "$1")"; then
        printf 'settings this script cannot read with certainty (a service, several hosts, quoting or percent-encoding)\n'
        return
    fi
    {
        IFS= read -r host
        IFS= read -r hostaddr
    } <<<"${target}"
    if [[ -n "${hostaddr}" ]]; then
        printf 'host %s, address %s\n' "${host:-<none>}" "${hostaddr}"
    else
        printf 'host %s\n' "${host:-<none>}"
    fi
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
            # libpq lets a `dbname` query parameter override the path, so one left in
            # place would keep the "derived" target in the original database.
            if [[ -n "${query}" ]]; then
                local kept="" pair
                local -a pairs=()
                IFS='&' read -ra pairs <<<"${query#\?}"
                for pair in "${pairs[@]}"; do
                    [[ "${pair%%=*}" == dbname || -z "${pair}" ]] && continue
                    kept+="${kept:+&}${pair}"
                done
                query="${kept:+?${kept}}"
            fi

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

        # A password can also sit in the query string or in a keyword.
        printf '%s\n' "${target}" | sed -E 's/(password=)[^&[:space:]]*/\1****/g'
    else
        printf '%s@%s:%s/%s\n' \
            "${PG_USER:-postgres}" \
            "${PG_HOST:-localhost}" \
            "${PG_PORT:-5432}" \
            "${db}"
    fi
}
