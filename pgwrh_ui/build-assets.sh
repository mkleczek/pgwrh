#!/bin/sh
# Embed local assets as SQL functions; never fetch assets during the build.
set -eu
cd "$(dirname "$0")"
htmx=vendor/htmx-2.0.10.min.js
expected=1f94ab71fca01e602e4c366984c1ea0492dcdc586cb0a8c6ef0fc2782a4545e49fc015834caa64ccf3fc73e70bb0af95
if command -v sha384sum >/dev/null 2>&1; then
    digest=$(sha384sum < "$htmx")
elif command -v shasum >/dev/null 2>&1; then
    digest=$(shasum -a 384 < "$htmx")
else
    echo 'Install sha384sum (coreutils) or shasum to verify the bundled htmx asset.' >&2
    exit 1
fi
if [ "${digest%% *}" != "$expected" ]; then
    echo 'Bundled htmx differs from the pinned upstream release.' >&2
    exit 1
fi

emit_asset() {
    name=$1 asset=$2 media=$3
    if LC_ALL=C grep -Fq '$pgwrh_ui_asset$' "$asset"; then
        echo "Reserved SQL delimiter in $asset" >&2
        exit 1
    fi
    printf 'CREATE FUNCTION %s() RETURNS "*/*"\n' "$name"
    printf 'LANGUAGE sql STABLE SET search_path = pg_catalog AS $function$\n'
    printf "    SELECT set_config('response.headers', '[{\"Content-Type\":\"%s; charset=utf-8\"},{\"Cache-Control\":\"no-cache\"},{\"X-Content-Type-Options\":\"nosniff\"}]', true);\n" "$media"
    printf '    SELECT convert_to($pgwrh_ui_asset$'
    cat "$asset"
    printf '$pgwrh_ui_asset$, '\''UTF8'\'')::pgwrh_ui."*/*";\n$function$;\n'
}

emit_asset style assets/app.css text/css
emit_asset script assets/app.js application/javascript
emit_asset htmx "$htmx" application/javascript
