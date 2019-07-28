#!/bin/bash

host=${BVT_HOST:?'BVT host should be specified by env var BVT_HOST!'}
username=${BVT_USERNAME:?'BVT user name should be specified by env var BVT_USERNAME!'}
password=${BVT_PASSWORD:?'BVT user password should be specified by env var BVT_PASSWORD!'}

apibase="https://$host/hpc"
req_cmd="request.cmd"
res_code="respose.code"
res_head="respose.head"
res_body="respose.body"
curl_err="curl.error"

function curl {
  cmd="curl -k -u \"$username:$password\" -o \"$res_body\" -D \"$res_head\" -w \"%{http_code}\" -sS $@ 2>\"$curl_err\""
  echo "$cmd" > "$req_cmd"
  eval "command $cmd"
}

function assert_ok {
  (( "$?" == 0 )) || exit 1
}

function assert_2xx {
  [[ "$1" =~ ^20[0-9]$ ]] || exit 1
}

function assert_regex {
  [[ "$1" =~ $2 ]] || exit 1
}

function assert_gte {
  (( "$1" >= "$2" )) || exit 1
}

function validate_json_string {
  read -r -d '' script <<'EOS'
import sys, json
a = json.load(sys.stdin)
if not isinstance(a, str):
  raise AssertionError
print(len(a))
EOS
  python3 -c "$script" <<<$1
  (( $? == 0 )) || exit 1
}

function validate_json_array {
  read -r -d '' script <<'EOS'
import sys, json
a = json.load(sys.stdin)
if not isinstance(a, list):
  raise AssertionError
print(len(a))
EOS
  python3 -c "$script" <<<$1
  (( $? == 0 )) || exit 1
}

res=$(curl "$apibase/cluster/version")
assert_ok
assert_2xx "$res"
body=$(< "$res_body")
assert_regex "$body" '^\"[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+\"$'


res=$(curl "$apibase/cluster/activeHeadNode")
assert_ok
assert_2xx "$res"
body=$(< "$res_body")
size=$(validate_json_string "$body")
assert_ok
assert_gte "$size" 1


res=$(curl "$apibase/cluster/info/dateTimeFormat")
assert_ok
assert_2xx "$res"
body=$(< "$res_body")
size=$(validate_json_string "$body")
assert_ok
assert_gte "$size" 1

res=$(curl "$apibase/nodes")
assert_ok
assert_2xx "$res"
body=$(< "$res_body")
size=$(validate_json_array "$body")
assert_ok
assert_gte "$size" 1

echo "OK"
