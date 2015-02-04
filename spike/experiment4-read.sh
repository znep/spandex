#!/usr/bin/env bash
### query elasticsearch mock data for autocomplete experimentation

pushd "$( dirname "${BASH_SOURCE[0]}" )"

NODE0="eel:9200"
upfour="qnmj-8ku6"

function suggest() {
  if [ -z "$1" ]; then
    return 0
  else
    doc=$(printf '{"suggest": {"text":"%s", "completion": {"field": "154480039"}}}' $1)
    curl -XPOST "$NODE0/$upfour/_suggest" -d "$doc" 2>/dev/null |jsawk 'return this.suggest[0].options'
  fi
}



suggest 'c'
suggest 'cr'
suggest 'cri'
suggest 'crim'
suggest 'crime'

