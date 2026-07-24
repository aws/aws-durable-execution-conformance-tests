#!/usr/bin/env bash

set -euo pipefail

if [[ "$#" -ne 3 ]]; then
  echo "usage: $0 <claude|codex> <expected-head-sha> <summary-file>" >&2
  exit 2
fi

reviewer="$1"
expected_head_sha="$2"
summary_file="$3"

case "$reviewer" in
  claude)
    marker="<!-- ai-pr-review:claude -->"
    title="Claude AI review"
    ;;
  codex)
    marker="<!-- ai-pr-review:codex -->"
    title="Codex AI review"
    ;;
  *)
    echo "unsupported AI reviewer: $reviewer" >&2
    exit 2
    ;;
esac

: "${GH_TOKEN:?GH_TOKEN must be set}"
: "${GITHUB_REPOSITORY:?GITHUB_REPOSITORY must be set}"
: "${GITHUB_RUN_ID:?GITHUB_RUN_ID must be set}"
: "${GITHUB_SERVER_URL:?GITHUB_SERVER_URL must be set}"
: "${PR_NUMBER:?PR_NUMBER must be set}"

if [[ ! -r "$summary_file" ]]; then
  echo "AI review summary is not readable: $summary_file" >&2
  exit 2
fi

summary="$(cat "$summary_file")"
if [[ -z "${summary//[[:space:]]/}" ]]; then
  echo "::error::$title returned an empty review body."
  exit 1
fi

current_head_sha="$(
  gh api "repos/${GITHUB_REPOSITORY}/pulls/${PR_NUMBER}" --jq .head.sha
)"
if [[ "$current_head_sha" != "$expected_head_sha" ]]; then
  echo "::error::The PR changed while it was being reviewed."
  exit 1
fi

run_url="${GITHUB_SERVER_URL}/${GITHUB_REPOSITORY}/actions/runs/${GITHUB_RUN_ID}"
# shellcheck disable=SC2016 # Markdown backticks are intentionally literal.
printf -v body '%s\n## %s\n\n%s\n\nReviewed commit `%s`. [Workflow run](%s)' \
  "$marker" "$title" "$summary" "$expected_head_sha" "$run_url"

new_comment_id="$(
  gh api \
    --method POST \
    "repos/${GITHUB_REPOSITORY}/issues/${PR_NUMBER}/comments" \
    --raw-field body="$body" \
    --jq .node_id
)"
if [[ -z "$new_comment_id" ]]; then
  echo "::error::GitHub did not return the new AI review comment ID."
  exit 1
fi

owner="${GITHUB_REPOSITORY%%/*}"
repository="${GITHUB_REPOSITORY#*/}"
comments_file="$(mktemp "${RUNNER_TEMP:-/tmp}/ai-review-comments.XXXXXX")"
trap 'rm -f "$comments_file"' EXIT

gh api graphql \
  --paginate \
  -F owner="$owner" \
  -F repository="$repository" \
  -F number="$PR_NUMBER" \
  -f query='
    query(
      $owner: String!,
      $repository: String!,
      $number: Int!,
      $endCursor: String
    ) {
      repository(owner: $owner, name: $repository) {
        pullRequest(number: $number) {
          comments(first: 100, after: $endCursor) {
            nodes {
              id
              body
              isMinimized
              author {
                login
              }
            }
            pageInfo {
              hasNextPage
              endCursor
            }
          }
        }
      }
    }
  ' > "$comments_file"

previous_comment_count=0
while IFS= read -r comment_id; do
  [[ -n "$comment_id" ]] || continue

  gh api graphql \
    -F id="$comment_id" \
    -f query='
      mutation($id: ID!) {
        minimizeComment(
          input: {
            subjectId: $id,
            classifier: OUTDATED
          }
        ) {
          minimizedComment {
            isMinimized
          }
        }
      }
    ' > /dev/null
  previous_comment_count=$((previous_comment_count + 1))
done < <(
  jq -rs \
    --arg current_id "$new_comment_id" \
    --arg marker "$marker" \
    --arg legacy_header "## $title" \
    '
      .[]
      | .data.repository.pullRequest.comments.nodes[]
      | select(.id != $current_id)
      | select(.isMinimized == false)
      | select(.author.login == "github-actions")
      | select(
          (.body | startswith($marker))
          or (.body | startswith($legacy_header))
        )
      | .id
    ' \
    "$comments_file"
)

echo "Minimized $previous_comment_count previous $title comment(s)."
