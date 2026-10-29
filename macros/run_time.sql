{#
  "Run time" = the newest fetched_at in the raw tables, NOT now().
  Rebuilding without new data therefore gives exactly the same results.
#}
{% macro run_time() %}
(
    select max(fetched_at)::timestamptz from (
        select fetched_at from {{ source('raw', 'raw_commits') }}
        union all select fetched_at from {{ source('raw', 'raw_pull_requests') }}
        union all select fetched_at from {{ source('raw', 'raw_pr_reviews') }}
        union all select fetched_at from {{ source('raw', 'raw_releases') }}
    )
)
{% endmacro %}

{# Fixed start of the analysis window, shared by every project. #}
{% macro window_start() %}'2024-01-01 00:00:00+00'::timestamptz{% endmacro %}

{#
  Bot rule, applied to PR authors, reviewers, and commit authors:
  the account's type is 'Bot' OR its login ends in "[bot]".
  A missing user (e.g. commit email not linked to an account) is not a bot.
#}
{% macro is_bot(user_path) %}
coalesce(payload->>'$.{{ user_path }}.type' = 'Bot'
         or payload->>'$.{{ user_path }}.login' like '%[bot]', false)
{% endmacro %}
