-- One row per pull request (all PRs fetched, including ones created before the window;
-- fct_pull_requests applies the window filter).
select
    repo,
    id                                       as pr_number,
    repo || '#' || id                        as pr_key,       -- PR numbers repeat across repos
    payload->>'$.user.login'                 as author_login,
    {{ is_bot('user') }}                     as is_bot,
    payload->>'$.state'                      as state,
    (payload->>'$.created_at')::timestamptz  as created_at,
    (payload->>'$.updated_at')::timestamptz  as updated_at,
    (payload->>'$.closed_at')::timestamptz   as closed_at,
    (payload->>'$.merged_at')::timestamptz   as merged_at,
    fetched_at::timestamptz                  as fetched_at
from {{ source('raw', 'raw_pull_requests') }}
qualify row_number() over (partition by repo, id order by fetched_at desc) = 1
