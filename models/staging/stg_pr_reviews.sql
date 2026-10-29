-- One row per submitted review. PENDING reviews have no submitted_at, so they are dropped.
select
    repo,
    id                                                                     as review_id,
    regexp_extract(payload->>'$.pull_request_url', '(\d+)$', 1)::bigint    as pr_number,
    repo || '#' || regexp_extract(payload->>'$.pull_request_url', '(\d+)$', 1) as pr_key,
    payload->>'$.user.login'                                               as reviewer_login,
    {{ is_bot('user') }}                                                   as is_bot,
    payload->>'$.state'                                                    as state,
    (payload->>'$.submitted_at')::timestamptz                              as submitted_at,
    fetched_at::timestamptz                                                as fetched_at
from {{ source('raw', 'raw_pr_reviews') }}
where payload->>'$.state' <> 'PENDING'
qualify row_number() over (partition by repo, id order by fetched_at desc) = 1
