-- One row per commit. author.login is NULL when the commit email is not linked
-- to a GitHub account; is_bot is then false.
select
    repo,
    id                                                as sha,
    payload->>'$.author.login'                        as author_login,
    {{ is_bot('author') }}                            as is_bot,
    (payload->>'$.commit.author.date')::timestamptz   as committed_at,
    fetched_at::timestamptz                           as fetched_at
from {{ source('raw', 'raw_commits') }}
qualify row_number() over (partition by repo, id order by fetched_at desc) = 1
