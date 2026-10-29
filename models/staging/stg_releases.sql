-- One row per published release. Drafts are dropped (unpublished, no published_at).
select
    repo,
    id                                          as release_id,
    payload->>'$.tag_name'                      as tag_name,
    (payload->>'$.published_at')::timestamptz   as published_at,
    (payload->>'$.prerelease')::boolean         as prerelease,
    fetched_at::timestamptz                     as fetched_at
from {{ source('raw', 'raw_releases') }}
where not coalesce((payload->>'$.draft')::boolean, false)
qualify row_number() over (partition by repo, id order by fetched_at desc) = 1
