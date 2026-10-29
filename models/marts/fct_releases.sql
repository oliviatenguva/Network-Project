-- One row per published release on or after 2024-01-01.
-- The lag is computed over ALL releases first, so the first release in the
-- window still measures its gap to the release before it.

with all_releases as (
    select
        repo,
        release_id,
        tag_name,
        published_at,
        prerelease as is_prerelease,
        date_diff('day',
                  lag(published_at) over (partition by repo order by published_at),
                  published_at) as days_since_previous
    from {{ ref('stg_releases') }}
    where published_at is not null
)

select *
from all_releases
where published_at >= {{ window_start() }}
