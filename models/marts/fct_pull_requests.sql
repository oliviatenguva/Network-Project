-- One row per PR created on or after 2024-01-01, with precalculated flags and durations.
-- PRs still open at run time are KEPT: is_open = true and every duration that needs
-- an end point (merge, close) is NULL rather than a made-up date.

with prs as (
    select * from {{ ref('stg_pull_requests') }}
    where created_at >= {{ window_start() }}
),

-- Reviews that count: not by a bot and not by the PR's own author.
qualifying_reviews as (
    select r.pr_key, r.state, r.submitted_at
    from {{ ref('stg_pr_reviews') }} r
    join prs p using (pr_key)
    where not r.is_bot
      and r.reviewer_login is distinct from p.author_login
),

review_summary as (
    select
        pr_key,
        min(submitted_at)           as first_review_at,
        bool_or(state = 'APPROVED') as has_approving_review
    from qualifying_reviews
    group by pr_key
),

flagged as (
    select
        p.repo,
        p.pr_number,
        p.pr_key,
        p.author_login,
        p.is_bot                                      as author_is_bot,
        p.state,
        p.created_at,
        p.updated_at,
        p.closed_at,
        p.merged_at,
        p.merged_at is not null                       as is_merged,
        p.state = 'closed' and p.merged_at is null    as is_closed_unmerged,
        p.state = 'open'                              as is_open,
        r.first_review_at,
        coalesce(r.has_approving_review, false)       as has_approving_review
    from prs p
    left join review_summary r using (pr_key)
)

select
    *,
    date_diff('second', created_at, first_review_at) / 3600.0      as hours_to_first_review,
    coalesce(date_diff('second', created_at, first_review_at) / 3600.0 <= 168, false)
                                                                   as reviewed_within_7d,
    case when is_merged then date_diff('second', created_at, merged_at) / 3600.0 end
                                                                   as hours_to_merge,
    case when not is_open then date_diff('second', created_at, closed_at) / 3600.0 end
                                                                   as hours_to_close,
    is_open and updated_at < {{ run_time() }} - interval 180 day   as is_stale
from flagged
