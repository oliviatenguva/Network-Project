-- One row per repo. Every column is a rate, share, median, or slope - never a raw
-- count - so a big project cannot win on volume alone. Task 5 scores these columns.

with run as (
    select {{ run_time() }} as run_time,
           date_diff('day', {{ window_start() }}, {{ run_time() }}) as window_days
),

prs as (select * from {{ ref('fct_pull_requests') }}),

pr_metrics as (
    select
        repo,
        median(hours_to_first_review)                as median_hours_to_first_review,
        quantile_cont(hours_to_first_review, 0.9)    as p90_hours_to_first_review,
        -- only PRs old enough to have had a full 7 days
        avg(reviewed_within_7d::int) filter (
            where created_at <= (select run_time from run) - interval 7 day)
                                                     as pct_reviewed_within_7d,
        avg(has_approving_review::int) filter (where is_merged)
                                                     as pct_merged_with_approval,
        count(*) filter (where is_merged)::double
            / nullif(count(*) filter (where is_merged or is_closed_unmerged), 0)
                                                     as merge_rate,
        median(hours_to_merge)                       as median_hours_to_merge,
        quantile_cont(hours_to_merge, 0.9)           as p90_hours_to_merge,
        avg(is_stale::int) filter (where is_open)    as pct_open_prs_stale
    from prs
    group by repo
),

-- Concentration of work: share of human-authored merged PRs written by the top 5 authors.
author_merges as (
    select repo, author_login, count(*) as n,
           row_number() over (partition by repo order by count(*) desc) as author_rank
    from prs
    where is_merged and not author_is_bot
    group by repo, author_login
),

concentration as (
    select repo, sum(n) filter (where author_rank <= 5)::double / sum(n) as pct_merges_by_top5_authors
    from author_merges
    group by repo
),

commit_rate as (
    select c.repo, count(*) / (any_value(r.window_days) / 7.0) as commits_per_week
    from {{ ref('stg_commits') }} c, run r
    where c.committed_at >= {{ window_start() }}
    group by c.repo
),

release_metrics as (
    select
        f.repo,
        count(*) / (any_value(r.window_days) / 365.25)                           as releases_per_year,
        stddev_samp(days_since_previous) / nullif(avg(days_since_previous), 0)   as release_interval_cv
    from {{ ref('fct_releases') }} f, run r
    group by f.repo
),

-- Trends: monthly slope over COMPLETE months, divided by the repo's average month.
-- A value of 0.01 means "growing by about 1% of a typical month, every month",
-- which compares fairly across projects of very different sizes.
complete_months as (
    select *, row_number() over (partition by repo order by month) as month_index
    from {{ ref('mart_repo_monthly') }}
    where not is_partial_month
),

trends as (
    select
        repo,
        regr_slope(commits, month_index) / nullif(avg(commits), 0)                         as commit_trend,
        regr_slope(active_contributors, month_index) / nullif(avg(active_contributors), 0) as contributor_trend
    from complete_months
    group by repo
)

select
    p.repo,
    -- Activity
    k.commits_per_week,
    t.commit_trend,
    -- Responsiveness
    p.median_hours_to_first_review,
    p.p90_hours_to_first_review,
    p.pct_reviewed_within_7d,
    p.pct_merged_with_approval,
    -- Throughput
    p.merge_rate,
    p.median_hours_to_merge,
    p.p90_hours_to_merge,
    -- Backlog
    p.pct_open_prs_stale,
    -- Release cadence
    rm.releases_per_year,
    rm.release_interval_cv,
    -- Contributor base
    c.pct_merges_by_top5_authors,
    t.contributor_trend
from pr_metrics p
left join commit_rate k      using (repo)
left join release_metrics rm using (repo)
left join concentration c    using (repo)
left join trends t           using (repo)
