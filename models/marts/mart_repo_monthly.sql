-- One row per repo x month, January 2024 through the run month.
-- The month spine guarantees a row (with zeros) even for months with no activity.

with months as (
    select unnest(generate_series(
        {{ window_start() }},
        date_trunc('month', {{ run_time() }}),
        interval 1 month
    ))::date as month
),

repos as (select distinct repo from {{ ref('stg_pull_requests') }}),

spine as (select repo, month from repos cross join months),

prs as (select * from {{ ref('fct_pull_requests') }}),

opened as (
    select repo, date_trunc('month', created_at)::date as month,
           count(*)                       as prs_opened,
           median(hours_to_first_review)  as median_first_review_hours,
           count(distinct author_login) filter (where not author_is_bot) as active_contributors
    from prs group by all
),

merged as (
    select repo, date_trunc('month', merged_at)::date as month,
           count(*)               as prs_merged,
           median(hours_to_merge) as median_merge_hours
    from prs where is_merged group by all
),

closed_unmerged as (
    select repo, date_trunc('month', closed_at)::date as month,
           count(*) as prs_closed_unmerged
    from prs where is_closed_unmerged group by all
),

commits as (
    select repo, date_trunc('month', committed_at)::date as month, count(*) as commits
    from {{ ref('stg_commits') }}
    where committed_at >= {{ window_start() }}
    group by all
),

releases as (
    select repo, date_trunc('month', published_at)::date as month, count(*) as releases
    from {{ ref('fct_releases') }} group by all
),

-- Open backlog at the end of each month: created by then, not yet closed by then.
backlog as (
    select s.repo, s.month, count(p.pr_key) as open_prs_end_of_month
    from spine s
    left join prs p
      on p.repo = s.repo
     and p.created_at < s.month + interval 1 month
     and (p.closed_at is null or p.closed_at >= s.month + interval 1 month)
    group by all
)

select
    s.repo,
    s.month,
    coalesce(o.prs_opened, 0)            as prs_opened,
    coalesce(m.prs_merged, 0)            as prs_merged,
    coalesce(c.prs_closed_unmerged, 0)   as prs_closed_unmerged,
    coalesce(m.prs_merged, 0)::double
        / nullif(coalesce(m.prs_merged, 0) + coalesce(c.prs_closed_unmerged, 0), 0)
                                         as merge_ratio,
    o.median_first_review_hours,
    m.median_merge_hours,
    coalesce(k.commits, 0)               as commits,
    coalesce(o.active_contributors, 0)   as active_contributors,
    coalesce(r.releases, 0)              as releases,
    b.open_prs_end_of_month,
    s.month = date_trunc('month', {{ run_time() }})::date as is_partial_month
from spine s
left join opened o          using (repo, month)
left join merged m          using (repo, month)
left join closed_unmerged c using (repo, month)
left join commits k         using (repo, month)
left join releases r        using (repo, month)
left join backlog b         using (repo, month)
