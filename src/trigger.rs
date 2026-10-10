use std::str::FromStr;

use anyhow::{Context, bail};
use chrono::{DateTime, Duration, Utc};
use chrono_tz::Tz;
use croner::Cron;
use rrule::RRuleSet;
use serde::{Deserialize, Serialize};

use crate::domain::Trigger;

pub const MAX_PREVIEW: usize = 256;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EvaluationRequest {
    pub trigger: Trigger,
    pub after: DateTime<Utc>,
    pub count: usize,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Evaluation {
    pub dates: Vec<DateTime<Utc>>,
    pub exhausted: bool,
}

pub fn validate_once(at: DateTime<Utc>) -> anyhow::Result<()> {
    if at.timestamp_subsec_nanos() >= 1_000_000_000 {
        bail!("Leap-second timestamps are not supported");
    }
    if !at.timestamp_subsec_nanos().is_multiple_of(1_000) {
        bail!(
            "Timestamp must be representable at microsecond precision (at most six fractional digits)"
        );
    }
    Ok(())
}

/// Runs in a disposable child process: arbitrary calendar rules get a hard wall-clock budget.
pub fn evaluate(request: EvaluationRequest) -> anyhow::Result<Evaluation> {
    if !(1..=MAX_PREVIEW).contains(&request.count) {
        bail!("count must be 1..={MAX_PREVIEW}");
    }
    match request.trigger {
        Trigger::Once { at } => {
            validate_once(at)?;
            Ok(Evaluation {
                dates: if at > request.after { vec![at] } else { vec![] },
                exhausted: true,
            })
        }
        Trigger::Delay { .. } => bail!("Delay must be resolved to an absolute timestamp first"),
        Trigger::Cron {
            expression,
            timezone,
        } => {
            if expression.len() > 512 || !matches!(expression.split_whitespace().count(), 5 | 6) {
                bail!("Cron requires five fields, or six fields with seconds first");
            }
            let tz = Tz::from_str(&timezone).context("Invalid IANA timezone")?;
            let cron = Cron::from_str(&expression).context("Invalid Cron expression")?;
            let mut dates = Vec::with_capacity(request.count);
            let mut after = request.after.with_timezone(&tz);
            for _ in 0..request.count {
                let next = cron
                    .find_next_occurrence(&after, false)
                    .context("Cron has no reachable next occurrence")?;
                if next <= after {
                    bail!("Cron did not advance");
                }
                dates.push(next.with_timezone(&Utc));
                after = next;
            }
            Ok(Evaluation {
                dates,
                exhausted: false,
            })
        }
        Trigger::Rrule { value } => {
            if value.len() > 16_384 {
                bail!("RRULE input exceeds 16 KiB");
            }
            for (property, dates) in value.lines().filter_map(|line| line.trim().split_once(':')) {
                let name = property.split(';').next().unwrap_or_default();
                if name == "EXRULE" {
                    bail!("EXRULE is not supported; use explicit EXDATE values");
                }
                if matches!(name, "DTSTART" | "RDATE" | "EXDATE")
                    && (property.split(';').any(|p| p == "VALUE=DATE")
                        || dates.split(',').any(|date| !date.contains('T')))
                {
                    bail!("All-day {name} is not supported; specify DATE-TIME values");
                }
            }
            let starts: Vec<_> = value
                .lines()
                .filter_map(|line| line.trim().split_once(':'))
                .filter(|(property, _)| property.split(';').next() == Some("DTSTART"))
                .collect();
            if starts.len() != 1 {
                bail!("RRULE requires exactly one DTSTART with UTC Z or an explicit TZID");
            }
            let (property, date) = starts[0];
            let has_timezone = property
                .split(';')
                .any(|parameter| parameter.starts_with("TZID="));
            if !date.ends_with('Z') && !has_timezone {
                bail!("Floating DTSTART is not supported; specify UTC Z or TZID");
            }
            let rules = RRuleSet::from_str(&value).context("Invalid RRULE set")?;
            if rules.get_rrule().len() > 4 {
                bail!("At most four RRULE lines are supported");
            }
            if rules.get_rrule().is_empty() && rules.get_rdate().is_empty() {
                bail!("RRULE set needs at least one RRULE or RDATE");
            }
            let mut dates = Vec::with_capacity(request.count);
            let mut cursor = request.after;
            loop {
                let start = cursor
                    .checked_add_signed(Duration::nanoseconds(1))
                    .context("Timestamp overflow")?
                    .with_timezone(&rrule::Tz::UTC);
                let remaining = request.count - dates.len();
                let result = rules.clone().after(start).all(remaining as u16);
                if result.limited && result.dates.len() < remaining {
                    bail!(
                        "RRULE hit its iteration budget; simplify the rule or shorten its history"
                    );
                }
                // Upstream merges inclusion sources but preserves duplicate
                // instants. Only unique occurrences count towards our limit.
                for date in result.dates {
                    let date = date.with_timezone(&Utc);
                    if date <= cursor || dates.last().is_some_and(|last| *last > date) {
                        bail!("RRULE returned non-increasing occurrences");
                    }
                    if dates.last() != Some(&date) {
                        dates.push(date);
                    }
                }
                if !result.limited || dates.len() == request.count {
                    return Ok(Evaluation {
                        dates,
                        exhausted: !result.limited,
                    });
                }
                // Refill past the last emitted instant without moving DTSTART,
                // preserving COUNT/INTERVAL anchors and upstream iteration limits.
                // Every pass contributes >= 1 unique date, so passes are bounded
                // by count (<= 256); the outer process wall-clock budget still applies.
                cursor = *dates.last().context("RRULE did not advance")?;
            }
        }
    }
}

pub fn resolve_delay(trigger: &mut Trigger, now: DateTime<Utc>) -> anyhow::Result<()> {
    if let Trigger::Delay { seconds } = trigger {
        let at = now
            .checked_add_signed(Duration::seconds(i64::from(*seconds)))
            .context("Delay timestamp overflow")?;
        *trigger = Trigger::Once { at };
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dt(s: &str) -> DateTime<Utc> {
        s.parse().unwrap()
    }

    #[test]
    fn cron_five_fields_and_timezone() {
        let result = evaluate(EvaluationRequest {
            trigger: Trigger::Cron {
                expression: "0 9 * * MON-FRI".into(),
                timezone: "Asia/Shanghai".into(),
            },
            after: dt("2026-09-21T01:00:00Z"),
            count: 2,
        })
        .unwrap();
        assert_eq!(
            result.dates,
            [dt("2026-09-22T01:00:00Z"), dt("2026-09-23T01:00:00Z")]
        );
    }

    #[test]
    fn cron_seconds_are_strictly_after_cursor() {
        let result = evaluate(EvaluationRequest {
            trigger: Trigger::Cron {
                expression: "*/5 * * * * *".into(),
                timezone: "UTC".into(),
            },
            after: dt("2026-09-21T00:00:05Z"),
            count: 2,
        })
        .unwrap();
        assert_eq!(
            result.dates,
            [dt("2026-09-21T00:00:10Z"), dt("2026-09-21T00:00:15Z")]
        );
    }

    #[test]
    fn rrule_count_exdate_and_rdate_preserve_original_anchor() {
        let result = evaluate(EvaluationRequest {
            trigger: Trigger::Rrule { value: "DTSTART;TZID=Asia/Shanghai:20260921T090000\nRRULE:FREQ=DAILY;COUNT=3\nEXDATE;TZID=Asia/Shanghai:20260922T090000\nRDATE;TZID=Asia/Shanghai:20260925T090000".into() },
            after: dt("2026-09-21T01:00:00Z"), count: 10,
        }).unwrap();
        assert_eq!(
            result.dates,
            [dt("2026-09-23T01:00:00Z"), dt("2026-09-25T01:00:00Z")]
        );
        assert!(result.exhausted);
    }

    #[test]
    fn rrule_last_weekday_and_dst() {
        let result = evaluate(EvaluationRequest {
            trigger: Trigger::Rrule { value: "DTSTART;TZID=America/New_York:20261030T090000\nRRULE:FREQ=MONTHLY;BYDAY=MO,TU,WE,TH,FR;BYSETPOS=-1;COUNT=2".into() },
            after: dt("2026-10-01T00:00:00Z"), count: 10,
        }).unwrap();
        assert_eq!(
            result.dates,
            [dt("2026-10-30T13:00:00Z"), dt("2026-11-30T14:00:00Z")]
        );
    }

    #[test]
    fn floating_rrule_and_unbounded_preview_are_rejected() {
        let trigger = Trigger::Rrule {
            value: "DTSTART:20260921T090000\nRRULE:FREQ=DAILY".into(),
        };
        assert!(
            evaluate(EvaluationRequest {
                trigger: trigger.clone(),
                after: Utc::now(),
                count: 1
            })
            .is_err()
        );
        assert!(
            evaluate(EvaluationRequest {
                trigger,
                after: Utc::now(),
                count: 1000
            })
            .is_err()
        );
    }

    #[test]
    fn all_day_and_duplicate_dtstart_are_rejected() {
        for value in [
            "DTSTART;TZID=Asia/Shanghai:20260921\nRRULE:FREQ=DAILY",
            "DTSTART:20260921T090000Z\nDTSTART:20260922T090000Z\nRRULE:FREQ=DAILY",
        ] {
            assert!(
                evaluate(EvaluationRequest {
                    trigger: Trigger::Rrule {
                        value: value.into()
                    },
                    after: dt("2026-09-20T00:00:00Z"),
                    count: 1,
                })
                .is_err()
            );
        }
    }

    #[test]
    fn rrule_upstream_dst_gap_behavior_is_explicit() {
        // rrule 0.14 shifts nonexistent 02:30 to 03:30. This compatibility
        // behavior differs from strict RFC 5545 recurrence-gap skipping.
        let result = evaluate(EvaluationRequest {
            trigger: Trigger::Rrule {
                value: "DTSTART;TZID=America/New_York:20260307T023000\nRRULE:FREQ=DAILY;COUNT=3"
                    .into(),
            },
            after: dt("2026-03-07T00:00:00Z"),
            count: 10,
        })
        .unwrap();
        assert_eq!(
            result.dates,
            [
                dt("2026-03-07T07:30:00Z"),
                dt("2026-03-08T07:30:00Z"),
                dt("2026-03-09T06:30:00Z")
            ]
        );
    }

    #[test]
    fn recurrence_union_deduplicates_without_shortening_preview() {
        for value in [
            "DTSTART:20260921T090000Z\nRRULE:FREQ=DAILY;COUNT=4\nRDATE:20260921T090000Z,20260921T090000Z,20260922T090000Z",
            "DTSTART:20260921T090000Z\nRRULE:FREQ=DAILY;COUNT=4\nRRULE:FREQ=DAILY;COUNT=4",
        ] {
            let trigger = Trigger::Rrule {
                value: value.into(),
            };
            let preview = evaluate(EvaluationRequest {
                trigger: trigger.clone(),
                after: dt("2026-09-20T00:00:00Z"),
                count: 3,
            })
            .unwrap();
            assert_eq!(
                preview.dates,
                [
                    dt("2026-09-21T09:00:00Z"),
                    dt("2026-09-22T09:00:00Z"),
                    dt("2026-09-23T09:00:00Z")
                ]
            );
            assert!(!preview.exhausted);
            let remainder = evaluate(EvaluationRequest {
                trigger,
                after: *preview.dates.last().unwrap(),
                count: 3,
            })
            .unwrap();
            assert_eq!(remainder.dates, [dt("2026-09-24T09:00:00Z")]);
            assert!(remainder.exhausted);
        }
    }

    #[test]
    fn unsupported_exrules_and_all_day_dates_are_rejected() {
        for property in [
            "EXRULE:FREQ=DAILY;COUNT=2",
            "RDATE;VALUE=DATE:20260922",
            "RDATE:20260922",
            "EXDATE;VALUE=DATE:20260922",
            "EXDATE;TZID=Asia/Shanghai:20260922",
        ] {
            let value = format!("DTSTART:20260921T090000Z\nRRULE:FREQ=DAILY;COUNT=3\n{property}");
            assert!(
                evaluate(EvaluationRequest {
                    trigger: Trigger::Rrule { value },
                    after: dt("2026-09-20T00:00:00Z"),
                    count: 5
                })
                .is_err(),
                "{property}"
            );
        }
    }

    #[test]
    fn once_and_delay_use_strict_cursor_and_checked_arithmetic() {
        let now = dt("2026-09-21T00:00:00Z");
        let mut trigger = Trigger::Delay { seconds: 5 };
        resolve_delay(&mut trigger, now).unwrap();
        let result = evaluate(EvaluationRequest {
            trigger: trigger.clone(),
            after: now,
            count: 10,
        })
        .unwrap();
        assert_eq!(result.dates, [now + Duration::seconds(5)]);
        assert!(result.exhausted);
        assert!(
            evaluate(EvaluationRequest {
                trigger,
                after: now + Duration::seconds(5),
                count: 1
            })
            .unwrap()
            .dates
            .is_empty()
        );
        assert!(
            resolve_delay(&mut Trigger::Delay { seconds: 1 }, DateTime::<Utc>::MAX_UTC).is_err()
        );
    }
}
