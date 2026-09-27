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

/// Compute a bounded set of occurrences strictly after the supplied cursor.
pub fn evaluate(request: EvaluationRequest) -> anyhow::Result<Evaluation> {
    if !(1..=MAX_PREVIEW).contains(&request.count) {
        bail!("count must be 1..={MAX_PREVIEW}");
    }
    match request.trigger {
        Trigger::Once { at } => Ok(Evaluation {
            dates: if at > request.after { vec![at] } else { vec![] },
            exhausted: true,
        }),
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
            if !date.contains('T') || (!date.ends_with('Z') && !has_timezone) {
                bail!("Floating/all-day DTSTART is not supported; specify UTC Z or TZID");
            }
            let rules = RRuleSet::from_str(&value).context("Invalid RRULE set")?;
            if rules.get_rrule().len() > 4 {
                bail!("At most four RRULE lines are supported");
            }
            if rules.get_rrule().is_empty() && rules.get_rdate().is_empty() {
                bail!("RRULE set needs at least one RRULE or RDATE");
            }
            let start = request
                .after
                .checked_add_signed(Duration::nanoseconds(1))
                .context("Timestamp overflow")?
                .with_timezone(&rrule::Tz::UTC);
            let result = rules.after(start).all(request.count as u16);
            if result.limited && result.dates.len() < request.count {
                bail!("RRULE hit its iteration budget; simplify the rule or shorten its history");
            }
            let dates: Vec<_> = result
                .dates
                .into_iter()
                .map(|date| date.with_timezone(&Utc))
                .collect();
            if dates.first().is_some_and(|date| *date <= request.after)
                || dates.windows(2).any(|pair| pair[0] >= pair[1])
            {
                bail!("RRULE returned non-increasing occurrences");
            }
            Ok(Evaluation {
                dates,
                exhausted: !result.limited,
            })
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
