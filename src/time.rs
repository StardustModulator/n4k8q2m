//! Timestamp forms F1, F2 and F3. Calendar checks reject impossible dates.

pub const SECOND: i64 = 1_000;
pub const DAY_MS: i64 = 86_400_000;
pub const WEEK_MS: i64 = 7 * DAY_MS;
pub const GAP_MS: i64 = 1_800_000;
pub const LATE_MS: i64 = DAY_MS;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Civil {
    pub year: i64,
    pub month: i64,
    pub day: i64,
}

#[inline]
fn leap(year: i64) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

#[inline]
fn days_in_month(year: i64, month: i64) -> i64 {
    const DAYS: [i64; 12] = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    if month == 2 && leap(year) {
        29
    } else {
        DAYS[(month as usize) - 1]
    }
}

const fn civil_days(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let year_of_era = year - era * 400;
    let day_of_year = (153 * ((month + 9) % 12) + 2) / 5 + day - 1;
    era * 146_097 + year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year - 719_468
}

// Jan 1 for 2005..=2044. The generator's windows sit inside this span.
const JAN1: [i64; 40] = {
    let mut out = [0i64; 40];
    let mut i = 0;
    while i < 40 {
        out[i] = civil_days(2005 + i as i64, 1, 1);
        i += 1;
    }
    out
};

const CUM_DAYS: [i64; 12] = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334];

/// Howard Hinnant `days_from_civil`. `None` when the calendar date is impossible.
#[inline]
pub fn days_from_civil(year: i64, month: i64, day: i64) -> Option<i64> {
    if !(1..=12).contains(&month) {
        return None;
    }
    let dim = days_in_month(year, month);
    if day < 1 || day > dim {
        return None;
    }
    Some(civil_days(year, month, day))
}

#[inline(always)]
fn fast_day(year: i64, month: i64, day: i64) -> Option<i64> {
    if !(1..=12).contains(&month) {
        return None;
    }
    let leap = leap(year);
    let dim = if month == 2 {
        28 + i64::from(leap)
    } else if matches!(month, 4 | 6 | 9 | 11) {
        30
    } else {
        31
    };
    if day < 1 || day > dim {
        return None;
    }
    if (2005..2045).contains(&year) {
        let mut off = CUM_DAYS[(month as usize) - 1] + day - 1;
        if month > 2 && leap {
            off += 1;
        }
        Some(JAN1[(year - 2005) as usize] + off)
    } else {
        Some(civil_days(year, month, day))
    }
}

pub fn civil_from_days(days: i64) -> Civil {
    let shifted = days + 719_468;
    let era = shifted.div_euclid(146_097);
    let day_of_era = shifted - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_index = (5 * day_of_year + 2) / 153;
    let month = if month_index < 10 {
        month_index + 3
    } else {
        month_index - 9
    };
    Civil {
        year: year_of_era + era * 400 + i64::from(month <= 2),
        month,
        day: day_of_year - (153 * month_index + 2) / 5 + 1,
    }
}

#[inline]
fn dig2(s: &[u8]) -> Option<i64> {
    let a = s[0].wrapping_sub(b'0');
    let b = s[1].wrapping_sub(b'0');
    if a > 9 || b > 9 {
        return None;
    }
    Some(a as i64 * 10 + b as i64)
}

#[inline]
fn dig4(s: &[u8]) -> Option<i64> {
    Some(dig2(&s[0..2])? * 100 + dig2(&s[2..4])?)
}

#[inline]
fn civil_ms(s: &[u8]) -> Option<i64> {
    if s[4] != b'-' || s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':' {
        return None;
    }
    let year = dig4(&s[0..4])?;
    let month = dig2(&s[5..7])?;
    let day = dig2(&s[8..10])?;
    let hour = dig2(&s[11..13])?;
    let minute = dig2(&s[14..16])?;
    let second = dig2(&s[17..19])?;
    if hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    let days = fast_day(year, month, day)?;
    Some(days * DAY_MS + ((hour * 60 + minute) * 60 + second) * SECOND)
}

/// F1 `YYYY-MM-DDTHH:MM:SSZ`, F2 with `±HH:MM`, or F3 digits (leading zeros allowed).
#[inline(always)]
pub fn parse_ts(s: &[u8]) -> Option<i64> {
    if s.is_empty() {
        return None;
    }
    if s.len() == 20 && s[19] == b'Z' {
        return civil_ms(s);
    }
    if s.len() == 25 && (s[19] == b'+' || s[19] == b'-') && s[22] == b':' {
        let local = civil_ms(&s[..19])?;
        let hh = dig2(&s[20..22])?;
        let mm = dig2(&s[23..25])?;
        if hh > 23 || mm > 59 {
            return None;
        }
        let off = (hh * 60 + mm) * 60 * SECOND;
        let signed = if s[19] == b'-' { -off } else { off };
        return Some(local - signed);
    }
    // F3. A value that does not fit in i64 is outside every window.
    if !s[0].is_ascii_digit() {
        return None;
    }
    let mut v: u64 = 0;
    for &c in s {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        v = v.checked_mul(10)?.checked_add(d as u64)?;
    }
    i64::try_from(v).ok()
}

pub fn format_day(days: i32, out: &mut [u8; 10]) {
    let c = civil_from_days(days as i64);
    let y = c.year as u32;
    let m = c.month as u32;
    let d = c.day as u32;
    out[0] = b'0' + (y / 1000) as u8;
    out[1] = b'0' + ((y / 100) % 10) as u8;
    out[2] = b'0' + ((y / 10) % 10) as u8;
    out[3] = b'0' + (y % 10) as u8;
    out[4] = b'-';
    out[5] = b'0' + (m / 10) as u8;
    out[6] = b'0' + (m % 10) as u8;
    out[7] = b'-';
    out[8] = b'0' + (d / 10) as u8;
    out[9] = b'0' + (d % 10) as u8;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epoch_and_known_instants() {
        assert_eq!(days_from_civil(1970, 1, 1), Some(0));
        assert_eq!(civil_from_days(0), Civil { year: 1970, month: 1, day: 1 });
        let f1 = parse_ts(b"2026-10-03T23:58:10Z").unwrap();
        let f2 = parse_ts(b"2026-10-03T19:58:10-04:00").unwrap();
        assert_eq!(f1, f2);
        assert_eq!(parse_ts(b"2026-10-03T23:58:10-00:00").unwrap(), f1);
        assert_eq!(parse_ts(b"0001791071890000"), parse_ts(b"1791071890000"));
        assert!(parse_ts(b"2028-02-29T00:00:00Z").is_some());
        assert!(parse_ts(b"2027-02-29T00:00:00Z").is_none());
        assert!(parse_ts(b"2026-02-30T00:00:00Z").is_none());
        assert!(parse_ts(b"2026-13-01T00:00:00Z").is_none());
        assert!(parse_ts(b"2026-10-03T24:00:00Z").is_none());
        assert!(parse_ts(b"2026-10-03T23:58:60Z").is_none());
        assert!(parse_ts(b"2026-10-03T23:58:10z").is_none());
        assert!(parse_ts(b"2026-10-03t23:58:10Z").is_none());
        assert!(parse_ts(b"2026-10-03T23:58:10+24:00").is_none());
        assert!(parse_ts(b"2026-10-03T23:58:10+05:60").is_none());
        assert!(parse_ts(b"2026-10-03T23:58:10.0Z").is_none());
        assert!(parse_ts(b"-5").is_none());
        assert!(parse_ts(b"").is_none());
        let days = days_from_civil(2027, 12, 9).unwrap();
        let mut buf = [0u8; 10];
        format_day(days as i32, &mut buf);
        assert_eq!(&buf, b"2027-12-09");
    }
}
