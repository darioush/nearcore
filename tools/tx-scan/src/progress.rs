use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};
use std::io::IsTerminal;
use std::time::{Duration, Instant};

/// Reports how far a long pass has got. A drawn bar needs a terminal, so when
/// standard error is redirected, as it is over `ssh --command` or into a log
/// file, this prints a plain line every `interval` instead.
pub struct ProgressReporter {
    bar: ProgressBar,
    unit: &'static str,
    total: u64,
    position: u64,
    /// Position this run resumed from. Rate and estimate count only the work
    /// done since then, so a resumed prefix is not treated as instant.
    start_position: u64,
    started: Instant,
    last_line: Instant,
    interval: Duration,
    plain: bool,
}

impl ProgressReporter {
    pub fn new(total: u64, unit: &'static str, interval: Duration) -> anyhow::Result<Self> {
        let plain = !std::io::stderr().is_terminal();
        let bar = ProgressBar::new(total);
        if plain {
            bar.set_draw_target(ProgressDrawTarget::hidden());
        } else {
            let template = format!(
                "{{spinner}} [{{elapsed_precise}}] [{{bar:40}}] {{pos}}/{{len}} {unit} ({{per_sec}}, eta {{eta}})"
            );
            bar.set_style(ProgressStyle::with_template(&template)?.progress_chars("=> "));
        }
        let now = Instant::now();
        Ok(Self {
            bar,
            unit,
            total,
            position: 0,
            start_position: 0,
            started: now,
            last_line: now,
            interval,
            plain,
        })
    }

    pub fn set_position(&mut self, position: u64) {
        self.position = position;
        self.bar.set_position(position);
        self.report_line(false);
    }

    pub fn advance(&mut self, delta: u64) {
        self.set_position(self.position + delta);
    }

    /// Where the pass resumed from, so the rate is measured over work this run
    /// actually did rather than counting a resumed prefix as instant.
    pub fn start_at(&mut self, position: u64) {
        self.position = position;
        self.start_position = position;
        self.bar.set_position(position);
        self.bar.reset_eta();
        self.bar.reset_elapsed();
        self.started = Instant::now();
        if self.plain && position > 0 {
            eprintln!("resuming at {position}/{} {}", self.total, self.unit);
        }
    }

    fn report_line(&mut self, force: bool) {
        if !self.plain {
            return;
        }
        if !force && self.last_line.elapsed() < self.interval {
            return;
        }
        self.last_line = Instant::now();
        let elapsed = self.started.elapsed().as_secs_f64();
        let done_this_run = self.position.saturating_sub(self.start_position);
        let rate = if elapsed > 0.0 { done_this_run as f64 / elapsed } else { 0.0 };
        let percent =
            if self.total > 0 { self.position as f64 * 100.0 / self.total as f64 } else { 0.0 };
        let remaining = self.total.saturating_sub(self.position);
        let eta = if rate > 0.0 { format_duration(remaining as f64 / rate) } else { "?".into() };
        eprintln!(
            "{} {}/{} ({percent:.2}%) {done_this_run} this run at {rate:.1}/s elapsed {} eta {eta}",
            self.unit,
            self.position,
            self.total,
            format_duration(elapsed)
        );
    }

    pub fn finish(mut self) {
        self.report_line(true);
        if self.plain {
            eprintln!(
                "{} finished: {}/{}, {} this run in {}",
                self.unit,
                self.position,
                self.total,
                self.position.saturating_sub(self.start_position),
                format_duration(self.started.elapsed().as_secs_f64())
            );
        } else {
            self.bar.finish();
        }
    }
}

fn format_duration(seconds: f64) -> String {
    let seconds = seconds.max(0.0) as u64;
    let (hours, minutes, seconds) = (seconds / 3600, (seconds % 3600) / 60, seconds % 60);
    if hours > 0 {
        format!("{hours}h{minutes:02}m{seconds:02}s")
    } else {
        format!("{minutes}m{seconds:02}s")
    }
}
