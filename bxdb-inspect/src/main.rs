use rustc_hash::FxHashMap;
use std::fs::File;
use std::io::{self, BufReader, Read, Seek};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use bxdb::chunk::{
    ChunkKind, ChunkRecord, FIXED_RECORD_SIZE, HEADER_SIZE, LOG_RECORD_BASE_SIZE, MAGIC_IDX,
    MAGIC_LOG, pa_of, snapshot_of,
};
use bxdb::format::read_and_verify_header;
use bxdb::btree::IndexMode;

use crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};

use ratatui::Frame;
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, List, ListItem, ListState, Paragraph};

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    let mut cli_snapshot: Option<u32> = None;
    let mut cli_pa: Option<u64> = None;
    let mut db_arg: Option<&str> = None;
    let mut i = 1;
    while i < args.len() {
        if args[i] == "--snapshot" && i + 1 < args.len() {
            match args[i + 1].parse() {
                Ok(s) => cli_snapshot = Some(s),
                Err(_) => {
                    eprintln!("bxdb-inspect: invalid snapshot id: {}", args[i + 1]);
                    return ExitCode::from(2);
                }
            }
            i += 2;
        } else if args[i] == "--pa" && i + 1 < args.len() {
            let val = &args[i + 1];
            let hex = if val.starts_with("0x") || val.starts_with("0X") {
                &val[2..]
            } else {
                val
            };
            match u64::from_str_radix(hex, 16) {
                Ok(p) => cli_pa = Some(p),
                Err(_) => {
                    eprintln!("bxdb-inspect: invalid pa (expected hex): {}", val);
                    return ExitCode::from(2);
                }
            }
            i += 2;
        } else {
            db_arg = Some(&args[i]);
            i += 1;
        }
    }
    let Some(db_arg) = db_arg else {
        eprintln!("usage: {} [--snapshot N] [--pa HEX] <db-dir>", args[0]);
        return ExitCode::from(2);
    };
    let dir = PathBuf::from(db_arg);
    let (mode, records, max_snap) = match load_records(&dir) {
        Ok(x) => x,
        Err(e) => {
            eprintln!("bxdb-inspect: {e}");
            return ExitCode::FAILURE;
        }
    };
    let stats = Stats::from_records(&records);
    let mut app = App::new(dir, mode, records, stats, max_snap);
    if cli_snapshot.is_some() || cli_pa.is_some() {
        app.apply_filter(cli_snapshot, cli_pa);
    }
    if let Err(e) = run(app) {
        eprintln!("bxdb-inspect: {e}");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}

// --- data loading -----------------------------------------------------------

fn load_records(dir: &Path) -> io::Result<(IndexMode, Vec<ChunkRecord>, u32)> {
    let idx_path = dir.join("index.bxdb");
    let log_path = dir.join("chunks.log");
    if idx_path.exists() {
        let f = File::open(&idx_path)?;
        let meta = f.metadata()?;
        let mut r = BufReader::new(f);
        let max_snap = read_and_verify_header(&mut r, &MAGIC_IDX)?;
        let capacity = (meta.len().saturating_sub(HEADER_SIZE as u64)
            / FIXED_RECORD_SIZE as u64) as usize;
        let mut records = Vec::with_capacity(capacity);
        let mut buf = [0u8; FIXED_RECORD_SIZE];
        let mut last_pct = 0u8;
        loop {
            match r.read_exact(&mut buf) {
                Ok(()) => records.push(ChunkRecord::decode_fixed(&buf)?),
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
            let pct = (records.len() as f64 * 100.0 / capacity as f64) as u8;
            if pct != last_pct && pct < 100 {
                last_pct = pct;
                if pct % 5 == 0 {
                    eprint!("\r  Load index.bxdb [{:>3}%] {} / {} records",
                        pct, records.len(), capacity);
                }
            }
        }
        eprintln!("\r  Load index.bxdb [100%] {} records", records.len());
        Ok((IndexMode::BTree, records, max_snap))
    } else if log_path.exists() {
        let f = File::open(&log_path)?;
        let meta = f.metadata()?;
        let total_bytes = meta.len();
        let mut r = BufReader::new(f);
        let max_snap = read_and_verify_header(&mut r, &MAGIC_LOG)?;
        let capacity = (total_bytes.saturating_sub(HEADER_SIZE as u64)
            / LOG_RECORD_BASE_SIZE as u64) as usize;
        let mut records = Vec::with_capacity(capacity);
        let mut last_pct = 0u8;
        while let Some(rec) = ChunkRecord::read_from(&mut r)? {
            records.push(rec);
            let pos = r.stream_position()?;
            let pct = ((pos as f64 * 100.0 / total_bytes as f64) as u8).min(100);
            if pct != last_pct && pct < 100 {
                last_pct = pct;
                if pct % 5 == 0 {
                    eprint!("\r  Load chunks.log [{:>3}%] {} records",
                        pct, records.len());
                }
            }
        }
        eprintln!("\r  Load chunks.log [100%] {} records (sorting...)", records.len());
        records.sort_by_key(|r| r.key);
        eprintln!("  Sorted {} records.", records.len());
        Ok((IndexMode::AppendOnly, records, max_snap))
    } else {
        Err(io::Error::new(
            io::ErrorKind::NotFound,
            "neither index.bxdb nor chunks.log found in db dir",
        ))
    }
}

struct Stats {
    total: usize,
    full: usize,
    delta: usize,
    zero: usize,
    full_bytes: u64,
    delta_bytes: u64,
}

impl Stats {
    fn from_records(records: &[ChunkRecord]) -> Self {
        let mut s = Self {
            total: records.len(),
            full: 0,
            delta: 0,
            zero: 0,
            full_bytes: 0,
            delta_bytes: 0,
        };
        for r in records {
            match r.kind {
                ChunkKind::Full => {
                    s.full += 1;
                    s.full_bytes += r.len as u64;
                }
                ChunkKind::Delta => {
                    s.delta += 1;
                    s.delta_bytes += r.len as u64;
                }
                ChunkKind::Zero => s.zero += 1,
            }
        }
        s
    }
    fn pct(&self, n: usize) -> f64 {
        if self.total == 0 {
            0.0
        } else {
            (n as f64) * 100.0 / (self.total as f64)
        }
    }
    fn total_bytes(&self) -> u64 {
        self.full_bytes + self.delta_bytes
    }
    fn byte_pct(&self, n: u64) -> f64 {
        let t = self.total_bytes();
        if t == 0 { 0.0 } else { (n as f64) * 100.0 / (t as f64) }
    }
}

fn fmt_bytes(n: u64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = KIB * 1024.0;
    const GIB: f64 = MIB * 1024.0;
    let f = n as f64;
    if f >= GIB {
        format!("{:.2} GiB", f / GIB)
    } else if f >= MIB {
        format!("{:.2} MiB", f / MIB)
    } else if f >= KIB {
        format!("{:.2} KiB", f / KIB)
    } else {
        format!("{n} B")
    }
}

// --- app state --------------------------------------------------------------

struct App {
    dir: PathBuf,
    mode: IndexMode,
    records: Vec<ChunkRecord>,


    stats: Stats,
    max_snapshot: u32,
    list_state: ListState,
    blob_files: FxHashMap<u8, File>,
    detail_scroll: u16,
    detail: Option<(usize, Detail)>,
    filter_snapshot: Option<u32>,
    filter_pa: Option<u64>,
    filtered_indices: Option<Vec<usize>>,
    filter_input: Option<String>,
    export_input: Option<String>,
    export_status: Option<String>,
}

enum Detail {
    Full { decompressed: Vec<u8> },
    Delta { patch: Vec<(u16, u64)> },
    Zero,
    Err(String),
}

impl App {
    fn new(dir: PathBuf, mode: IndexMode, records: Vec<ChunkRecord>, stats: Stats, max_snapshot: u32) -> Self {
        let mut list_state = ListState::default();
        if !records.is_empty() {
            list_state.select(Some(0));
        }
        Self {
            dir,
            mode,
            records,
            stats,
            max_snapshot,
            list_state,
            blob_files: FxHashMap::default(),
            detail_scroll: 0,
            detail: None,
            filter_snapshot: None,
            filter_pa: None,
            filtered_indices: None,
            filter_input: None,
            export_input: None,
            export_status: None,
        }
    }

    fn selected(&self) -> Option<usize> {
        self.list_state.selected()
    }

    fn move_by(&mut self, delta: isize) {
        if self.active_count() == 0 {
            return;
        }
        let cur = self.list_state.selected().unwrap_or(0) as isize;
        let last = self.active_count() as isize - 1;
        let next = (cur + delta).clamp(0, last) as usize;
        self.list_state.select(Some(next));
        self.detail_scroll = 0;
    }

    fn go_to(&mut self, i: usize) {
        let total = self.active_count();
        if total == 0 {
            return;
        }
        let clamped = i.min(total - 1);
        self.list_state.select(Some(clamped));
        self.detail_scroll = 0;
    }

    fn ensure_detail(&mut self) {
        let Some(i) = self.selected() else {
            return;
        };
        if self.detail.as_ref().map(|(idx, _)| *idx) == Some(i) {
            return;
        }
        let rec = *self.record_at(i);
        let d = match rec.kind {
            ChunkKind::Zero => Detail::Zero,
            ChunkKind::Full => match self.read_blob(rec.worker_id, rec.offset, rec.len) {
                Ok(b) => match zstd::decode_all(&b[..]) {
                    Ok(d) => Detail::Full { decompressed: d },
                    Err(e) => Detail::Err(format!("zstd decode failed: {e}")),
                },
                Err(e) => Detail::Err(format!("blob read failed: {e}")),
            },
            ChunkKind::Delta => match self.read_blob(rec.worker_id, rec.offset, rec.len) {
                Ok(b) => match decode_delta(&b) {
                    Ok(p) => Detail::Delta { patch: p },
                    Err(e) => Detail::Err(format!("bad delta blob: {e}")),
                },
                Err(e) => Detail::Err(format!("blob read failed: {e}")),
            },
        };
        self.detail = Some((i, d));
    }

    fn read_blob(&mut self, worker_id: u8, offset: u64, len: u32) -> io::Result<Vec<u8>> {
        if !self.blob_files.contains_key(&worker_id) {
            let path = self
                .dir
                .join("blobs")
                .join(format!("worker_{worker_id}.blob"));
            self.blob_files.insert(worker_id, File::open(&path)?);
        }
        let f = &self.blob_files[&worker_id];
        let mut buf = vec![0u8; len as usize];
        f.read_exact_at(&mut buf, offset)?;
        Ok(buf)
    }

    fn active_count(&self) -> usize {
        match &self.filtered_indices {
            Some(f) => f.len(),
            None => self.records.len(),
        }
    }

    fn real_idx(&self, active_idx: usize) -> usize {
        match &self.filtered_indices {
            Some(f) => f[active_idx],
            None => active_idx,
        }
    }

    fn record_at(&self, active_idx: usize) -> &ChunkRecord {
        &self.records[self.real_idx(active_idx)]
    }

    fn apply_filter(&mut self, snap: Option<u32>, pa: Option<u64>) {
        let indices: Vec<usize> = self
            .records
            .iter()
            .enumerate()
            .filter(|(_, r)| {
                if let Some(s) = snap {
                    if snapshot_of(r.key) != s {
                        return false;
                    }
                }
                if let Some(p) = pa {
                    if pa_of(r.key) != p {
                        return false;
                    }
                }
                true
            })
            .map(|(i, _)| i)
            .collect();
        self.filtered_indices = Some(indices);
        self.filter_snapshot = snap;
        self.filter_pa = pa;
        self.detail = None;
        self.list_state.select(if self.active_count() > 0 { Some(0) } else { None });
        self.detail_scroll = 0;
    }

    fn clear_filter(&mut self) {
        self.filtered_indices = None;
        self.filter_snapshot = None;
        self.filter_pa = None;
        self.detail = None;
        self.list_state.select(if self.active_count() > 0 { Some(0) } else { None });
        self.detail_scroll = 0;
    }

    fn export_detail(&mut self, path: &str) {
        let msg = match &self.detail {
            Some((_, Detail::Full { decompressed })) => {
                match std::fs::write(path, decompressed) {
                    Ok(()) => format!("Exported {} bytes to {path}", decompressed.len()),
                    Err(e) => format!("Export error: {e}"),
                }
            }
            _ => "Nothing to export (select a Full chunk)".to_string(),
        };
        self.export_status = Some(msg);
    }
}

fn decode_delta(blob: &[u8]) -> Result<Vec<(u16, u64)>, String> {
    if !blob.len().is_multiple_of(10) {
        return Err(format!("length {} not a multiple of 10", blob.len()));
    }
    let mut out = Vec::with_capacity(blob.len() / 10);
    for c in blob.chunks_exact(10) {
        let idx = u16::from_le_bytes([c[0], c[1]]);
        let v = u64::from_le_bytes(c[2..10].try_into().unwrap());
        out.push((idx, v));
    }
    Ok(out)
}

fn parse_filter_input(s: &str) -> (Option<u32>, Option<u64>) {
    let mut snap = None;
    let mut pa = None;
    for token in s.split_whitespace() {
        if token.is_empty() {
            continue;
        }
        if token.starts_with("0x") || token.starts_with("0X") {
            if let Ok(p) = u64::from_str_radix(&token[2..], 16) {
                pa = Some(p);
            }
        } else if token.chars().all(|c| c.is_ascii_digit()) {
            if let Ok(s) = token.parse::<u32>() {
                snap = Some(s);
            }
        }
    }
    (snap, pa)
}

// --- terminal lifecycle -----------------------------------------------------

fn run(mut app: App) -> io::Result<()> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let result = event_loop(&mut app, &mut terminal);

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;
    result
}

fn event_loop(
    app: &mut App,
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
) -> io::Result<()> {
    loop {
        app.ensure_detail();
        terminal.draw(|f| draw(f, app))?;
        let ev = event::read()?;
        let Event::Key(k) = ev else { continue };
        if k.kind != KeyEventKind::Press {
            continue;
        }

        // --- filter input mode ---
        if let Some(ref mut buf) = app.filter_input {
            match k.code {
                KeyCode::Esc => {
                    app.filter_input = None;
                }
                KeyCode::Enter => {
                    let s = std::mem::take(buf);
                    app.filter_input = None;
                    if s.is_empty() {
                        app.clear_filter();
                    } else {
                        let (snap, pa) = parse_filter_input(&s);
                        if snap.is_some() || pa.is_some() {
                            app.apply_filter(snap, pa);
                        }
                    }
                }
                KeyCode::Backspace => {
                    buf.pop();
                }
                KeyCode::Char(c) if c.is_ascii_hexdigit() || c == 'x' || c == 'X' || c == ' ' => {
                    if buf.len() < 40 {
                        buf.push(c);
                    }
                }
                _ => {}
            }
            continue;
        }

        // --- export input mode ---
        if let Some(ref mut path) = app.export_input {
            match k.code {
                KeyCode::Esc => {
                    app.export_input = None;
                }
                KeyCode::Enter => {
                    let p = std::mem::take(path);
                    app.export_input = None;
                    if !p.is_empty() {
                        app.export_detail(&p);
                    }
                }
                KeyCode::Backspace => {
                    path.pop();
                }
                KeyCode::Char(c) if !c.is_control() => {
                    if path.len() < 255 {
                        path.push(c);
                    }
                }
                _ => {}
            }
            continue;
        }

        app.export_status = None;
        match (k.code, k.modifiers) {
            (KeyCode::Char('q'), _) | (KeyCode::Esc, _) => break,
            (KeyCode::Char('c'), KeyModifiers::CONTROL) => break,
            (KeyCode::Char('f'), _) => {
                if app.filter_snapshot.is_some() || app.filter_pa.is_some() {
                    app.clear_filter();
                } else {
                    app.filter_input = Some(String::new());
                }
            }
            (KeyCode::Char('e'), _) => {
                if let Some(i) = app.selected() {
                    if app.record_at(i).kind == ChunkKind::Full {
                        app.export_input = Some(String::new());
                    }
                }
            }
            (KeyCode::Up, _) => app.move_by(-1),
            (KeyCode::Down, _) => app.move_by(1),
            (KeyCode::Char('k'), m) if !m.contains(KeyModifiers::SHIFT) => app.move_by(-1),
            (KeyCode::Char('j'), m) if !m.contains(KeyModifiers::SHIFT) => app.move_by(1),
            (KeyCode::PageUp, _) => app.move_by(-10),
            (KeyCode::PageDown, _) => app.move_by(10),
            (KeyCode::Home, _) | (KeyCode::Char('g'), _) => app.go_to(0),
            (KeyCode::End, _) | (KeyCode::Char('G'), _) => app.go_to(usize::MAX),
            (KeyCode::Char('K'), _) => app.detail_scroll = app.detail_scroll.saturating_sub(1),
            (KeyCode::Char('J'), _) => app.detail_scroll = app.detail_scroll.saturating_add(1),
            (KeyCode::Char('u'), KeyModifiers::CONTROL) => {
                app.detail_scroll = app.detail_scroll.saturating_sub(10)
            }
            (KeyCode::Char('d'), KeyModifiers::CONTROL) => {
                app.detail_scroll = app.detail_scroll.saturating_add(10)
            }
            _ => {}
        }
    }
    Ok(())
}

// --- rendering --------------------------------------------------------------

fn draw(f: &mut Frame, app: &App) {
    let vert = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(5),
            Constraint::Min(0),
            Constraint::Length(if app.filter_input.is_some() || app.export_input.is_some() { 2 } else { 1 }),
        ])
        .split(f.area());

    draw_header(f, vert[0], app);
    let body = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(40), Constraint::Percentage(60)])
        .split(vert[1]);
    draw_list(f, body[0], app);
    draw_detail(f, body[1], app);
    if let Some(ref path) = app.export_input {
        draw_export_prompt(f, vert[2], path);
    } else if app.filter_input.is_some() {
        draw_filter_prompt(f, vert[2], app);
    } else if let Some(ref status) = app.export_status {
        draw_status(f, vert[2], status);
    } else {
        draw_help(f, vert[2]);
    }
}

fn draw_header(f: &mut Frame, area: Rect, app: &App) {
    let mode = match app.mode {
        IndexMode::BTree => "B-Tree (index.bxdb)",
        IndexMode::AppendOnly => "Append-Only (chunks.log)",
    };
    let s = &app.stats;
    let mut header = vec![
        Span::raw("Path: "),
        Span::styled(
            app.dir.display().to_string(),
            Style::default().fg(Color::Cyan),
        ),
        Span::raw("   Mode: "),
        Span::styled(mode, Style::default().fg(Color::Yellow)),
        Span::raw(format!("   Records: {}", s.total)),
        Span::raw(format!("   Max snapshot: {}", app.max_snapshot)),
    ];
    if let Some(s) = app.filter_snapshot {
        header.push(Span::raw("   Filter: "));
        header.push(Span::styled(
            format!("snap={s}"),
            Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
        ));
    }
    if let Some(p) = app.filter_pa {
        if app.filter_snapshot.is_some() {
            header.push(Span::raw("  "));
        } else {
            header.push(Span::raw("   Filter: "));
        }
        header.push(Span::styled(
            format!("pa=0x{p:x}"),
            Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
        ));
    }
    if app.filter_snapshot.is_some() || app.filter_pa.is_some() {
        header.push(Span::raw(format!(
            "  ({} records)",
            app.active_count()
        )));
    }
    let lines = vec![
        Line::from(header),
        Line::from(vec![
            Span::styled("Full ", Style::default().fg(Color::Green)),
            Span::raw(format!("{:>6} ({:5.1}%)   ", s.full, s.pct(s.full))),
            Span::styled("Delta ", Style::default().fg(Color::Magenta)),
            Span::raw(format!("{:>6} ({:5.1}%)   ", s.delta, s.pct(s.delta))),
            Span::styled("Zero ", Style::default().fg(Color::DarkGray)),
            Span::raw(format!("{:>6} ({:5.1}%)", s.zero, s.pct(s.zero))),
        ]),
        Line::from(vec![
            Span::raw("Blob: "),
            Span::styled("Full ", Style::default().fg(Color::Green)),
            Span::raw(format!(
                "{:>10} ({:5.1}%)   ",
                fmt_bytes(s.full_bytes),
                s.byte_pct(s.full_bytes)
            )),
            Span::styled("Delta ", Style::default().fg(Color::Magenta)),
            Span::raw(format!(
                "{:>10} ({:5.1}%)   ",
                fmt_bytes(s.delta_bytes),
                s.byte_pct(s.delta_bytes)
            )),
            Span::raw(format!("Total {}", fmt_bytes(s.total_bytes()))),
        ]),
    ];
    let p =
        Paragraph::new(lines).block(Block::default().borders(Borders::ALL).title("bxdb-inspect"));
    f.render_widget(p, area);
}

fn draw_list(f: &mut Frame, area: Rect, app: &App) {
    let total = app.active_count();
    if total == 0 {
        return;
    }
    let visible = (area.height as usize).saturating_sub(2); // borders
    let sel = app.list_state.selected().unwrap_or(0);

    // Keep the selected row centered in the visible window.
    let start = if sel < visible / 2 {
        0
    } else {
        (sel - visible / 2).min(total.saturating_sub(visible))
    };
    let end = (start + visible).min(total);

    let items: Vec<ListItem> = (start..end)
        .map(|i| {
            let rec = app.record_at(i);
            let pa = pa_of(rec.key);
            let snap = snapshot_of(rec.key);
            let label = format!("PA=0x{pa:011x}  S={snap:>5}");
            let (sym, col) = kind_marker(rec.kind);
            ListItem::new(Line::from(vec![
                Span::styled(
                    format!("{sym} "),
                    Style::default().fg(col).add_modifier(Modifier::BOLD),
                ),
                Span::raw(label.to_owned()),
            ]))
        })
        .collect();

    let mut state = ListState::default()
        .with_selected(Some(sel - start))
        .with_offset(0);
    let title = match (app.filter_snapshot, app.filter_pa) {
        (Some(s), Some(p)) => format!("Records [{} / {}]  filter: snap={s} pa=0x{p:x}", total, app.records.len()),
        (Some(s), None) => format!("Records [{} / {}]  filter: snap={s}", total, app.records.len()),
        (None, Some(p)) => format!("Records [{} / {}]  filter: pa=0x{p:x}", total, app.records.len()),
        (None, None) => format!("Records [{total}]"),
    };
    let list = List::new(items)
        .block(Block::default().borders(Borders::ALL).title(title))
        .highlight_style(
            Style::default()
                .bg(Color::DarkGray)
                .add_modifier(Modifier::BOLD),
        )
        .highlight_symbol("> ");
    f.render_stateful_widget(list, area, &mut state);
}

fn draw_detail(f: &mut Frame, area: Rect, app: &App) {
    let (title, lines) = match app.selected() {
        None => ("Detail".to_string(), vec![Line::raw("(no records)")]),
        Some(i) => {
            let rec = app.record_at(i);
            let pa = pa_of(rec.key);
            let snap = snapshot_of(rec.key);
            let title = format!("Detail - PA=0x{pa:x}  S={snap}");
            let mut v = vec![
                Line::from(vec![
                    Span::raw("Key (u64):     "),
                    Span::styled(
                        format!("0x{:016x}", rec.key),
                        Style::default().fg(Color::Cyan),
                    ),
                ]),
                Line::from(vec![
                    Span::raw("PA (45 bits):  "),
                    Span::styled(
                        format!("0x{pa:x}  ({pa})"),
                        Style::default().fg(Color::Cyan),
                    ),
                ]),
                Line::from(vec![
                    Span::raw("Snapshot id:   "),
                    Span::styled(format!("{snap}"), Style::default().fg(Color::Cyan)),
                ]),
                Line::from(vec![Span::raw("Kind:          "), kind_span(rec.kind)]),
            ];
            match rec.kind {
                ChunkKind::Zero => {
                    v.push(Line::raw("(no blob data - page is all zero)"));
                }
                ChunkKind::Full => {
                    v.push(Line::raw(format!(
                        "Worker blob:   worker_{}.blob",
                        rec.worker_id
                    )));
                    v.push(Line::raw(format!(
                        "Blob range:    [0x{:x} .. 0x{:x})   ({} bytes)",
                        rec.offset,
                        rec.offset + rec.len as u64,
                        rec.len
                    )));
                    match app.detail.as_ref() {
                        Some((idx, Detail::Full { decompressed })) if *idx == i => {
                            v.push(Line::raw(format!(
                                "Decompressed:  {} bytes",
                                decompressed.len()
                            )));
                            v.push(Line::raw(""));
                            v.push(Line::styled(
                                "-- hex dump --",
                                Style::default().add_modifier(Modifier::DIM),
                            ));
                            append_hex(&mut v, decompressed);
                        }
                        Some((idx, Detail::Err(msg))) if *idx == i => {
                            v.push(Line::styled(
                                format!("error: {msg}"),
                                Style::default().fg(Color::Red),
                            ));
                        }
                        _ => v.push(Line::raw("(loading...)")),
                    }
                }
                ChunkKind::Delta => {
                    v.push(Line::raw(format!(
                        "Worker blob:   worker_{}.blob",
                        rec.worker_id
                    )));
                    v.push(Line::raw(format!(
                        "Blob range:    [0x{:x} .. 0x{:x})   ({} bytes)",
                        rec.offset,
                        rec.offset + rec.len as u64,
                        rec.len
                    )));
                    v.push(Line::raw(format!(
                        "Base key:      0x{:016x}   (PA=0x{:x}  S={})",
                        rec.base_key,
                        pa_of(rec.base_key),
                        snapshot_of(rec.base_key)
                    )));
                    match app.detail.as_ref() {
                        Some((idx, Detail::Delta { patch })) if *idx == i => {
                            v.push(Line::raw(format!("Patch:         {} entries", patch.len())));
                            v.push(Line::raw(""));
                            v.push(Line::styled(
                                "  word  |  xor value           |  page bytes",
                                Style::default().add_modifier(Modifier::DIM),
                            ));
                            for (widx, xor) in patch {
                                let byte = (*widx as usize) * 8;
                                v.push(Line::raw(format!(
                                    "  {:>4}  |  0x{:016x}    |  [{:>4} .. {:>4})",
                                    widx,
                                    xor,
                                    byte,
                                    byte + 8
                                )));
                            }
                        }
                        Some((idx, Detail::Err(msg))) if *idx == i => {
                            v.push(Line::styled(
                                format!("error: {msg}"),
                                Style::default().fg(Color::Red),
                            ));
                        }
                        _ => v.push(Line::raw("(loading...)")),
                    }
                }
            }
            (title, v)
        }
    };
    let p = Paragraph::new(lines)
        .block(Block::default().borders(Borders::ALL).title(title))
        .scroll((app.detail_scroll, 0));
    f.render_widget(p, area);
}

fn draw_filter_prompt(f: &mut Frame, area: Rect, app: &App) {
    let buf = app.filter_input.as_deref().unwrap_or("");
    let line = Line::from(vec![
        Span::styled(
            "Filter (snapshot and/or PA, e.g. 42 0x1234): ",
            Style::default().fg(Color::Yellow),
        ),
        Span::styled(
            if buf.is_empty() { "(Enter to apply, Esc to cancel)" }
            else { buf },
            Style::default().fg(Color::White),
        ),
    ]);
    f.render_widget(Paragraph::new(line), area);
}

fn kind_marker(k: ChunkKind) -> (&'static str, Color) {
    match k {
        ChunkKind::Full => ("F", Color::Green),
        ChunkKind::Delta => ("D", Color::Magenta),
        ChunkKind::Zero => ("Z", Color::DarkGray),
    }
}

fn kind_span(k: ChunkKind) -> Span<'static> {
    let (label, color) = match k {
        ChunkKind::Full => ("Full", Color::Green),
        ChunkKind::Delta => ("Delta", Color::Magenta),
        ChunkKind::Zero => ("Zero", Color::DarkGray),
    };
    Span::styled(
        label,
        Style::default().fg(color).add_modifier(Modifier::BOLD),
    )
}

fn append_hex(out: &mut Vec<Line<'_>>, data: &[u8]) {
    for (row, chunk) in data.chunks(16).enumerate() {
        let off = row * 16;
        let mut hex = String::with_capacity(16 * 3 + 1);
        let mut asc = String::with_capacity(16);
        for (i, b) in chunk.iter().enumerate() {
            if i == 8 {
                hex.push(' ');
            }
            hex.push_str(&format!("{b:02x} "));
            asc.push(if *b >= 0x20 && *b < 0x7f {
                *b as char
            } else {
                '.'
            });
        }
        out.push(Line::raw(format!("  {off:04x}  {hex:<50}  |{asc}|")));
    }
}

fn draw_help(f: &mut Frame, area: Rect) {
    let line = Line::from(vec![
        Span::styled(" j/k/Up/Down ", Style::default().fg(Color::Yellow)),
        Span::raw("select  "),
        Span::styled(" PgUp/PgDn ", Style::default().fg(Color::Yellow)),
        Span::raw("+/-10  "),
        Span::styled(" g/G ", Style::default().fg(Color::Yellow)),
        Span::raw("top/bot  "),
        Span::styled(" J/K ", Style::default().fg(Color::Yellow)),
        Span::raw("scroll  "),
        Span::styled(" ^U/^D ", Style::default().fg(Color::Yellow)),
        Span::raw("+/-10  "),
        Span::styled(" f ", Style::default().fg(Color::Red)),
        Span::raw("filter  "),
        Span::styled(" e ", Style::default().fg(Color::Green)),
        Span::raw("export  "),
        Span::styled(" q ", Style::default().fg(Color::Yellow)),
        Span::raw("quit"),
    ]);
    f.render_widget(Paragraph::new(line), area);
}

fn draw_export_prompt(f: &mut Frame, area: Rect, path: &str) {
    let line = Line::from(vec![
        Span::styled(
            "Export file: ",
            Style::default().fg(Color::Yellow),
        ),
        Span::styled(
            if path.is_empty() { "(type filename, Enter to save, Esc to cancel)" }
            else { path },
            Style::default().fg(Color::White),
        ),
    ]);
    f.render_widget(Paragraph::new(line), area);
}

fn draw_status(f: &mut Frame, area: Rect, msg: &str) {
    let line = Line::from(vec![
        Span::styled(msg, Style::default().fg(Color::Green)),
    ]);
    f.render_widget(Paragraph::new(line), area);
}
