use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufReader, Read};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use bxdb::chunk::{
    ChunkKind, ChunkRecord, FIXED_RECORD_SIZE, MAGIC_IDX, MAGIC_LOG, pa_of, snapshot_of,
};
use bxdb::format::read_and_verify_header;
use bxdb::read::IndexMode;

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
    if args.len() != 2 {
        eprintln!("usage: {} <db-dir>", args[0]);
        return ExitCode::from(2);
    }
    let dir = PathBuf::from(&args[1]);
    let (mode, records) = match load_records(&dir) {
        Ok(x) => x,
        Err(e) => {
            eprintln!("bxdb-inspect: {e}");
            return ExitCode::FAILURE;
        }
    };
    let stats = Stats::from_records(&records);
    let app = App::new(dir, mode, records, stats);
    if let Err(e) = run(app) {
        eprintln!("bxdb-inspect: {e}");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}

// --- data loading -----------------------------------------------------------

fn load_records(dir: &Path) -> io::Result<(IndexMode, Vec<ChunkRecord>)> {
    let idx_path = dir.join("index.bxdb");
    let log_path = dir.join("chunks.log");
    if idx_path.exists() {
        let f = File::open(&idx_path)?;
        let mut r = BufReader::new(f);
        read_and_verify_header(&mut r, &MAGIC_IDX)?;
        let mut records = Vec::new();
        let mut buf = [0u8; FIXED_RECORD_SIZE];
        loop {
            match r.read_exact(&mut buf) {
                Ok(()) => records.push(ChunkRecord::decode_fixed(&buf)?),
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
        }
        Ok((IndexMode::BTree, records))
    } else if log_path.exists() {
        let f = File::open(&log_path)?;
        let mut r = BufReader::new(f);
        read_and_verify_header(&mut r, &MAGIC_LOG)?;
        let mut records = Vec::new();
        while let Some(rec) = ChunkRecord::read_from(&mut r)? {
            records.push(rec);
        }
        records.sort_by_key(|r| r.key);
        Ok((IndexMode::AppendOnly, records))
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
}

impl Stats {
    fn from_records(records: &[ChunkRecord]) -> Self {
        let mut s = Self { total: records.len(), full: 0, delta: 0, zero: 0 };
        for r in records {
            match r.kind {
                ChunkKind::Full => s.full += 1,
                ChunkKind::Delta => s.delta += 1,
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
}

// --- app state --------------------------------------------------------------

struct App {
    dir: PathBuf,
    mode: IndexMode,
    records: Vec<ChunkRecord>,
    stats: Stats,
    list_state: ListState,
    blob_files: HashMap<u8, File>,
    detail_scroll: u16,
    detail: Option<(usize, Detail)>,
}

enum Detail {
    Full { decompressed: Vec<u8> },
    Delta { patch: Vec<(u16, u64)> },
    Zero,
    Err(String),
}

impl App {
    fn new(dir: PathBuf, mode: IndexMode, records: Vec<ChunkRecord>, stats: Stats) -> Self {
        let mut list_state = ListState::default();
        if !records.is_empty() {
            list_state.select(Some(0));
        }
        Self {
            dir,
            mode,
            records,
            stats,
            list_state,
            blob_files: HashMap::new(),
            detail_scroll: 0,
            detail: None,
        }
    }

    fn selected(&self) -> Option<usize> {
        self.list_state.selected()
    }

    fn move_by(&mut self, delta: isize) {
        if self.records.is_empty() {
            return;
        }
        let cur = self.list_state.selected().unwrap_or(0) as isize;
        let last = self.records.len() as isize - 1;
        let next = (cur + delta).clamp(0, last) as usize;
        self.list_state.select(Some(next));
        self.detail_scroll = 0;
    }

    fn go_to(&mut self, i: usize) {
        if self.records.is_empty() {
            return;
        }
        let clamped = i.min(self.records.len() - 1);
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
        let rec = self.records[i];
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
        match (k.code, k.modifiers) {
            (KeyCode::Char('q'), _) | (KeyCode::Esc, _) => break,
            (KeyCode::Char('c'), KeyModifiers::CONTROL) => break,
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
            Constraint::Length(4),
            Constraint::Min(0),
            Constraint::Length(1),
        ])
        .split(f.area());

    draw_header(f, vert[0], app);
    let body = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(40), Constraint::Percentage(60)])
        .split(vert[1]);
    draw_list(f, body[0], app);
    draw_detail(f, body[1], app);
    draw_help(f, vert[2]);
}

fn draw_header(f: &mut Frame, area: Rect, app: &App) {
    let mode = match app.mode {
        IndexMode::BTree => "B-Tree (index.bxdb)",
        IndexMode::AppendOnly => "Append-Only (chunks.log)",
    };
    let s = &app.stats;
    let lines = vec![
        Line::from(vec![
            Span::raw("Path: "),
            Span::styled(
                app.dir.display().to_string(),
                Style::default().fg(Color::Cyan),
            ),
            Span::raw("   Mode: "),
            Span::styled(mode, Style::default().fg(Color::Yellow)),
            Span::raw(format!("   Records: {}", s.total)),
        ]),
        Line::from(vec![
            Span::styled("Full ", Style::default().fg(Color::Green)),
            Span::raw(format!("{:>6} ({:5.1}%)   ", s.full, s.pct(s.full))),
            Span::styled("Delta ", Style::default().fg(Color::Magenta)),
            Span::raw(format!("{:>6} ({:5.1}%)   ", s.delta, s.pct(s.delta))),
            Span::styled("Zero ", Style::default().fg(Color::DarkGray)),
            Span::raw(format!("{:>6} ({:5.1}%)", s.zero, s.pct(s.zero))),
        ]),
    ];
    let p = Paragraph::new(lines)
        .block(Block::default().borders(Borders::ALL).title("bxdb-inspect"));
    f.render_widget(p, area);
}

fn draw_list(f: &mut Frame, area: Rect, app: &App) {
    let items: Vec<ListItem> = app
        .records
        .iter()
        .map(|r| {
            let pa = pa_of(r.key);
            let snap = snapshot_of(r.key);
            let (sym, col) = kind_marker(r.kind);
            ListItem::new(Line::from(vec![
                Span::styled(
                    format!("{sym} "),
                    Style::default().fg(col).add_modifier(Modifier::BOLD),
                ),
                Span::raw(format!("PA=0x{pa:011x}  S={snap:>5}")),
            ]))
        })
        .collect();
    let list = List::new(items)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(format!("Records [{}]", app.records.len())),
        )
        .highlight_style(
            Style::default()
                .bg(Color::DarkGray)
                .add_modifier(Modifier::BOLD),
        )
        .highlight_symbol("> ");
    let mut state = app.list_state.clone();
    f.render_stateful_widget(list, area, &mut state);
}

fn draw_detail(f: &mut Frame, area: Rect, app: &App) {
    let (title, lines) = match app.selected() {
        None => ("Detail".to_string(), vec![Line::raw("(no records)")]),
        Some(i) => {
            let rec = &app.records[i];
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
                    Span::styled(format!("0x{pa:x}  ({pa})"), Style::default().fg(Color::Cyan)),
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
        out.push(Line::raw(format!(
            "  {off:04x}  {hex:<50}  |{asc}|"
        )));
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
        Span::raw("scroll detail  "),
        Span::styled(" ^U/^D ", Style::default().fg(Color::Yellow)),
        Span::raw("+/-10  "),
        Span::styled(" q ", Style::default().fg(Color::Yellow)),
        Span::raw("quit"),
    ]);
    f.render_widget(Paragraph::new(line), area);
}
