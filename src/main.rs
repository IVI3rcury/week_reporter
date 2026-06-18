use actix_web::{
    error::ErrorInternalServerError,
    get, post,
    web::{Bytes, Data, Form, Path, Query},
    App, HttpResponse, HttpServer, Result,
};
use chrono::Local;
use rusqlite::{params, Connection, OptionalExtension};
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::env;
use std::sync::Mutex;

const DB_PATH: &str = "weekly_log.db";

struct AppState {
    conn: Mutex<Connection>,
}

#[derive(Debug, Clone)]
struct OptionItem {
    id: i64,
    label: String,
    is_active: bool,
    sort_order: i64,
}

#[derive(Debug, Clone, Copy)]
enum OptionKind {
    Person,
    Project,
    Specialty,
}

impl OptionKind {
    fn table(self) -> &'static str {
        match self {
            OptionKind::Person => "persons",
            OptionKind::Project => "projects",
            OptionKind::Specialty => "specialties",
        }
    }

    fn label_column(self) -> &'static str {
        match self {
            OptionKind::Person => "name",
            OptionKind::Project => "project_no",
            OptionKind::Specialty => "name",
        }
    }

    fn title(self) -> &'static str {
        match self {
            OptionKind::Person => "人员名单",
            OptionKind::Project => "项目号选项",
            OptionKind::Specialty => "专业方向选项",
        }
    }

    fn path(self) -> &'static str {
        match self {
            OptionKind::Person => "/summary/options/persons",
            OptionKind::Project => "/summary/options/projects",
            OptionKind::Specialty => "/summary/options/specialties",
        }
    }

    fn input_label(self) -> &'static str {
        match self {
            OptionKind::Person => "人员姓名",
            OptionKind::Project => "项目号",
            OptionKind::Specialty => "专业方向",
        }
    }
}

#[derive(Debug, Clone)]
struct WeeklyLogView {
    id: i64,
    meeting_month: i64,
    meeting_count: i64,
    project_id: i64,
    project_no: String,
    specialty_id: i64,
    specialty: String,
    module_owner_id: i64,
    module_owner: String,
    last_work_item: String,
    completer_id: i64,
    completer: String,
    progress: String,
    reason: String,
    next_work_item: String,
    next_owner_id: i64,
    next_owner: String,
    created_at: String,
}

#[derive(Debug, Deserialize)]
struct WeeklyLogForm {
    meeting_month: i64,
    meeting_count: i64,
    project_id: i64,
    specialty_id: i64,
    module_owner_id: i64,
    last_work_item: String,
    completer_id: i64,
    progress: String,
    reason: String,
    next_work_item: String,
}

#[derive(Debug, Deserialize, Default, Clone)]
struct FilterQuery {
    meeting_month: Option<i64>,
    meeting_count: Option<i64>,
    project_id: Option<i64>,
    specialty_id: Option<i64>,
    person_id: Option<i64>,
    progress: Option<String>,
}

#[derive(Debug, Deserialize, Default, Clone)]
struct ProjectReportQuery {
    meeting_month: Option<i64>,
    meeting_count: Option<i64>,
    project_id: Option<i64>,
}

#[derive(Debug, Deserialize, Default, Clone)]
struct AttendanceQuery {
    meeting_month: Option<i64>,
    meeting_count: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct OptionForm {
    name: String,
}

fn now_string() -> String {
    Local::now().format("%Y-%m-%d %H:%M:%S").to_string()
}

fn esc(input: &str) -> String {
    input
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#x27;")
}

fn meeting_label(month: i64, count: i64) -> String {
    format!("{}月第{}次例会", month, count)
}

fn table_has_column(conn: &Connection, table: &str, column: &str) -> rusqlite::Result<bool> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({})", table))?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(1))?;
    for row in rows {
        if row? == column {
            return Ok(true);
        }
    }
    Ok(false)
}

fn init_db(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch(
        r#"
        PRAGMA foreign_keys = ON;

        CREATE TABLE IF NOT EXISTS persons (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            is_active INTEGER NOT NULL DEFAULT 1,
            sort_order INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS projects (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_no TEXT NOT NULL UNIQUE,
            is_active INTEGER NOT NULL DEFAULT 1,
            sort_order INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS specialties (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            is_active INTEGER NOT NULL DEFAULT 1,
            sort_order INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS weekly_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            start_date TEXT NOT NULL DEFAULT '',
            end_date TEXT NOT NULL DEFAULT '',
            meeting_month INTEGER NOT NULL,
            meeting_count INTEGER NOT NULL,
            project_id INTEGER NOT NULL,
            specialty_id INTEGER NOT NULL,
            module_owner_id INTEGER NOT NULL,
            last_work_item TEXT NOT NULL,
            completer_id INTEGER NOT NULL,
            progress TEXT NOT NULL,
            reason TEXT NOT NULL,
            next_work_item TEXT NOT NULL,
            next_owner_id INTEGER NOT NULL,
            created_at TEXT NOT NULL,

            FOREIGN KEY(project_id) REFERENCES projects(id),
            FOREIGN KEY(specialty_id) REFERENCES specialties(id),
            FOREIGN KEY(module_owner_id) REFERENCES persons(id),
            FOREIGN KEY(completer_id) REFERENCES persons(id),
            FOREIGN KEY(next_owner_id) REFERENCES persons(id)
        );
        "#,
    )?;

    // 兼容旧版数据库：旧版 weekly_logs 可能只有 start_date/end_date，没有例会字段。
    if !table_has_column(conn, "weekly_logs", "start_date")? {
        conn.execute("ALTER TABLE weekly_logs ADD COLUMN start_date TEXT NOT NULL DEFAULT ''", [])?;
    }
    if !table_has_column(conn, "weekly_logs", "end_date")? {
        conn.execute("ALTER TABLE weekly_logs ADD COLUMN end_date TEXT NOT NULL DEFAULT ''", [])?;
    }
    if !table_has_column(conn, "weekly_logs", "meeting_month")? {
        conn.execute("ALTER TABLE weekly_logs ADD COLUMN meeting_month INTEGER NOT NULL DEFAULT 1", [])?;
    }
    if !table_has_column(conn, "weekly_logs", "meeting_count")? {
        conn.execute("ALTER TABLE weekly_logs ADD COLUMN meeting_count INTEGER NOT NULL DEFAULT 1", [])?;
    }

    conn.execute_batch(
        r#"
        CREATE INDEX IF NOT EXISTS idx_weekly_logs_meeting
        ON weekly_logs(meeting_month, meeting_count);

        CREATE INDEX IF NOT EXISTS idx_weekly_logs_project
        ON weekly_logs(project_id);

        CREATE INDEX IF NOT EXISTS idx_weekly_logs_specialty
        ON weekly_logs(specialty_id);

        CREATE INDEX IF NOT EXISTS idx_weekly_logs_persons
        ON weekly_logs(module_owner_id, completer_id, next_owner_id);
        "#,
    )?;
    Ok(())
}

fn find_option_id(conn: &Connection, kind: OptionKind, label: &str) -> rusqlite::Result<Option<i64>> {
    let sql = format!(
        "SELECT id FROM {} WHERE {} = ?1",
        kind.table(),
        kind.label_column()
    );
    conn.query_row(&sql, params![label.trim()], |row| row.get(0))
        .optional()
}

fn ensure_option(conn: &Connection, kind: OptionKind, label: &str) -> rusqlite::Result<i64> {
    let label = label.trim();
    if label.is_empty() {
        return Err(rusqlite::Error::InvalidQuery);
    }

    if let Some(id) = find_option_id(conn, kind, label)? {
        return Ok(id);
    }

    let insert_sql = format!(
        "INSERT INTO {} ({}, is_active, sort_order, created_at)
         VALUES (?1, 1, (SELECT COALESCE(MAX(sort_order), 0) + 1 FROM {}), ?2)",
        kind.table(),
        kind.label_column(),
        kind.table()
    );
    conn.execute(&insert_sql, params![label, now_string()])?;
    Ok(conn.last_insert_rowid())
}

fn add_option(conn: &Connection, kind: OptionKind, label: &str) -> std::result::Result<(), String> {
    let label = label.trim();
    if label.is_empty() {
        return Err("配置项名称不能为空。".to_string());
    }

    let exists = find_option_id(conn, kind, label).map_err(|e| e.to_string())?;
    if exists.is_some() {
        return Err(format!("配置项“{}”已存在，请勿重复新增。", label));
    }

    ensure_option(conn, kind, label).map_err(|e| e.to_string())?;
    Ok(())
}

fn fetch_options(
    conn: &Connection,
    kind: OptionKind,
    active_only: bool,
) -> rusqlite::Result<Vec<OptionItem>> {
    let where_clause = if active_only { "WHERE is_active = 1" } else { "" };
    let sql = format!(
        "SELECT id, {} AS label, is_active, sort_order
         FROM {}
         {}
         ORDER BY is_active DESC, sort_order ASC, label ASC",
        kind.label_column(),
        kind.table(),
        where_clause
    );

    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map([], |row| {
        let is_active: i64 = row.get(2)?;
        Ok(OptionItem {
            id: row.get(0)?,
            label: row.get(1)?,
            is_active: is_active == 1,
            sort_order: row.get(3)?,
        })
    })?;

    let mut items = Vec::new();
    for item in rows {
        items.push(item?);
    }
    Ok(items)
}

fn toggle_option(conn: &Connection, kind: OptionKind, id: i64) -> rusqlite::Result<()> {
    let sql = format!(
        "UPDATE {} SET is_active = CASE WHEN is_active = 1 THEN 0 ELSE 1 END WHERE id = ?1",
        kind.table()
    );
    conn.execute(&sql, params![id])?;
    Ok(())
}

fn option_reference_count(conn: &Connection, kind: OptionKind, id: i64) -> rusqlite::Result<i64> {
    match kind {
        OptionKind::Person => conn.query_row(
            "SELECT COUNT(*) FROM weekly_logs WHERE module_owner_id = ?1 OR completer_id = ?1 OR next_owner_id = ?1",
            params![id],
            |row| row.get(0),
        ),
        OptionKind::Project => conn.query_row(
            "SELECT COUNT(*) FROM weekly_logs WHERE project_id = ?1",
            params![id],
            |row| row.get(0),
        ),
        OptionKind::Specialty => conn.query_row(
            "SELECT COUNT(*) FROM weekly_logs WHERE specialty_id = ?1",
            params![id],
            |row| row.get(0),
        ),
    }
}

fn delete_option(conn: &Connection, kind: OptionKind, id: i64) -> std::result::Result<(), String> {
    let ref_count = option_reference_count(conn, kind, id).map_err(|e| e.to_string())?;
    if ref_count > 0 {
        return Err(format!(
            "该选项已被 {} 条日志引用，不能删除。可以使用“禁用”让填报端不再显示它。",
            ref_count
        ));
    }

    let sql = format!("DELETE FROM {} WHERE id = ?1", kind.table());
    conn.execute(&sql, params![id]).map_err(|e| e.to_string())?;
    Ok(())
}

fn option_id(conn: &Connection, kind: OptionKind, label: &str) -> rusqlite::Result<i64> {
    ensure_option(conn, kind, label)
}

fn maybe_seed_sample(conn: &Connection) -> rusqlite::Result<()> {
    let should_seed = env::var("SEED_SAMPLE").unwrap_or_default() == "1";
    if !should_seed {
        return Ok(());
    }

    let log_count: i64 = conn.query_row("SELECT COUNT(*) FROM weekly_logs", [], |row| row.get(0))?;
    if log_count > 0 {
        return Ok(());
    }

    let persons = ["张三", "李四", "王二", "何仪周", "吴海玉", "王丽", "董向明"];
    let projects = ["CH-20A", "2603", "2601", "002"];
    let specialties = ["保障性", "六性", "适航性", "技术出版物设计"];

    for name in persons {
        ensure_option(conn, OptionKind::Person, name)?;
    }
    for project_no in projects {
        ensure_option(conn, OptionKind::Project, project_no)?;
    }
    for name in specialties {
        ensure_option(conn, OptionKind::Specialty, name)?;
    }

    let rows = vec![
        (6, 1, "CH-20A", "保障性", "何仪周", "手册策划", "李四", "完成", "XXX", "手册策划"),
        (6, 1, "CH-20A", "保障性", "何仪周", "模板编制", "李四", "完成", "XXX", "模板编制"),
        (6, 1, "CH-20A", "保障性", "何仪周", "下发手册", "李四", "完成", "XXX", "下发手册"),
        (6, 1, "CH-20A", "保障性", "何仪周", "手册排版", "张三", "完成", "无", "手册排版"),
        (6, 1, "CH-20A", "六性", "吴海玉", "手册编制", "王二", "未完成", "XXX", "手册编制"),
        (6, 1, "CH-20A", "六性", "吴海玉", "模板设计", "王二", "完成", "XXX", "模板设计"),
        (6, 1, "2603", "六性", "吴海玉", "手册定稿", "李四", "完成", "XXX", "手册定稿"),
        (6, 2, "2601", "适航性", "王丽", "完成方案编制", "张三", "完成", "无", "下发模板"),
        (6, 2, "002", "适航性", "王丽", "法规研制", "李四", "未完成", "XXX", "继续"),
        (7, 1, "CH-20A", "技术出版物设计", "董向明", "法规研制", "李四", "未完成", "XXX", "继续"),
    ];

    for row in rows {
        let form = WeeklyLogForm {
            meeting_month: row.0,
            meeting_count: row.1,
            project_id: option_id(conn, OptionKind::Project, row.2)?,
            specialty_id: option_id(conn, OptionKind::Specialty, row.3)?,
            module_owner_id: option_id(conn, OptionKind::Person, row.4)?,
            last_work_item: row.5.to_string(),
            completer_id: option_id(conn, OptionKind::Person, row.6)?,
            progress: row.7.to_string(),
            reason: row.8.to_string(),
            next_work_item: row.9.to_string(),
        };
        insert_log(conn, &form)?;
    }

    Ok(())
}

fn validate_meeting(month: i64, count: i64) -> std::result::Result<(), String> {
    if !(1..=12).contains(&month) {
        return Err("月份必须在 1 到 12 之间。".to_string());
    }
    if !(1..=20).contains(&count) {
        return Err("例会次数必须在 1 到 20 之间。".to_string());
    }
    Ok(())
}

fn insert_log(conn: &Connection, form: &WeeklyLogForm) -> rusqlite::Result<()> {
    let legacy_period = meeting_label(form.meeting_month, form.meeting_count);
    conn.execute(
        r#"
        INSERT INTO weekly_logs
        (start_date, end_date, meeting_month, meeting_count, project_id, specialty_id, module_owner_id, last_work_item,
         completer_id, progress, reason, next_work_item, next_owner_id, created_at)
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)
        "#,
        params![
            legacy_period,
            legacy_period,
            form.meeting_month,
            form.meeting_count,
            form.project_id,
            form.specialty_id,
            form.module_owner_id,
            form.last_work_item.trim(),
            form.completer_id,
            form.progress.trim(),
            form.reason.trim(),
            form.next_work_item.trim(),
            form.completer_id,
            now_string()
        ],
    )?;
    Ok(())
}

fn insert_logs(conn: &mut Connection, forms: &[WeeklyLogForm]) -> rusqlite::Result<()> {
    let tx = conn.transaction()?;
    let created_at = now_string();
    for form in forms {
        let legacy_period = meeting_label(form.meeting_month, form.meeting_count);
        tx.execute(
            r#"
            INSERT INTO weekly_logs
            (start_date, end_date, meeting_month, meeting_count, project_id, specialty_id, module_owner_id, last_work_item,
             completer_id, progress, reason, next_work_item, next_owner_id, created_at)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)
            "#,
            params![
                legacy_period,
                legacy_period,
                form.meeting_month,
                form.meeting_count,
                form.project_id,
                form.specialty_id,
                form.module_owner_id,
                form.last_work_item.trim(),
                form.completer_id,
                form.progress.trim(),
                form.reason.trim(),
                form.next_work_item.trim(),
                form.completer_id,
                created_at
            ],
        )?;
    }
    tx.commit()?;
    Ok(())
}

fn parse_multi_log_form(body: &[u8]) -> std::result::Result<Vec<WeeklyLogForm>, String> {
    let mut fields: HashMap<String, Vec<String>> = HashMap::new();
    for (key, value) in url::form_urlencoded::parse(body) {
        fields.entry(key.into_owned()).or_default().push(value.into_owned());
    }

    let one = |name: &str| -> std::result::Result<String, String> {
        fields
            .get(name)
            .and_then(|v| v.first())
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
            .ok_or_else(|| format!("缺少字段：{}", name))
    };
    let many = |name: &str| -> Vec<String> {
        fields.get(name).cloned().unwrap_or_default()
    };
    let parse_id = |name: &str, value: &str| -> std::result::Result<i64, String> {
        value.trim().parse::<i64>().map_err(|_| format!("字段 {} 的值无效。", name))
    };

    let meeting_month = parse_id("meeting_month", &one("meeting_month")?)?;
    let meeting_count = parse_id("meeting_count", &one("meeting_count")?)?;
    validate_meeting(meeting_month, meeting_count)?;

    let project_ids = many("project_id");
    let specialty_ids = many("specialty_id");
    let module_owner_ids = many("module_owner_id");
    let last_work_items = many("last_work_item");
    let completer_ids = many("completer_id");
    let progresses = many("progress");
    let reasons = many("reason");
    let next_work_items = many("next_work_item");

    let count = project_ids.len();
    if count == 0 {
        return Err("至少需要填写一条工作项。".to_string());
    }

    let required_lengths = [
        ("specialty_id", specialty_ids.len()),
        ("module_owner_id", module_owner_ids.len()),
        ("last_work_item", last_work_items.len()),
        ("completer_id", completer_ids.len()),
        ("progress", progresses.len()),
        ("next_work_item", next_work_items.len()),
    ];
    for (name, len) in required_lengths {
        if len != count {
            return Err(format!("工作项字段数量不一致：{}。", name));
        }
    }

    let mut forms = Vec::new();
    for i in 0..count {
        let last_work_item = last_work_items[i].trim().to_string();
        let next_work_item = next_work_items[i].trim().to_string();
        if last_work_item.is_empty() && next_work_item.is_empty() {
            continue;
        }
        if last_work_item.is_empty() {
            return Err(format!("第 {} 条工作项缺少上周工作项。", i + 1));
        }
        if next_work_item.is_empty() {
            return Err(format!("第 {} 条工作项缺少本周计划工作项。", i + 1));
        }

        forms.push(WeeklyLogForm {
            meeting_month,
            meeting_count,
            project_id: parse_id("project_id", &project_ids[i])?,
            specialty_id: parse_id("specialty_id", &specialty_ids[i])?,
            module_owner_id: parse_id("module_owner_id", &module_owner_ids[i])?,
            last_work_item,
            completer_id: parse_id("completer_id", &completer_ids[i])?,
            progress: progresses[i].trim().to_string(),
            reason: reasons.get(i).map(|v| v.trim().to_string()).unwrap_or_default(),
            next_work_item,
        });
    }

    if forms.is_empty() {
        return Err("至少需要填写一条有效工作项。".to_string());
    }
    Ok(forms)
}

fn update_log(conn: &Connection, id: i64, form: &WeeklyLogForm) -> rusqlite::Result<()> {
    conn.execute(
        r#"
        UPDATE weekly_logs
        SET start_date = ?1,
            end_date = ?2,
            meeting_month = ?3,
            meeting_count = ?4,
            project_id = ?5,
            specialty_id = ?6,
            module_owner_id = ?7,
            last_work_item = ?8,
            completer_id = ?9,
            progress = ?10,
            reason = ?11,
            next_work_item = ?12,
            next_owner_id = ?13
        WHERE id = ?14
        "#,
        params![
            meeting_label(form.meeting_month, form.meeting_count),
            meeting_label(form.meeting_month, form.meeting_count),
            form.meeting_month,
            form.meeting_count,
            form.project_id,
            form.specialty_id,
            form.module_owner_id,
            form.last_work_item.trim(),
            form.completer_id,
            form.progress.trim(),
            form.reason.trim(),
            form.next_work_item.trim(),
            form.completer_id,
            id
        ],
    )?;
    Ok(())
}

fn delete_log(conn: &Connection, id: i64) -> rusqlite::Result<()> {
    conn.execute("DELETE FROM weekly_logs WHERE id = ?1", params![id])?;
    Ok(())
}

fn fetch_all_logs(conn: &Connection) -> rusqlite::Result<Vec<WeeklyLogView>> {
    let mut stmt = conn.prepare(
        r#"
        SELECT
            l.id,
            l.meeting_month,
            l.meeting_count,
            l.project_id,
            p.project_no,
            l.specialty_id,
            s.name AS specialty,
            l.module_owner_id,
            mo.name AS module_owner,
            l.last_work_item,
            l.completer_id,
            co.name AS completer,
            l.progress,
            l.reason,
            l.next_work_item,
            l.next_owner_id,
            no.name AS next_owner,
            l.created_at
        FROM weekly_logs l
        JOIN projects p ON l.project_id = p.id
        JOIN specialties s ON l.specialty_id = s.id
        JOIN persons mo ON l.module_owner_id = mo.id
        JOIN persons co ON l.completer_id = co.id
        JOIN persons no ON l.next_owner_id = no.id
        ORDER BY l.meeting_month DESC, l.meeting_count DESC, p.project_no ASC, s.name ASC, l.id DESC
        "#,
    )?;

    let rows = stmt.query_map([], |row| {
        Ok(WeeklyLogView {
            id: row.get(0)?,
            meeting_month: row.get(1)?,
            meeting_count: row.get(2)?,
            project_id: row.get(3)?,
            project_no: row.get(4)?,
            specialty_id: row.get(5)?,
            specialty: row.get(6)?,
            module_owner_id: row.get(7)?,
            module_owner: row.get(8)?,
            last_work_item: row.get(9)?,
            completer_id: row.get(10)?,
            completer: row.get(11)?,
            progress: row.get(12)?,
            reason: row.get(13)?,
            next_work_item: row.get(14)?,
            next_owner_id: row.get(15)?,
            next_owner: row.get(16)?,
            created_at: row.get(17)?,
        })
    })?;

    let mut logs = Vec::new();
    for row in rows {
        logs.push(row?);
    }
    Ok(logs)
}

fn fetch_log(conn: &Connection, id: i64) -> rusqlite::Result<Option<WeeklyLogView>> {
    let mut stmt = conn.prepare(
        r#"
        SELECT
            l.id,
            l.meeting_month,
            l.meeting_count,
            l.project_id,
            p.project_no,
            l.specialty_id,
            s.name AS specialty,
            l.module_owner_id,
            mo.name AS module_owner,
            l.last_work_item,
            l.completer_id,
            co.name AS completer,
            l.progress,
            l.reason,
            l.next_work_item,
            l.next_owner_id,
            no.name AS next_owner,
            l.created_at
        FROM weekly_logs l
        JOIN projects p ON l.project_id = p.id
        JOIN specialties s ON l.specialty_id = s.id
        JOIN persons mo ON l.module_owner_id = mo.id
        JOIN persons co ON l.completer_id = co.id
        JOIN persons no ON l.next_owner_id = no.id
        WHERE l.id = ?1
        "#,
    )?;

    stmt.query_row(params![id], |row| {
        Ok(WeeklyLogView {
            id: row.get(0)?,
            meeting_month: row.get(1)?,
            meeting_count: row.get(2)?,
            project_id: row.get(3)?,
            project_no: row.get(4)?,
            specialty_id: row.get(5)?,
            specialty: row.get(6)?,
            module_owner_id: row.get(7)?,
            module_owner: row.get(8)?,
            last_work_item: row.get(9)?,
            completer_id: row.get(10)?,
            completer: row.get(11)?,
            progress: row.get(12)?,
            reason: row.get(13)?,
            next_work_item: row.get(14)?,
            next_owner_id: row.get(15)?,
            next_owner: row.get(16)?,
            created_at: row.get(17)?,
        })
    })
    .optional()
}

fn has_text(value: &Option<String>) -> Option<String> {
    value.as_ref().map(|v| v.trim().to_string()).filter(|v| !v.is_empty())
}

fn filter_logs(logs: Vec<WeeklyLogView>, filters: &FilterQuery) -> Vec<WeeklyLogView> {
    let progress = has_text(&filters.progress);

    logs.into_iter()
        .filter(|log| {
            if let Some(month) = filters.meeting_month {
                if month > 0 && log.meeting_month != month {
                    return false;
                }
            }
            if let Some(count) = filters.meeting_count {
                if count > 0 && log.meeting_count != count {
                    return false;
                }
            }
            if let Some(project_id) = filters.project_id {
                if project_id > 0 && log.project_id != project_id {
                    return false;
                }
            }
            if let Some(specialty_id) = filters.specialty_id {
                if specialty_id > 0 && log.specialty_id != specialty_id {
                    return false;
                }
            }
            if let Some(person_id) = filters.person_id {
                if person_id > 0
                    && log.module_owner_id != person_id
                    && log.completer_id != person_id
                {
                    return false;
                }
            }
            if let Some(progress) = &progress {
                if progress != "全部" && log.progress.as_str() != progress.as_str() {
                    return false;
                }
            }
            true
        })
        .collect()
}

fn redirect_to(target: &str) -> HttpResponse {
    HttpResponse::SeeOther()
        .append_header(("Location", target))
        .finish()
}

fn page(title: &str, _active: &str, body: String) -> HttpResponse {
    let html = format!(
        r#"<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{}</title>
  <style>
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "Microsoft YaHei", sans-serif; background: #f6f7fb; color: #222; }}
    header {{ background: #182235; color: #fff; padding: 14px 22px; display: flex; align-items: center; justify-content: space-between; gap: 16px; }}
    header h1 {{ font-size: 18px; margin: 0; font-weight: 650; }}
    header .subtitle {{ color: #dbe6ff; font-size: 13px; }}
    main {{ max-width: 1360px; margin: 22px auto; padding: 0 18px 40px; }}
    .card {{ background: #fff; border: 1px solid #e6e9f1; border-radius: 14px; padding: 18px; margin-bottom: 18px; box-shadow: 0 8px 24px rgba(18, 30, 56, .06); }}
    .muted {{ color: #667085; font-size: 13px; }}
    .grid {{ display: grid; grid-template-columns: repeat(4, minmax(160px, 1fr)); gap: 12px; }}
    .grid-3 {{ display: grid; grid-template-columns: repeat(3, minmax(180px, 1fr)); gap: 12px; }}
    .grid-2 {{ display: grid; grid-template-columns: repeat(2, minmax(260px, 1fr)); gap: 14px; }}
    label {{ display: block; font-size: 13px; color: #475467; margin-bottom: 6px; }}
    input, select, textarea {{ width: 100%; border: 1px solid #d0d5dd; border-radius: 10px; padding: 9px 10px; font: inherit; background: #fff; }}
    textarea {{ min-height: 86px; resize: vertical; line-height: 1.55; }}
    .btn {{ display: inline-flex; align-items: center; justify-content: center; border: none; border-radius: 10px; padding: 9px 14px; color: #fff; background: #2563eb; cursor: pointer; text-decoration: none; font: inherit; }}
    .btn.secondary {{ color: #344054; background: #eef2f7; }}
    .btn.danger {{ background: #dc2626; }}
    .btn.warn {{ background: #f59e0b; color: #111827; }}
    .btn.small {{ padding: 6px 10px; border-radius: 8px; font-size: 13px; }}
    .actions {{ display: flex; gap: 8px; align-items: center; flex-wrap: wrap; }}
    table {{ width: 100%; border-collapse: collapse; background: #fff; min-width: 1120px; }}
    th, td {{ border-bottom: 1px solid #eaecf0; padding: 10px 8px; text-align: left; vertical-align: top; font-size: 14px; }}
    th {{ background: #f9fafb; color: #344054; font-weight: 650; position: sticky; top: 0; }}
    .table-wrap {{ overflow-x: auto; border: 1px solid #eaecf0; border-radius: 12px; }}
    .pill {{ display: inline-block; padding: 3px 8px; border-radius: 999px; font-size: 12px; background: #eef2ff; color: #3730a3; }}
    .pill.off {{ background: #f2f4f7; color: #667085; }}
    .pill.ok {{ background: #ecfdf3; color: #067647; }}
    .pill.no {{ background: #fef3f2; color: #b42318; }}
    .status-label {{ display: inline-flex; align-items: center; gap: 7px; font-size: 13px; font-weight: 650; color: #067647; }}
    .status-label.off {{ color: #667085; }}
    .status-dot {{ width: 8px; height: 8px; border-radius: 999px; background: #12b76a; display: inline-block; }}
    .status-label.off .status-dot {{ background: #98a2b3; }}
    .section-title {{ display: flex; justify-content: space-between; align-items: flex-end; gap: 12px; margin-bottom: 14px; }}
    .section-title h2 {{ margin: 0; font-size: 20px; }}
    .work-group {{ margin-top: 14px; border-top: 1px solid #eef2f7; padding-top: 12px; }}
    .work-group h3 {{ margin: 0 0 10px; font-size: 16px; color: #0f172a; }}
    .work-item {{ padding: 10px 0; border-bottom: 1px dashed #e5e7eb; }}
    .work-item:last-child {{ border-bottom: 0; }}
    .warning {{ padding: 12px 14px; border-radius: 12px; background: #fff7ed; border: 1px solid #fed7aa; color: #9a3412; margin-bottom: 16px; }}
    .success {{ padding: 14px; border-radius: 12px; background: #ecfdf3; border: 1px solid #abefc6; color: #067647; }}
    .search-select {{ display: grid; gap: 6px; }}
    .select-search {{ border-color: #c7d2fe; background: #f8fafc; }}
    .text-block {{ white-space: pre-wrap; overflow-wrap: anywhere; word-break: break-word; line-height: 1.65; max-height: 150px; overflow-y: auto; padding-right: 4px; }}
    .summary-title {{ overflow-wrap: anywhere; word-break: break-word; line-height: 1.55; }}
    .table-text {{ white-space: pre-wrap; overflow-wrap: anywhere; word-break: break-word; line-height: 1.55; max-height: 120px; overflow-y: auto; min-width: 180px; }}
    .table-short {{ overflow-wrap: anywhere; word-break: break-word; }}
    .work-item, .work-item * {{ overflow-wrap: anywhere; word-break: break-word; }}
    .tool-card h2 {{ margin-top: 0; }}
    @media (max-width: 860px) {{ .grid, .grid-3, .grid-2 {{ grid-template-columns: 1fr; }} header {{ align-items: flex-start; flex-direction: column; }} }}
  </style>
</head>
<body>
<header>
  <h1>日志填报系统</h1>
  <div class="subtitle">局域网轻量填报与汇总</div>
</header>
<main>
{}
</main>

<script>
function normalizeForFilter(text) {{
  return (text || '').toString().trim().toLowerCase().replace(/\s+/g, '');
}}
function initialsOf(text) {{
  return normalizeForFilter(text)
    .split(/[^a-z0-9]+/i)
    .filter(Boolean)
    .map(function(part) {{ return part.charAt(0); }})
    .join('');
}}
function ensureSelectCache(select) {{
  if (select.dataset.allOptions) return;
  const options = Array.from(select.options).map(function(option, index) {{
    return {{
      value: option.value,
      text: option.textContent || '',
      key: option.getAttribute('data-key') || '',
      isPlaceholder: index === 0
    }};
  }});
  select.dataset.allOptions = JSON.stringify(options);
}}
function optionMatchesKeyword(item, keyword) {{
  if (!keyword) return true;
  const text = normalizeForFilter(item.text);
  const key = normalizeForFilter(item.key);
  const initials = initialsOf(item.text);
  return text.startsWith(keyword)
    || text.includes(keyword)
    || key.startsWith(keyword)
    || key.includes(keyword)
    || initials.startsWith(keyword);
}}
function rebuildSelectOptions(select, keyword) {{
  ensureSelectCache(select);
  const allOptions = JSON.parse(select.dataset.allOptions || '[]');
  const currentValue = select.value;
  select.innerHTML = '';
  allOptions.forEach(function(item, index) {{
    if (index !== 0 && !optionMatchesKeyword(item, keyword)) return;
    const option = document.createElement('option');
    option.value = item.value;
    option.textContent = item.text;
    if (item.key) option.setAttribute('data-key', item.key);
    select.appendChild(option);
  }});
  const hasCurrent = Array.from(select.options).some(function(option) {{ return option.value === currentValue; }});
  if (hasCurrent) {{
    select.value = currentValue;
  }} else {{
    select.selectedIndex = 0;
  }}
}}
function filterLinkedSelect(input) {{
  const box = input.closest('.search-select');
  if (!box) return;
  const select = box.querySelector('select');
  if (!select) return;
  rebuildSelectOptions(select, normalizeForFilter(input.value));
}}
document.addEventListener('DOMContentLoaded', function() {{
  document.querySelectorAll('.search-select select').forEach(ensureSelectCache);
}});
</script>
</body>
</html>"#,
        esc(title), body
    );

    HttpResponse::Ok().content_type("text/html; charset=utf-8").body(html)
}

fn render_select(name: &str, options: &[OptionItem], selected: Option<i64>, placeholder: &str) -> String {
    let mut html = format!(
        r#"<div class="search-select"><input class="select-search" type="search" placeholder="输入首字母或关键词筛选{}" oninput="filterLinkedSelect(this)"><select name="{}" required><option value="">{}</option>"#,
        esc(placeholder.trim_start_matches("请选择")),
        esc(name),
        esc(placeholder)
    );
    for item in options {
        let selected_attr = if Some(item.id) == selected { " selected" } else { "" };
        let disabled_hint = if item.is_active { "" } else { "（已禁用）" };
        html.push_str(&format!(
            r#"<option value="{}"{} data-key="{}">{}{}</option>"#,
            item.id,
            selected_attr,
            esc(&item.label),
            esc(&item.label),
            disabled_hint
        ));
    }
    html.push_str("</select></div>");
    html
}

fn render_filter_select(name: &str, options: &[OptionItem], selected: Option<i64>, all_label: &str) -> String {
    let mut html = format!(
        r#"<select name="{}"><option value="0">{}</option>"#,
        esc(name),
        esc(all_label)
    );
    for item in options {
        let selected_attr = if Some(item.id) == selected { " selected" } else { "" };
        let disabled_hint = if item.is_active { "" } else { "（已禁用）" };
        html.push_str(&format!(
            r#"<option value="{}"{}>{}{}</option>"#,
            item.id,
            selected_attr,
            esc(&item.label),
            disabled_hint
        ));
    }
    html.push_str("</select>");
    html
}

fn month_select(name: &str, selected: Option<i64>, include_all: bool) -> String {
    let mut html = format!(r#"<select name="{}"{}>"#, esc(name), if include_all { "" } else { " required" });
    if include_all {
        html.push_str(r#"<option value="0">全部月份</option>"#);
    } else {
        html.push_str(r#"<option value="">请选择月份</option>"#);
    }
    for month in 1..=12 {
        let selected_attr = if Some(month) == selected { " selected" } else { "" };
        html.push_str(&format!(r#"<option value="{}"{}>{}月</option>"#, month, selected_attr, month));
    }
    html.push_str("</select>");
    html
}

fn meeting_count_select(name: &str, selected: Option<i64>, include_all: bool) -> String {
    let mut html = format!(r#"<select name="{}"{}>"#, esc(name), if include_all { "" } else { " required" });
    if include_all {
        html.push_str(r#"<option value="0">全部次数</option>"#);
    } else {
        html.push_str(r#"<option value="">请选择例会次数</option>"#);
    }
    for count in 1..=20 {
        let selected_attr = if Some(count) == selected { " selected" } else { "" };
        html.push_str(&format!(r#"<option value="{}"{}>第{}次例会</option>"#, count, selected_attr, count));
    }
    html.push_str("</select>");
    html
}

fn progress_select(selected: Option<&str>) -> String {
    let choices = ["完成", "进行中", "未完成", "暂停"];
    let mut html = String::from(r#"<select name="progress" required><option value="">请选择完成进度</option>"#);
    for choice in choices {
        let selected_attr = if selected == Some(choice) { " selected" } else { "" };
        html.push_str(&format!(
            r#"<option value="{}"{}>{}</option>"#,
            esc(choice), selected_attr, esc(choice)
        ));
    }
    html.push_str("</select>");
    html
}

fn progress_filter_select(selected: Option<&str>) -> String {
    let choices = ["全部", "完成", "进行中", "未完成", "暂停"];
    let mut html = String::from(r#"<select name="progress">"#);
    for choice in choices {
        let selected_attr = if selected == Some(choice) || (selected.is_none() && choice == "全部") {
            " selected"
        } else {
            ""
        };
        html.push_str(&format!(
            r#"<option value="{}"{}>{}</option>"#,
            esc(choice), selected_attr, esc(choice)
        ));
    }
    html.push_str("</select>");
    html
}

fn render_log_form(
    action: &str,
    title: &str,
    log: Option<&WeeklyLogView>,
    projects: &[OptionItem],
    specialties: &[OptionItem],
    persons: &[OptionItem],
    submit_label: &str,
    show_admin_links: bool,
) -> String {
    let meeting_month = log.map(|l| l.meeting_month);
    let meeting_count = log.map(|l| l.meeting_count);
    let project_id = log.map(|l| l.project_id);
    let specialty_id = log.map(|l| l.specialty_id);
    let module_owner_id = log.map(|l| l.module_owner_id);
    let completer_id = log.map(|l| l.completer_id);
    let last_work_item = log.map(|l| l.last_work_item.as_str()).unwrap_or("");
    let progress = log.map(|l| l.progress.as_str());
    let reason = log.map(|l| l.reason.as_str()).unwrap_or("");
    let next_work_item = log.map(|l| l.next_work_item.as_str()).unwrap_or("");

    let mut warning = String::new();
    if projects.is_empty() || specialties.is_empty() || persons.is_empty() {
        warning.push_str(
            r#"<div class="warning">填报前需要先维护人员名单、项目号和专业方向。当前至少有一类选项为空，表单可能无法提交。</div>"#,
        );
    }

    let admin_option_link = if show_admin_links {
        r#"<a class="btn secondary" href="/summary/options">去维护选项</a>"#
    } else {
        ""
    };
    let admin_back_link = if show_admin_links {
        r#"<a class="btn secondary" href="/summary">返回汇总端</a>"#
    } else {
        ""
    };

    format!(
        r#"
<div class="section-title">
  <div>
    <h2>{}</h2>
    <div class="muted">项目号、专业方向、人员只能从预置选项中选择，不能自由填写。</div>
  </div>
  {}
</div>
{}
<form class="card" method="post" action="{}">
  <div class="grid">
    <div>
      <label>月份</label>
      {}
    </div>
    <div>
      <label>例会次数</label>
      {}
    </div>
    <div>
      <label>项目号</label>
      {}
    </div>
    <div>
      <label>专业方向</label>
      {}
    </div>
    <div>
      <label>工作模块负责人</label>
      {}
    </div>
    <div>
      <label>工作项填报人</label>
      {}
    </div>
    <div>
      <label>完成进度</label>
      {}
    </div>
  </div>

  <div class="grid-3" style="margin-top:14px;">
    <div>
      <label>上周工作项</label>
      <textarea name="last_work_item" required>{}</textarea>
    </div>
    <div>
      <label>原因</label>
      <textarea name="reason">{}</textarea>
    </div>
    <div>
      <label>本周计划工作项</label>
      <textarea name="next_work_item" required>{}</textarea>
    </div>
  </div>

  <div class="actions" style="margin-top:16px;">
    <button class="btn" type="submit">{}</button>
    {}
  </div>
</form>
"#,
        esc(title),
        admin_option_link,
        warning,
        esc(action),
        month_select("meeting_month", meeting_month, false),
        meeting_count_select("meeting_count", meeting_count, false),
        render_select("project_id", projects, project_id, "请选择项目号"),
        render_select("specialty_id", specialties, specialty_id, "请选择专业方向"),
        render_select("module_owner_id", persons, module_owner_id, "请选择工作模块负责人"),
        render_select("completer_id", persons, completer_id, "请选择工作项填报人"),
        progress_select(progress),
        esc(last_work_item),
        esc(reason),
        esc(next_work_item),
        esc(submit_label),
        admin_back_link
    )
}

fn render_fill_work_item(projects: &[OptionItem], specialties: &[OptionItem], persons: &[OptionItem]) -> String {
    format!(
        r#"
<div class="work-form-item card" style="margin-bottom:14px;">
  <div class="section-title">
    <div>
      <h3 class="work-form-title" style="margin:0;font-size:17px;">工作项 1</h3>
      <div class="muted">每条工作项会保存成一条汇总记录。</div>
    </div>
    <button class="btn danger small" type="button" onclick="removeWorkItem(this)">删除本条</button>
  </div>
  <div class="grid">
    <div>
      <label>项目号</label>
      {}
    </div>
    <div>
      <label>专业方向</label>
      {}
    </div>
    <div>
      <label>工作模块负责人</label>
      {}
    </div>
    <div>
      <label>工作项填报人</label>
      {}
    </div>
    <div>
      <label>完成进度</label>
      {}
    </div>
  </div>
  <div class="grid-3" style="margin-top:14px;">
    <div>
      <label>上周工作项</label>
      <textarea name="last_work_item" required></textarea>
    </div>
    <div>
      <label>原因</label>
      <textarea name="reason"></textarea>
    </div>
    <div>
      <label>本周计划工作项</label>
      <textarea name="next_work_item" required></textarea>
    </div>
  </div>
</div>
"#,
        render_select("project_id", projects, None, "请选择项目号"),
        render_select("specialty_id", specialties, None, "请选择专业方向"),
        render_select("module_owner_id", persons, None, "请选择工作模块负责人"),
        render_select("completer_id", persons, None, "请选择工作项填报人"),
        progress_select(None),
    )
}

fn render_fill_form(
    projects: &[OptionItem],
    specialties: &[OptionItem],
    persons: &[OptionItem],
    error: Option<&str>,
) -> String {
    let mut warning = String::new();
    if projects.is_empty() || specialties.is_empty() || persons.is_empty() {
        warning.push_str(
            r#"<div class="warning">填报前需要先维护人员名单、项目号和专业方向。当前至少有一类选项为空，表单可能无法提交。</div>"#,
        );
    }
    if let Some(msg) = error {
        warning.push_str(&format!(r#"<div class="warning">{}</div>"#, esc(msg)));
    }

    format!(
        r#"
<div class="section-title">
  <div>
    <h2>日志填报</h2>
    <div class="muted">一次可以提交多条工作项。请选择“月份”和“第几次例会”，再填写工作项列表。</div>
  </div>
</div>
{}
<form method="post" action="/fill/new" id="fill-form">
  <section class="card">
    <div class="grid">
      <div>
        <label>月份</label>
        {}
      </div>
      <div>
        <label>例会次数</label>
        {}
      </div>
    </div>
  </section>

  <section>
    <div class="section-title">
      <div>
        <h2>工作项列表</h2>
        <div class="muted">点击“新增工作项”可以继续添加；每条工作项可单独删除。</div>
      </div>
      <button class="btn secondary" type="button" onclick="addWorkItem()">新增工作项</button>
    </div>
    <div id="work-items">
      {}
    </div>
  </section>

  <section class="card">
    <div class="actions">
      <button class="btn" type="submit">提交全部工作项</button>
      <button class="btn secondary" type="button" onclick="addWorkItem()">继续新增工作项</button>
    </div>
  </section>
</form>
<script>
function resetField(el) {{
  if (el.tagName === 'SELECT') {{ rebuildSelectOptions(el, ''); el.selectedIndex = 0; return; }}
  if (el.tagName === 'TEXTAREA') {{ el.value = ''; return; }}
  if (el.tagName === 'INPUT' && el.type !== 'date') {{ el.value = ''; }}
}}
function refreshWorkItemNumbers() {{
  const items = document.querySelectorAll('.work-form-item');
  items.forEach((item, index) => {{
    const title = item.querySelector('.work-form-title');
    if (title) title.textContent = '工作项 ' + (index + 1);
  }});
}}
function addWorkItem() {{
  const container = document.getElementById('work-items');
  const first = container.querySelector('.work-form-item');
  if (!first) return;
  const clone = first.cloneNode(true);
  clone.querySelectorAll('input, textarea, select').forEach(resetField);
  container.appendChild(clone);
  refreshWorkItemNumbers();
}}
function removeWorkItem(btn) {{
  const items = document.querySelectorAll('.work-form-item');
  if (items.length <= 1) {{
    alert('至少保留一条工作项。');
    return;
  }}
  btn.closest('.work-form-item').remove();
  refreshWorkItemNumbers();
}}
</script>
"#,
        warning,
        month_select("meeting_month", None, false),
        meeting_count_select("meeting_count", None, false),
        render_fill_work_item(projects, specialties, persons)
    )
}

fn render_filter(
    filters: &FilterQuery,
    projects: &[OptionItem],
    specialties: &[OptionItem],
    persons: &[OptionItem],
) -> String {
    let progress = filters.progress.as_deref();

    format!(
        r#"
<form class="card" method="get" action="/summary">
  <div class="section-title">
    <div>
      <h2>日志筛选</h2>
      <div class="muted">本页只显示完整的每条填报日志和筛选框；禁用项仍可用于历史汇总筛选。</div>
    </div>
    <div class="actions">
      <a class="btn secondary" href="/summary">清空筛选</a>
      <button class="btn" type="submit">查询</button>
    </div>
  </div>
  <div class="grid">
    <div><label>月份</label>{}</div>
    <div><label>例会次数</label>{}</div>
    <div><label>项目号</label>{}</div>
    <div><label>专业方向</label>{}</div>
    <div><label>人员</label>{}</div>
    <div><label>完成进度</label>{}</div>
  </div>
</form>
"#,
        month_select("meeting_month", filters.meeting_month, true),
        meeting_count_select("meeting_count", filters.meeting_count, true),
        render_filter_select("project_id", projects, filters.project_id, "全部项目"),
        render_filter_select("specialty_id", specialties, filters.specialty_id, "全部专业方向"),
        render_filter_select("person_id", persons, filters.person_id, "全部人员"),
        progress_filter_select(progress)
    )
}

fn render_summary_tools(projects: &[OptionItem]) -> String {
    format!(
        r#"
<section class="grid-2">
  <div class="card tool-card">
    <h2>日志汇总</h2>
    <p class="muted">选择某次例会和某个项目，进入独立页面生成“上周工作项汇总 / 本周计划工作项汇总”。</p>
    <form method="get" action="/summary/project-report">
      <div class="grid-3">
        <div><label>月份</label>{}</div>
        <div><label>例会次数</label>{}</div>
        <div><label>项目号</label>{}</div>
      </div>
      <div class="actions" style="margin-top:14px;"><button class="btn" type="submit">生成项目日志汇总</button></div>
    </form>
  </div>
  <div class="card tool-card">
    <h2>填报统计</h2>
    <p class="muted">选择某次例会，进入独立页面查看完整人员名单及是否已填报。</p>
    <form method="get" action="/summary/attendance">
      <div class="grid-3">
        <div><label>月份</label>{}</div>
        <div><label>例会次数</label>{}</div>
      </div>
      <div class="actions" style="margin-top:14px;"><button class="btn" type="submit">查看填报统计</button></div>
    </form>
  </div>
</section>
"#,
        month_select("meeting_month", None, false),
        meeting_count_select("meeting_count", None, false),
        render_filter_select("project_id", projects, None, "请选择项目"),
        month_select("meeting_month", None, false),
        meeting_count_select("meeting_count", None, false),
    )
}

fn render_work_panels(logs: &[WeeklyLogView]) -> String {
    let mut by_specialty: BTreeMap<String, Vec<&WeeklyLogView>> = BTreeMap::new();
    for log in logs {
        by_specialty.entry(log.specialty.clone()).or_default().push(log);
    }

    let mut last_html = String::new();
    let mut next_html = String::new();

    for (specialty, items) in by_specialty {
        last_html.push_str(&format!(r#"<div class="work-group"><h3>{}</h3>"#, esc(&specialty)));
        next_html.push_str(&format!(r#"<div class="work-group"><h3>{}</h3>"#, esc(&specialty)));

        for log in items {
            last_html.push_str(&format!(
                r#"<div class="work-item">
  <div class="summary-title"><span class="pill">{}</span></div>
  <div class="muted" style="margin-top:6px;">项目号：{}｜工作项填报人：{}｜工作模块负责人：{}</div>
  <div class="text-block" style="margin-top:8px;"><strong>上周工作：</strong>{}</div>
  <div class="text-block muted" style="margin-top:6px;"><strong>原因：</strong>{}</div>
</div>"#,
                esc(&log.progress),
                esc(&log.project_no),
                esc(&log.completer),
                esc(&log.module_owner),
                esc(&log.last_work_item),
                esc(&log.reason)
            ));

            next_html.push_str(&format!(
                r#"<div class="work-item">
  <div class="muted">项目号：{}｜来源填报人：{}</div>
  <div class="text-block" style="margin-top:8px;"><strong>本周计划工作：</strong>{}</div>
</div>"#,
                esc(&log.project_no),
                esc(&log.completer),
                esc(&log.next_work_item)
            ));
        }

        last_html.push_str("</div>");
        next_html.push_str("</div>");
    }

    if logs.is_empty() {
        last_html.push_str(r#"<div class="muted">暂无记录。</div>"#);
        next_html.push_str(r#"<div class="muted">暂无记录。</div>"#);
    }

    format!(
        r#"
<div class="grid-2">
  <section class="card">
    <div class="section-title"><h2>上周工作项汇总</h2></div>
    {}
  </section>
  <section class="card">
    <div class="section-title"><h2>本周计划工作项汇总</h2></div>
    {}
  </section>
</div>
"#,
        last_html, next_html
    )
}

fn render_project_report_form(query: &ProjectReportQuery, projects: &[OptionItem]) -> String {
    format!(
        r#"
<section class="card">
  <div class="section-title">
    <div>
      <h2>项目日志汇总</h2>
      <div class="muted">选择某次例会和某个项目，生成上周工作项汇总和本周计划工作项汇总。</div>
    </div>
    <div class="actions"><a class="btn secondary" href="/summary">返回日志明细</a></div>
  </div>
  <form method="get" action="/summary/project-report">
    <div class="grid">
      <div><label>月份</label>{}</div>
      <div><label>例会次数</label>{}</div>
      <div><label>项目号</label>{}</div>
      <div style="align-self:end;"><button class="btn" type="submit">生成汇总</button></div>
    </div>
  </form>
</section>
"#,
        month_select("meeting_month", query.meeting_month, false),
        meeting_count_select("meeting_count", query.meeting_count, false),
        render_filter_select("project_id", projects, query.project_id, "请选择项目"),
    )
}

fn render_attendance_form(query: &AttendanceQuery) -> String {
    format!(
        r#"
<section class="card">
  <div class="section-title">
    <div>
      <h2>填报统计</h2>
      <div class="muted">选择某次例会后，系统会列出完整人员名单，并判断该人员是否已有填报记录。</div>
    </div>
    <div class="actions"><a class="btn secondary" href="/summary">返回日志明细</a></div>
  </div>
  <form method="get" action="/summary/attendance">
    <div class="grid">
      <div><label>月份</label>{}</div>
      <div><label>例会次数</label>{}</div>
      <div style="align-self:end;"><button class="btn" type="submit">生成统计表</button></div>
    </div>
  </form>
</section>
"#,
        month_select("meeting_month", query.meeting_month, false),
        meeting_count_select("meeting_count", query.meeting_count, false),
    )
}

fn render_attendance_table(persons: &[OptionItem], logs: &[WeeklyLogView], month: i64, count: i64) -> String {
    let mut rows = String::new();
    for person in persons {
        let person_logs: Vec<&WeeklyLogView> = logs
            .iter()
            .filter(|log| log.meeting_month == month && log.meeting_count == count && log.completer_id == person.id)
            .collect();
        let status = if person_logs.is_empty() {
            r#"<span class="pill no">未填报</span>"#.to_string()
        } else {
            r#"<span class="pill ok">已填报</span>"#.to_string()
        };
        let projects: BTreeSet<String> = person_logs.iter().map(|l| l.project_no.clone()).collect();
        let specialties: BTreeSet<String> = person_logs.iter().map(|l| l.specialty.clone()).collect();
        let latest = person_logs
            .iter()
            .map(|l| l.created_at.as_str())
            .max()
            .unwrap_or("-");
        let active_hint = if person.is_active { "" } else { "（已禁用）" };
        rows.push_str(&format!(
            r#"<tr>
<td>{}{}</td>
<td>{}</td>
<td>{}</td>
<td>{}</td>
<td>{}</td>
<td>{}</td>
</tr>"#,
            esc(&person.label),
            active_hint,
            status,
            person_logs.len(),
            esc(&projects.into_iter().collect::<Vec<_>>().join("、")),
            esc(&specialties.into_iter().collect::<Vec<_>>().join("、")),
            esc(latest)
        ));
    }

    if rows.is_empty() {
        rows.push_str(r#"<tr><td colspan="6" class="muted">暂无人员名单，请先到选项维护中新增人员。</td></tr>"#);
    }

    format!(
        r#"
<section class="card">
  <div class="section-title">
    <div>
      <h2>{} 填报统计表</h2>
      <div class="muted">按“工作项填报人”判断是否有该次例会的填报记录。</div>
    </div>
  </div>
  <div class="table-wrap">
    <table>
      <thead><tr><th>人员</th><th>状态</th><th>填报条数</th><th>涉及项目</th><th>专业方向</th><th>最近提交时间</th></tr></thead>
      <tbody>{}</tbody>
    </table>
  </div>
</section>
"#,
        esc(&meeting_label(month, count)),
        rows
    )
}

fn render_table(logs: &[WeeklyLogView]) -> String {
    let mut rows = String::new();
    for log in logs {
        rows.push_str(&format!(
            r#"<tr>
<td class="table-short">{}</td>
<td class="table-short">{}</td>
<td class="table-short">{}</td>
<td class="table-short">{}</td>
<td><div class="table-text">{}</div></td>
<td class="table-short">{}</td>
<td><span class="pill">{}</span></td>
<td><div class="table-text">{}</div></td>
<td><div class="table-text">{}</div></td>
<td class="table-short">{}</td>
<td>
  <div class="actions">
    <a class="btn secondary small" href="/summary/logs/edit/{}">编辑</a>
    <form method="post" action="/summary/logs/delete/{}" onsubmit="return confirm('确认删除这条记录？');">
      <button class="btn danger small" type="submit">删除</button>
    </form>
  </div>
</td>
</tr>"#,
            esc(&meeting_label(log.meeting_month, log.meeting_count)),
            esc(&log.project_no),
            esc(&log.specialty),
            esc(&log.module_owner),
            esc(&log.last_work_item),
            esc(&log.completer),
            esc(&log.progress),
            esc(&log.reason),
            esc(&log.next_work_item),
            esc(&log.created_at),
            log.id,
            log.id
        ));
    }

    if rows.is_empty() {
        rows.push_str(r#"<tr><td colspan="11" class="muted">暂无记录。</td></tr>"#);
    }

    format!(
        r#"
<section class="card">
  <div class="section-title">
    <div>
      <h2>日志明细</h2>
      <div class="muted">完整展示每条填报日志。较长工作描述会在单元格内自动换行，并限制高度。</div>
    </div>
  </div>
  <div class="table-wrap">
    <table>
      <thead>
        <tr>
          <th>例会</th>
          <th>项目号</th>
          <th>专业方向</th>
          <th>工作模块负责人</th>
          <th>上周工作项</th>
          <th>工作项填报人</th>
          <th>完成进度</th>
          <th>原因</th>
          <th>本周计划工作项</th>
          <th>创建时间</th>
          <th>操作</th>
        </tr>
      </thead>
      <tbody>{}</tbody>
    </table>
  </div>
</section>
"#,
        rows
    )
}

fn render_options_home() -> String {
    r#"
<div class="section-title">
  <div>
    <h2>选项维护</h2>
    <div class="muted">汇总端维护基础选项，填报端只能从这些选项中选择。</div>
  </div>
  <a class="btn secondary" href="/summary">返回汇总端</a>
</div>
<div class="grid-3">
  <a class="card" href="/summary/options/persons" style="text-decoration:none;color:inherit;">
    <h2>人员名单</h2>
    <p class="muted">维护工作模块负责人、工作项填报人可选项。</p>
  </a>
  <a class="card" href="/summary/options/projects" style="text-decoration:none;color:inherit;">
    <h2>项目号</h2>
    <p class="muted">维护填报端可选择的项目号。</p>
  </a>
  <a class="card" href="/summary/options/specialties" style="text-decoration:none;color:inherit;">
    <h2>专业方向</h2>
    <p class="muted">维护填报端可选择的专业方向。</p>
  </a>
</div>
"#
    .to_string()
}

fn render_option_admin(kind: OptionKind, items: &[OptionItem], notice: Option<&str>, notice_is_error: bool) -> String {
    let mut rows = String::new();
    for item in items {
        let status = if item.is_active {
            r#"<span class="status-label"><span class="status-dot"></span>启用中</span>"#
        } else {
            r#"<span class="status-label off"><span class="status-dot"></span>已禁用</span>"#
        };
        let button_text = if item.is_active { "设为禁用" } else { "重新启用" };
        let button_class = if item.is_active { "btn secondary small" } else { "btn small" };
        rows.push_str(&format!(
            r#"<tr>
<td>{}</td>
<td>{}</td>
<td>
  <div class="actions">
    <form method="post" action="{}/{}/toggle">
      <button class="{}" type="submit">{}</button>
    </form>
    <form method="post" action="{}/{}/delete" onsubmit="return confirm('确认删除这个选项？如果已被日志引用，系统会阻止删除。');">
      <button class="btn danger small" type="submit">删除</button>
    </form>
  </div>
</td>
</tr>"#,
            esc(&item.label),
            status,
            kind.path(),
            item.id,
            button_class,
            button_text,
            kind.path(),
            item.id
        ));
    }

    if rows.is_empty() {
        rows.push_str(r#"<tr><td colspan="3" class="muted">暂无选项。</td></tr>"#);
    }

    let notice_html = match notice {
        Some(msg) if notice_is_error => format!(r#"<div class="warning">{}</div>"#, esc(msg)),
        Some(msg) => format!(r#"<div class="success">{}</div>"#, esc(msg)),
        None => String::new(),
    };

    format!(
        r#"
<div class="section-title">
  <div>
    <h2>{}</h2>
    <div class="muted">建议禁用不再使用的选项，不要物理删除，这样历史日志仍能正常展示。</div>
  </div>
  <a class="btn secondary" href="/summary/options">返回选项维护</a>
</div>
{}
<form class="card" method="post" action="{}">
  <div class="grid-3">
    <div>
      <label>{}</label>
      <input name="name" placeholder="请输入{}" required>
    </div>
    <div style="align-self:end;">
      <button class="btn" type="submit">新增配置项</button>
    </div>
  </div>
</form>
<section class="card">
  <div class="table-wrap">
    <table>
      <thead><tr><th>名称</th><th>状态</th><th>操作</th></tr></thead>
      <tbody>{}</tbody>
    </table>
  </div>
</section>
"#,
        kind.title(),
        notice_html,
        kind.path(),
        kind.input_label(),
        kind.input_label(),
        rows
    )
}

#[get("/")]
async fn root() -> Result<HttpResponse> {
    Ok(redirect_to("/summary"))
}

#[get("/fill")]
async fn fill_root() -> Result<HttpResponse> {
    Ok(redirect_to("/fill/new"))
}

fn load_active_options(state: &Data<AppState>) -> Result<(Vec<OptionItem>, Vec<OptionItem>, Vec<OptionItem>)> {
    let conn = state
        .conn
        .lock()
        .map_err(|e| ErrorInternalServerError(e.to_string()))?;
    Ok((
        fetch_options(&conn, OptionKind::Project, true).map_err(ErrorInternalServerError)?,
        fetch_options(&conn, OptionKind::Specialty, true).map_err(ErrorInternalServerError)?,
        fetch_options(&conn, OptionKind::Person, true).map_err(ErrorInternalServerError)?,
    ))
}

#[get("/fill/new")]
async fn fill_new_page(state: Data<AppState>) -> Result<HttpResponse> {
    let (projects, specialties, persons) = load_active_options(&state)?;
    let body = render_fill_form(&projects, &specialties, &persons, None);
    Ok(page("日志填报", "fill", body))
}

#[post("/fill/new")]
async fn fill_create(state: Data<AppState>, body: Bytes) -> Result<HttpResponse> {
    let forms = match parse_multi_log_form(&body) {
        Ok(forms) => forms,
        Err(msg) => {
            let (projects, specialties, persons) = load_active_options(&state)?;
            return Ok(page(
                "填报错误",
                "fill",
                render_fill_form(&projects, &specialties, &persons, Some(&msg)),
            ));
        }
    };

    let mut conn = state
        .conn
        .lock()
        .map_err(|e| ErrorInternalServerError(e.to_string()))?;
    insert_logs(&mut conn, &forms).map_err(ErrorInternalServerError)?;
    Ok(redirect_to("/fill/success"))
}

#[get("/fill/success")]
async fn fill_success() -> Result<HttpResponse> {
    let body = r#"
<div class="card success">
  <h2>提交成功</h2>
  <p>本次提交的工作项已经保存到 SQLite 数据库。</p>
  <div class="actions">
    <a class="btn" href="/fill/new">继续填报</a>
  </div>
</div>
"#
    .to_string();
    Ok(page("提交成功", "fill", body))
}

#[get("/summary")]
async fn summary_page(state: Data<AppState>, query: Query<FilterQuery>) -> Result<HttpResponse> {
    let filters = query.into_inner();
    let (logs, projects, specialties, persons) = {
        let conn = state
            .conn
            .lock()
            .map_err(|e| ErrorInternalServerError(e.to_string()))?;
        (
            fetch_all_logs(&conn).map_err(ErrorInternalServerError)?,
            fetch_options(&conn, OptionKind::Project, false).map_err(ErrorInternalServerError)?,
            fetch_options(&conn, OptionKind::Specialty, false).map_err(ErrorInternalServerError)?,
            fetch_options(&conn, OptionKind::Person, false).map_err(ErrorInternalServerError)?,
        )
    };

    let filtered = filter_logs(logs, &filters);
    let mut body = String::new();
    body.push_str(&render_summary_tools(&projects));
    body.push_str(&render_filter(&filters, &projects, &specialties, &persons));
    body.push_str(&render_table(&filtered));
    Ok(page("汇总端", "summary", body))
}

#[get("/summary/project-report")]
async fn project_report_page(state: Data<AppState>, query: Query<ProjectReportQuery>) -> Result<HttpResponse> {
    let query = query.into_inner();
    let (logs, projects) = {
        let conn = state
            .conn
            .lock()
            .map_err(|e| ErrorInternalServerError(e.to_string()))?;
        (
            fetch_all_logs(&conn).map_err(ErrorInternalServerError)?,
            fetch_options(&conn, OptionKind::Project, false).map_err(ErrorInternalServerError)?,
        )
    };

    let mut body = render_project_report_form(&query, &projects);
    if let (Some(month), Some(count), Some(project_id)) = (query.meeting_month, query.meeting_count, query.project_id) {
        if month > 0 && count > 0 && project_id > 0 {
            let filtered: Vec<WeeklyLogView> = logs
                .into_iter()
                .filter(|log| log.meeting_month == month && log.meeting_count == count && log.project_id == project_id)
                .collect();
            let project_name = projects
                .iter()
                .find(|p| p.id == project_id)
                .map(|p| p.label.clone())
                .unwrap_or_else(|| "所选项目".to_string());
            body.push_str(&format!(
                r#"<section class="card"><h2>{} - {}</h2><div class="muted">以下为该项目在该次例会中的上周/本周计划工作项汇总。</div></section>"#,
                esc(&project_name),
                esc(&meeting_label(month, count))
            ));
            body.push_str(&render_work_panels(&filtered));
        }
    }

    Ok(page("项目日志汇总", "summary", body))
}

#[get("/summary/attendance")]
async fn attendance_page(state: Data<AppState>, query: Query<AttendanceQuery>) -> Result<HttpResponse> {
    let query = query.into_inner();
    let (logs, persons) = {
        let conn = state
            .conn
            .lock()
            .map_err(|e| ErrorInternalServerError(e.to_string()))?;
        (
            fetch_all_logs(&conn).map_err(ErrorInternalServerError)?,
            fetch_options(&conn, OptionKind::Person, false).map_err(ErrorInternalServerError)?,
        )
    };

    let mut body = render_attendance_form(&query);
    if let (Some(month), Some(count)) = (query.meeting_month, query.meeting_count) {
        if month > 0 && count > 0 {
            body.push_str(&render_attendance_table(&persons, &logs, month, count));
        }
    }
    Ok(page("填报统计", "summary", body))
}

#[get("/summary/logs/edit/{id}")]
async fn edit_log_page(state: Data<AppState>, path: Path<i64>) -> Result<HttpResponse> {
    let id = path.into_inner();
    let (log, projects, specialties, persons) = {
        let conn = state
            .conn
            .lock()
            .map_err(|e| ErrorInternalServerError(e.to_string()))?;
        (
            fetch_log(&conn, id).map_err(ErrorInternalServerError)?,
            fetch_options(&conn, OptionKind::Project, false).map_err(ErrorInternalServerError)?,
            fetch_options(&conn, OptionKind::Specialty, false).map_err(ErrorInternalServerError)?,
            fetch_options(&conn, OptionKind::Person, false).map_err(ErrorInternalServerError)?,
        )
    };

    let body = match log {
        Some(log) => render_log_form(
            &format!("/summary/logs/edit/{}", id),
            "编辑日志",
            Some(&log),
            &projects,
            &specialties,
            &persons,
            "保存修改",
            true,
        ),
        None => r#"<div class="card warning">记录不存在。</div>"#.to_string(),
    };
    Ok(page("编辑日志", "summary", body))
}

#[post("/summary/logs/edit/{id}")]
async fn update_log_handler(
    state: Data<AppState>,
    path: Path<i64>,
    form: Form<WeeklyLogForm>,
) -> Result<HttpResponse> {
    let id = path.into_inner();
    validate_meeting(form.meeting_month, form.meeting_count).map_err(ErrorInternalServerError)?;
    let conn = state
        .conn
        .lock()
        .map_err(|e| ErrorInternalServerError(e.to_string()))?;
    update_log(&conn, id, &form).map_err(ErrorInternalServerError)?;
    Ok(redirect_to("/summary"))
}

#[post("/summary/logs/delete/{id}")]
async fn delete_log_handler(state: Data<AppState>, path: Path<i64>) -> Result<HttpResponse> {
    let id = path.into_inner();
    let conn = state
        .conn
        .lock()
        .map_err(|e| ErrorInternalServerError(e.to_string()))?;
    delete_log(&conn, id).map_err(ErrorInternalServerError)?;
    Ok(redirect_to("/summary"))
}

#[get("/summary/options")]
async fn options_home() -> Result<HttpResponse> {
    Ok(page("选项维护", "options", render_options_home()))
}

fn render_option_page(state: &Data<AppState>, kind: OptionKind) -> Result<HttpResponse> {
    render_option_page_with_notice(state, kind, None, false)
}

fn render_option_page_with_notice(
    state: &Data<AppState>,
    kind: OptionKind,
    notice: Option<&str>,
    notice_is_error: bool,
) -> Result<HttpResponse> {
    let items = {
        let conn = state
            .conn
            .lock()
            .map_err(|e| ErrorInternalServerError(e.to_string()))?;
        fetch_options(&conn, kind, false).map_err(ErrorInternalServerError)?
    };
    Ok(page(
        kind.title(),
        "options",
        render_option_admin(kind, &items, notice, notice_is_error),
    ))
}

fn add_option_handler(state: &Data<AppState>, kind: OptionKind, form: &OptionForm) -> Result<HttpResponse> {
    let add_result = {
        let conn = state
            .conn
            .lock()
            .map_err(|e| ErrorInternalServerError(e.to_string()))?;
        add_option(&conn, kind, &form.name)
    };

    match add_result {
        Ok(()) => Ok(redirect_to(kind.path())),
        Err(msg) => render_option_page_with_notice(state, kind, Some(&msg), true),
    }
}

fn toggle_option_handler(state: &Data<AppState>, kind: OptionKind, id: i64) -> Result<HttpResponse> {
    let conn = state
        .conn
        .lock()
        .map_err(|e| ErrorInternalServerError(e.to_string()))?;
    toggle_option(&conn, kind, id).map_err(ErrorInternalServerError)?;
    Ok(redirect_to(kind.path()))
}

fn delete_option_handler(state: &Data<AppState>, kind: OptionKind, id: i64) -> Result<HttpResponse> {
    let delete_result = {
        let conn = state
            .conn
            .lock()
            .map_err(|e| ErrorInternalServerError(e.to_string()))?;
        delete_option(&conn, kind, id)
    };

    match delete_result {
        Ok(()) => Ok(redirect_to(kind.path())),
        Err(msg) => render_option_page_with_notice(state, kind, Some(&msg), true),
    }
}

#[get("/summary/options/persons")]
async fn persons_page(state: Data<AppState>) -> Result<HttpResponse> {
    render_option_page(&state, OptionKind::Person)
}

#[post("/summary/options/persons")]
async fn persons_add(state: Data<AppState>, form: Form<OptionForm>) -> Result<HttpResponse> {
    add_option_handler(&state, OptionKind::Person, &form)
}

#[post("/summary/options/persons/{id}/toggle")]
async fn persons_toggle(state: Data<AppState>, path: Path<i64>) -> Result<HttpResponse> {
    toggle_option_handler(&state, OptionKind::Person, path.into_inner())
}

#[post("/summary/options/persons/{id}/delete")]
async fn persons_delete(state: Data<AppState>, path: Path<i64>) -> Result<HttpResponse> {
    delete_option_handler(&state, OptionKind::Person, path.into_inner())
}

#[get("/summary/options/projects")]
async fn projects_page(state: Data<AppState>) -> Result<HttpResponse> {
    render_option_page(&state, OptionKind::Project)
}

#[post("/summary/options/projects")]
async fn projects_add(state: Data<AppState>, form: Form<OptionForm>) -> Result<HttpResponse> {
    add_option_handler(&state, OptionKind::Project, &form)
}

#[post("/summary/options/projects/{id}/toggle")]
async fn projects_toggle(state: Data<AppState>, path: Path<i64>) -> Result<HttpResponse> {
    toggle_option_handler(&state, OptionKind::Project, path.into_inner())
}

#[post("/summary/options/projects/{id}/delete")]
async fn projects_delete(state: Data<AppState>, path: Path<i64>) -> Result<HttpResponse> {
    delete_option_handler(&state, OptionKind::Project, path.into_inner())
}

#[get("/summary/options/specialties")]
async fn specialties_page(state: Data<AppState>) -> Result<HttpResponse> {
    render_option_page(&state, OptionKind::Specialty)
}

#[post("/summary/options/specialties")]
async fn specialties_add(state: Data<AppState>, form: Form<OptionForm>) -> Result<HttpResponse> {
    add_option_handler(&state, OptionKind::Specialty, &form)
}

#[post("/summary/options/specialties/{id}/toggle")]
async fn specialties_toggle(state: Data<AppState>, path: Path<i64>) -> Result<HttpResponse> {
    toggle_option_handler(&state, OptionKind::Specialty, path.into_inner())
}

#[post("/summary/options/specialties/{id}/delete")]
async fn specialties_delete(state: Data<AppState>, path: Path<i64>) -> Result<HttpResponse> {
    delete_option_handler(&state, OptionKind::Specialty, path.into_inner())
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let conn = Connection::open(DB_PATH).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    init_db(&conn).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    maybe_seed_sample(&conn).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

    let state = Data::new(AppState {
        conn: Mutex::new(conn),
    });

    println!("日志填报系统启动：");
    println!("  填报端：http://127.0.0.1:3000/fill/new");
    println!("  汇总端：http://127.0.0.1:3000/summary");
    println!("  项目日志汇总：http://127.0.0.1:3000/summary/project-report");
    println!("  填报统计：http://127.0.0.1:3000/summary/attendance");
    println!("  选项维护：http://127.0.0.1:3000/summary/options");

    HttpServer::new(move || {
        App::new()
            .app_data(state.clone())
            .service(root)
            .service(fill_root)
            .service(fill_new_page)
            .service(fill_create)
            .service(fill_success)
            .service(summary_page)
            .service(project_report_page)
            .service(attendance_page)
            .service(edit_log_page)
            .service(update_log_handler)
            .service(delete_log_handler)
            .service(options_home)
            .service(persons_page)
            .service(persons_add)
            .service(persons_toggle)
            .service(persons_delete)
            .service(projects_page)
            .service(projects_add)
            .service(projects_toggle)
            .service(projects_delete)
            .service(specialties_page)
            .service(specialties_add)
            .service(specialties_toggle)
            .service(specialties_delete)
    })
    .bind(("0.0.0.0", 3000))?
    .run()
    .await
}
