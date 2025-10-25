from flask import Flask, render_template, request, jsonify
import os
import sqlite3
import threading
from datetime import datetime
import traceback
from werkzeug.exceptions import HTTPException

app = Flask(__name__)

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
DB_PATH = os.path.join(BASE_DIR, 'db', 'index.db')

# ---------------- DB helpers ----------------
def get_conn():
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    conn.row_factory = sqlite3.Row
    return conn

def get_table_columns(conn, table):
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    return {row[1] for row in cur.fetchall()}  # set de nombres de columna

def column_exists(conn, table, col):
    return col in get_table_columns(conn, table)

def init_db():
    os.makedirs(os.path.join(BASE_DIR, 'db'), exist_ok=True)
    with get_conn() as conn:
        c = conn.cursor()
        c.execute("PRAGMA journal_mode=WAL")
        c.execute("PRAGMA synchronous=NORMAL")
        c.execute("PRAGMA busy_timeout=30000")

        # Tablas base
        c.execute('''
            CREATE TABLE IF NOT EXISTS files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                disk_name TEXT,
                folder TEXT,
                file_name TEXT,
                size INTEGER,
                created_at TEXT,
                modified_at TEXT
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS disks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                disk_name TEXT UNIQUE,
                last_scan_date TEXT
                -- columnas nuevas se añaden por ALTER TABLE abajo
            )
        ''')
        conn.commit()

        # Migraciones seguras (intenta añadir, si ya existen no pasa nada)
        for col, ddl in [
            ('status',           "ALTER TABLE disks ADD COLUMN status TEXT"),
            ('message',          "ALTER TABLE disks ADD COLUMN message TEXT"),
            ('total_files',      "ALTER TABLE disks ADD COLUMN total_files INTEGER"),
            ('total_bytes',      "ALTER TABLE disks ADD COLUMN total_bytes INTEGER"),
            ('processed_files',  "ALTER TABLE disks ADD COLUMN processed_files INTEGER"),
            ('processed_bytes',  "ALTER TABLE disks ADD COLUMN processed_bytes INTEGER"),
            ('segments_total',   "ALTER TABLE disks ADD COLUMN segments_total INTEGER"),
            ('segments_done',    "ALTER TABLE disks ADD COLUMN segments_done INTEGER"),
        ]:
            try:
                if not column_exists(conn, 'disks', col):
                    c.execute(ddl)
            except sqlite3.OperationalError:
                # Si falla por alguna razón (p.ej. columna ya existe con otro tipo), seguimos
                pass
        conn.commit()

        # Índices
        c.execute('CREATE INDEX IF NOT EXISTS idx_files_disk ON files(disk_name)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_files_folder ON files(folder)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_files_name ON files(file_name)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_files_size ON files(size)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_files_ctime ON files(created_at)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_files_mtime ON files(modified_at)')
        conn.commit()

# ---------------- Escaneo por segmentos ----------------
def list_first_level_segments(folder):
    segments = [("<ROOT_FILES>", folder)]
    try:
        with os.scandir(folder) as it:
            for entry in it:
                if entry.is_dir(follow_symlinks=False):
                    segments.append((entry.name, entry.path))
    except Exception:
        pass
    return segments

def process_root_files(disk_name, root_folder, progress):
    batch, BATCH_SIZE = [], 500
    try:
        with os.scandir(root_folder) as it:
            for entry in it:
                if entry.is_file(follow_symlinks=False):
                    try:
                        st = entry.stat()
                        size = st.st_size
                        batch.append((
                            disk_name, root_folder, entry.name, size,
                            datetime.fromtimestamp(st.st_ctime).isoformat(),
                            datetime.fromtimestamp(st.st_mtime).isoformat()
                        ))
                        progress['processed_files'] += 1
                        progress['processed_bytes'] += size
                        if len(batch) >= BATCH_SIZE:
                            flush_batch(disk_name, batch, progress, f"Indexando raíz… {progress['processed_files']} ficheros")
                    except Exception:
                        pass
        if batch:
            flush_batch(disk_name, batch, progress, f"Indexando raíz… {progress['processed_files']} ficheros")
    except Exception:
        pass

def process_subtree(disk_name, subdir_path, progress):
    batch, BATCH_SIZE = [], 500
    for root, _, files in os.walk(subdir_path):
        for name in files:
            file_path = os.path.join(root, name)
            try:
                st = os.stat(file_path)
                size = st.st_size
                batch.append((
                    disk_name, root, name, size,
                    datetime.fromtimestamp(st.st_ctime).isoformat(),
                    datetime.fromtimestamp(st.st_mtime).isoformat()
                ))
                progress['processed_files'] += 1
                progress['processed_bytes'] += size
                if len(batch) >= BATCH_SIZE:
                    flush_batch(disk_name, batch, progress, f"Indexando… {progress['processed_files']} ficheros")
            except Exception:
                pass
    if batch:
        flush_batch(disk_name, batch, progress, f"Indexando… {progress['processed_files']} ficheros")

def flush_batch(disk_name, batch, progress, msg):
    with get_conn() as conn:
        c = conn.cursor()
        c.execute("PRAGMA busy_timeout=30000")
        c.executemany('''
            INSERT INTO files (disk_name, folder, file_name, size, created_at, modified_at)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', batch)
        # Algunas DB antiguas quizá no tengan processed_*; protegemos con try
        try:
            c.execute('''
                UPDATE disks
                   SET processed_files=?, processed_bytes=?, message=?
                 WHERE disk_name=?
            ''', (progress['processed_files'], progress['processed_bytes'], msg, disk_name))
        except sqlite3.OperationalError:
            # columnas no existen en schema viejo -> ignoramos el update parcial
            pass
        conn.commit()
    batch.clear()

def scan_directory(disk_name, folder):
    try:
        if not os.path.isdir(folder):
            raise ValueError(f"La carpeta no existe o no es accesible: {folder}")

        segments = list_first_level_segments(folder)
        segments_total = len(segments)
        segments_done = 0

        with get_conn() as conn:
            c = conn.cursor()
            # limpia archivos previos de ese disco si la tabla existe
            try:
                c.execute('DELETE FROM files WHERE disk_name=?', (disk_name,))
            except sqlite3.OperationalError:
                pass
            # set estado inicial; muchas columnas pueden no existir -> construimos SQL mínimo
            try:
                c.execute('''
                    INSERT INTO disks (disk_name, last_scan_date, status, message, segments_total, segments_done,
                                       total_files, total_bytes, processed_files, processed_bytes)
                    VALUES (?, ?, 'indexing', 'Preparando…', ?, 0, 0, 0, 0, 0)
                    ON CONFLICT(disk_name) DO UPDATE SET
                        last_scan_date=excluded.last_scan_date,
                        status='indexing', message='Preparando…',
                        segments_total=?, segments_done=0,
                        total_files=0, total_bytes=0, processed_files=0, processed_bytes=0
                ''', (disk_name, datetime.now().isoformat(), segments_total, segments_total))
            except sqlite3.OperationalError:
                # Fallback minimalista si columnas no existen aún
                c.execute('''
                    INSERT INTO disks (disk_name, last_scan_date)
                    VALUES (?, ?)
                    ON CONFLICT(disk_name) DO UPDATE SET last_scan_date=excluded.last_scan_date
                ''', (disk_name, datetime.now().isoformat()))
            conn.commit()

        progress = {'processed_files': 0, 'processed_bytes': 0}

        # raíz
        try:
            with get_conn() as conn:
                c = conn.cursor()
                c.execute('UPDATE disks SET message=? WHERE disk_name=?',
                          (f"Indexando carpeta raíz… (1/{segments_total})", disk_name))
                conn.commit()
        except sqlite3.OperationalError:
            pass

        process_root_files(disk_name, segments[0][1], progress)
        segments_done += 1
        try:
            with get_conn() as conn:
                c = conn.cursor()
                c.execute('UPDATE disks SET segments_done=?, message=? WHERE disk_name=?',
                          (segments_done, f"Completado {segments_done}/{segments_total}: raíz", disk_name))
                conn.commit()
        except sqlite3.OperationalError:
            pass

        # subcarpetas primer nivel
        for idx, (name, path) in enumerate(segments[1:], start=2):
            try:
                with get_conn() as conn:
                    c = conn.cursor()
                    c.execute('UPDATE disks SET message=? WHERE disk_name=?',
                              (f"Indexando {name}… ({idx}/{segments_total})", disk_name))
                    conn.commit()
            except sqlite3.OperationalError:
                pass

            process_subtree(disk_name, path, progress)

            segments_done += 1
            try:
                with get_conn() as conn:
                    c = conn.cursor()
                    c.execute('UPDATE disks SET segments_done=?, message=? WHERE disk_name=?',
                              (segments_done, f"Completado {segments_done}/{segments_total}: {name}", disk_name))
                    conn.commit()
            except sqlite3.OperationalError:
                pass

        # Fin
        with get_conn() as conn:
            c = conn.cursor()
            try:
                c.execute('UPDATE disks SET last_scan_date=?, status=?, message=? WHERE disk_name=?',
                          (datetime.now().isoformat(), 'done', 'Escaneo completado', disk_name))
            except sqlite3.OperationalError:
                # schema viejo: al menos marca last_scan_date
                c.execute('UPDATE disks SET last_scan_date=? WHERE disk_name=?',
                          (datetime.now().isoformat(), disk_name))
            conn.commit()

        print(f"[OK] {disk_name}: archivos={progress['processed_files']} bytes={progress['processed_bytes']} seg={segments_done}/{segments_total}")

    except Exception as e:
        err = f"{type(e).__name__}: {e}"
        print("[ERROR] Escaneo fallido:", err)
        traceback.print_exc()
        try:
            with get_conn() as conn:
                c = conn.cursor()
                try:
                    c.execute('''
                        INSERT INTO disks (disk_name, last_scan_date, status, message)
                        VALUES (?, ?, 'error', ?)
                        ON CONFLICT(disk_name) DO UPDATE SET
                            last_scan_date=excluded.last_scan_date,
                            status='error', message=excluded.message
                    ''', (disk_name, datetime.now().isoformat(), err))
                except sqlite3.OperationalError:
                    c.execute('''
                        INSERT INTO disks (disk_name, last_scan_date)
                        VALUES (?, ?)
                        ON CONFLICT(disk_name) DO UPDATE SET last_scan_date=excluded.last_scan_date
                    ''', (disk_name, datetime.now().isoformat()))
                conn.commit()
        except Exception as e2:
            print("[FATAL] No se pudo registrar el error en la DB:", e2)

# ---------------- API ----------------
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/scan', methods=['POST'])
def scan():
    data = request.json or {}
    disk_name = (data.get('disk_name') or '').strip()
    folder = (data.get('folder') or '').strip()
    if not disk_name or not folder:
        return jsonify({"status":"error","message":"Faltan disk_name o folder"}), 400
    threading.Thread(target=scan_directory, args=(disk_name, folder), daemon=True).start()
    return jsonify({"status":"ok","message":"Escaneo iniciado"})

@app.route('/disks', methods=['GET'])
def get_disks():
    with get_conn() as conn:
        cols = get_table_columns(conn, 'disks')
        # columnas que intentaremos leer si existen
        wanted = [
            'id','disk_name','last_scan_date','status','message',
            'total_files','total_bytes','processed_files','processed_bytes',
            'segments_total','segments_done'
        ]
        existing = [c for c in wanted if c in cols]
        # siempre garantizamos estas 3 como mínimo
        base = ['id','disk_name','last_scan_date']
        select_list = existing or base
        sql = f"SELECT {', '.join(select_list)} FROM disks ORDER BY (last_scan_date IS NULL) ASC, last_scan_date DESC"
        cur = conn.cursor()
        cur.execute("PRAGMA busy_timeout=30000")
        cur.execute(sql)
        rows = [dict(r) for r in cur.fetchall()]

    # normalizamos: añadimos claves faltantes con default
    defaults = {
        'status': 'idle', 'message': '',
        'total_files': 0, 'total_bytes': 0,
        'processed_files': 0, 'processed_bytes': 0,
        'segments_total': 0, 'segments_done': 0
    }
    for r in rows:
        for k, v in defaults.items():
            r.setdefault(k, v)
    return jsonify(rows)

@app.route('/search', methods=['GET'])
def search():
    q = (request.args.get('q') or '').strip()
    disk = (request.args.get('disk') or '').strip()
    folder_contains = (request.args.get('folder') or '').strip()
    name_contains = (request.args.get('name') or '').strip()
    ext = (request.args.get('ext') or '').strip().lstrip('.')
    size_min = request.args.get('size_min', type=int)
    size_max = request.args.get('size_max', type=int)
    created_from = (request.args.get('created_from') or '').strip()
    created_to = (request.args.get('created_to') or '').strip()
    modified_from = (request.args.get('modified_from') or '').strip()
    modified_to = (request.args.get('modified_to') or '').strip()
    order_by = request.args.get('order_by', 'modified_at DESC')
    limit = request.args.get('limit', default=100, type=int)
    offset = request.args.get('offset', default=0, type=int)

    where, params = [], []
    if q:
        like = f'%{q}%'
        where.append('(disk_name LIKE ? OR folder LIKE ? OR file_name LIKE ?)')
        params += [like, like, like]
    if disk:
        where.append('disk_name = ?'); params.append(disk)
    if folder_contains:
        where.append('folder LIKE ?'); params.append(f'%{folder_contains}%')
    if name_contains:
        where.append('file_name LIKE ?'); params.append(f'%{name_contains}%')
    if ext:
        where.append('LOWER(file_name) LIKE ?'); params.append(f'%.{ext.lower()}')
    if size_min is not None:
        where.append('size >= ?'); params.append(size_min)
    if size_max is not None:
        where.append('size <= ?'); params.append(size_max)
    if created_from:
        where.append('created_at >= ?'); params.append(created_from)
    if created_to:
        where.append('created_at <= ?'); params.append(created_to)
    if modified_from:
        where.append('modified_at >= ?'); params.append(modified_from)
    if modified_to:
        where.append('modified_at <= ?'); params.append(modified_to)

    where_sql = ('WHERE ' + ' AND '.join(where)) if where else ''
    valid_orders = {
        'size ASC','size DESC','file_name ASC','file_name DESC',
        'created_at ASC','created_at DESC','modified_at ASC','modified_at DESC',
        'folder ASC','folder DESC'
    }
    if order_by not in valid_orders:
        order_by = 'modified_at DESC'

    with get_conn() as conn:
        c = conn.cursor()
        c.execute("PRAGMA busy_timeout=30000")
        c.execute(f'SELECT COUNT(*) FROM files {where_sql}', params)
        total = c.fetchone()[0]
        c.execute(f'''
            SELECT id, disk_name, folder, file_name, size, created_at, modified_at
            FROM files
            {where_sql}
            ORDER BY {order_by}
            LIMIT ? OFFSET ?
        ''', params + [limit, offset])
        rows = [dict(r) for r in c.fetchall()]
    return jsonify({"total": total, "items": rows})

# ---------------- Error handler JSON ----------------
@app.errorhandler(Exception)
def handle_error(e):
    code = 500; msg = str(e)
    if isinstance(e, HTTPException):
        code = e.code; msg = e.description
    return jsonify({"status":"error","message":msg}), code

if __name__ == '__main__':
    init_db()
    app.run(debug=True, use_reloader=False)

