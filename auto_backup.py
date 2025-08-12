import os
import io
import sys
import zipfile
import pandas as pd
from datetime import datetime, timedelta

# ==== 設定ここから ====
RETENTION_DAYS = int(os.getenv("BACKUP_RETENTION_DAYS", "60"))  # 何日残すか（環境変数で上書き可）
DATA_DIR = os.getenv("DATA_DIR", ".")                            # アプリと同じルール
# ==== 設定ここまで ====

CSV_PATH      = os.path.join(DATA_DIR, "attendance_log.csv")
HOLIDAY_CSV   = os.path.join(DATA_DIR, "holiday_requests.csv")
AUDIT_LOG_CSV = os.path.join(DATA_DIR, "holiday_audit_log.csv")
LOGIN_CSV     = os.path.join(DATA_DIR, "社員ログイン情報.csv")

BACKUP_TABLES = [
    (CSV_PATH,      ["社員ID","氏名","日付","出勤時刻","退勤時刻","緯度","経度"], "attendance_log.csv"),
    (HOLIDAY_CSV,   ["社員ID","氏名","申請日","休暇日","休暇種類","備考","ステータス","承認者","承認日時","却下理由"], "holiday_requests.csv"),
    (AUDIT_LOG_CSV, ["timestamp","承認者","社員ID","氏名","休暇日","申請日","旧ステータス","新ステータス","却下理由"], "holiday_audit_log.csv"),
    (LOGIN_CSV,     ["社員ID","氏名","部署","パスワード"], "社員ログイン情報.csv"),
]

def log(msg: str):
    bdir = os.path.join(DATA_DIR, "backups")
    os.makedirs(bdir, exist_ok=True)
    with open(os.path.join(bdir, "backup.log"), "a", encoding="utf-8") as f:
        f.write(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}\n")

def _read_csv_flexible(path: str, columns: list[str]) -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame(columns=columns)
    for enc in ("utf-8-sig", "utf-8", "cp932"):
        try:
            df = pd.read_csv(path, dtype=str, encoding=enc).fillna("")
            break
        except UnicodeDecodeError:
            continue
    else:
        df = pd.read_csv(path, dtype=str, encoding="cp932", encoding_errors="replace").fillna("")
    # 足りない列を追加、余計な列は落とす
    for c in columns:
        if c not in df.columns:
            df[c] = ""
    return df[columns].copy()

def make_backup():
    os.makedirs(DATA_DIR, exist_ok=True)
    backup_dir = os.path.join(DATA_DIR, "backups")
    os.makedirs(backup_dir, exist_ok=True)

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    zip_path = os.path.join(backup_dir, f"backup_{stamp}.zip")

    try:
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for path, cols, fname in BACKUP_TABLES:
                df = _read_csv_flexible(path, cols)
                content = df.to_csv(index=False)           # 文字列
                zf.writestr(fname, content.encode("cp932"))# cp932で格納（Excel互換）
        with open(zip_path, "wb") as f:
            f.write(buf.getvalue())
        log(f"OK: created {zip_path}")
        return True, zip_path
    except Exception as e:
        log(f"ERROR: backup failed: {e}")
        return False, str(e)

def rotate_backups():
    try:
        if RETENTION_DAYS <= 0:
            return
        cutoff = datetime.now() - timedelta(days=RETENTION_DAYS)
        backup_dir = os.path.join(DATA_DIR, "backups")
        if not os.path.isdir(backup_dir):
            return
        removed = 0
        for name in os.listdir(backup_dir):
            if not name.lower().endswith(".zip"):
                continue
            p = os.path.join(backup_dir, name)
            try:
                mtime = datetime.fromtimestamp(os.path.getmtime(p))
                if mtime < cutoff:
                    os.remove(p)
                    removed += 1
            except Exception:
                continue
        if removed:
            log(f"ROTATE: removed {removed} old backups (> {RETENTION_DAYS} days)")
    except Exception as e:
        log(f"ERROR: rotate failed: {e}")

if __name__ == "__main__":
    ok, msg = make_backup()
    rotate_backups()
    # コンソールにも一言
    print("Backup", "OK" if ok else "FAILED", msg)
    sys.exit(0 if ok else 1)
