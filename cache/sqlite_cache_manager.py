import sqlite3
import hashlib
import pickle
import zlib
from datetime import datetime, timedelta
from typing import Optional, Tuple

import pandas as pd


class SQLiteCacheManager:
    """
    SQLite 缓存管理器：
    - 参数哈希索引
    - 压缩存储 DataFrame
    - TTL 过期
    - 增量更新（只补算 cache_end 之后的数据）
    """

    def __init__(self, db_path: str, version: str = "v1", ttl_days: Optional[int] = None):
        self.db_path = db_path
        self.version = version
        self.ttl_days = ttl_days
        self._init_db()

    def _init_db(self) -> None:
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS strategy_cache (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            strategy_type TEXT NOT NULL,
            params_hash TEXT NOT NULL,
            cache_start TEXT NOT NULL,
            cache_end TEXT NOT NULL,
            version TEXT NOT NULL,
            data_blob BLOB NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """)
        cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_cache_unique
        ON strategy_cache(strategy_type, params_hash, version)
        """)
        conn.commit()
        conn.close()

    @staticmethod
    def _hash_params(strategy_type: str, params: dict) -> str:
        # params 里有 datetime/date 对象时也能稳定 stringify（排序后）
        key = strategy_type + str(sorted(params.items(), key=lambda x: x[0]))
        return hashlib.md5(key.encode("utf-8")).hexdigest()

    def _is_expired(self, created_at_iso: str) -> bool:
        if self.ttl_days is None:
            return False
        created = datetime.fromisoformat(created_at_iso)
        return datetime.now() > created + timedelta(days=self.ttl_days)

    def load_for_range(
        self,
        strategy_type: str,
        params: dict,
        request_start,
        request_end
    ) -> Tuple[Optional[pd.DataFrame], Optional[pd.Timestamp]]:
        """
        返回：
        - cached_df：如果有可用缓存则返回（可能只覆盖到 cache_end）
        - cache_end：若需要增量更新则返回 cache_end，否则 None
        """
        params_hash = self._hash_params(strategy_type, params)
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        cur.execute("""
        SELECT cache_start, cache_end, data_blob, created_at
        FROM strategy_cache
        WHERE strategy_type=? AND params_hash=? AND version=?
        """, (strategy_type, params_hash, self.version))
        row = cur.fetchone()
        conn.close()

        if not row:
            return None, None

        cache_start_s, cache_end_s, blob, created_at_s = row

        if self._is_expired(created_at_s):
            # 过期就当没有
            return None, None

        df = pickle.loads(zlib.decompress(blob))

        cache_start = pd.to_datetime(cache_start_s)
        cache_end = pd.to_datetime(cache_end_s)
        request_start = pd.to_datetime(request_start)
        request_end = pd.to_datetime(request_end)

        # 完全覆盖：直接裁剪返回
        if request_start >= cache_start and request_end <= cache_end:
            out = df[(df["datetime"] >= request_start) & (df["datetime"] <= request_end)].copy()
            return out, None

        # 部分覆盖：允许“向后增量”补齐
        if request_start >= cache_start and request_end > cache_end:
            return df.copy(), cache_end

        # 如果请求区间在缓存之前（向前补算）——本版本先不做（你要的话后续可加）
        return None, None

    def save_full(self, strategy_type: str, params: dict, df: pd.DataFrame) -> None:
        """
        覆盖保存整段 df（更新 cache_start/cache_end）
        """
        if df is None or df.empty:
            return

        df = df.copy()
        df["datetime"] = pd.to_datetime(df["datetime"])
        cache_start = str(df["datetime"].min())
        cache_end = str(df["datetime"].max())

        params_hash = self._hash_params(strategy_type, params)
        blob = zlib.compress(pickle.dumps(df))
        now = datetime.now().isoformat()

        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        cur.execute("""
        INSERT INTO strategy_cache(strategy_type, params_hash, cache_start, cache_end, version, data_blob, created_at, updated_at)
        VALUES(?,?,?,?,?,?,?,?)
        ON CONFLICT(strategy_type, params_hash, version)
        DO UPDATE SET
            cache_start=excluded.cache_start,
            cache_end=excluded.cache_end,
            data_blob=excluded.data_blob,
            updated_at=excluded.updated_at
        """, (strategy_type, params_hash, cache_start, cache_end, self.version, blob, now, now))
        conn.commit()
        conn.close()