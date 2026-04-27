# ==========================================
# data_loader.py - 資料讀取與 API 串接（CSV / API 模式）
# ==========================================

from hashlib import sha256
from pathlib import Path
from typing import Iterable, Mapping

import numpy as np
import pandas as pd

from config import FINMIND_CONFIG


FINMIND_API_URL = 'https://api.finmindtrade.com/api/v4/data'
FINMIND_BROKER_DATASET = 'TaiwanStockTradingDailyReport'
FINMIND_PRICE_DATASET = 'TaiwanStockPrice'
FINMIND_HOLDER_DATASET = 'TaiwanStockHoldingSharesPer'
FINMIND_STOCK_INFO_DATASET = 'TaiwanStockInfo'
FINMIND_TRADER_INFO_DATASET = 'TaiwanSecuritiesTraderInfo'
FINMIND_BROKER_DATA_START = pd.Timestamp(FINMIND_CONFIG['broker_min_date'])
FINMIND_PRICE_DATA_START = pd.Timestamp('1994-10-01')
FINMIND_HOLDER_DATA_START = pd.Timestamp('2010-01-29')
TW_STOCK_LOT_SIZE = 1000
FINMIND_KNOWN_GAPS = (
    (pd.Timestamp('2022-10-31'), pd.Timestamp('2022-11-03')),
    (pd.Timestamp('2023-01-11'), pd.Timestamp('2023-01-17')),
)


def _validate_required_columns(
    df: pd.DataFrame,
    required_columns: Iterable[str],
    dataset_name: str
) -> None:
    """確認必要欄位齊全，缺漏時回傳清楚錯誤訊息。"""
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        missing_text = ", ".join(missing)
        raise ValueError(f"{dataset_name} 缺少必要欄位：{missing_text}")


def _normalize_stock_ids(series: pd.Series) -> pd.Series:
    """將股票代號統一成不帶空白、不帶尾端 .0 的字串。"""
    def normalize(value) -> str | None:
        if pd.isna(value):
            return None
        if isinstance(value, (int, np.integer)):
            return str(int(value))
        if isinstance(value, (float, np.floating)) and float(value).is_integer():
            return str(int(value))

        text = str(value).strip()
        if text.endswith(".0"):
            text = text[:-2]
        return text or None

    return series.map(normalize)


def _normalize_date_input(value, field_name: str) -> pd.Timestamp:
    """將輸入日期轉成 pandas Timestamp，失敗時提供清楚錯誤訊息。"""
    timestamp = pd.to_datetime(value, errors='coerce')
    if pd.isna(timestamp):
        raise ValueError(f'{field_name} 格式錯誤，請使用 YYYY-MM-DD')
    return pd.Timestamp(timestamp).normalize()


def _prepare_broker_trading_df(
    df: pd.DataFrame,
    dataset_name: str,
) -> pd.DataFrame:
    """統一整理券商分點資料，供 CSV 與 API 模式共用。"""
    prepared = df.copy()
    prepared.columns = prepared.columns.astype(str).str.strip()
    _validate_required_columns(
        prepared,
        ['date', 'stock_id', 'stock_name', 'broker', 'branch', 'buy_volume', 'sell_volume', 'net_buy'],
        dataset_name,
    )

    prepared['date'] = pd.to_datetime(prepared['date'], errors='coerce')

    for col in ['buy_volume', 'sell_volume', 'net_buy']:
        if col in prepared.columns:
            prepared[col] = pd.to_numeric(prepared[col], errors='coerce').fillna(0)

    if 'buy_avg_price' in prepared.columns:
        prepared['buy_avg_price'] = pd.to_numeric(prepared['buy_avg_price'], errors='coerce')
    else:
        prepared['buy_avg_price'] = np.nan

    prepared['stock_id'] = _normalize_stock_ids(prepared['stock_id'])
    prepared = prepared.dropna(subset=['date', 'stock_id'])
    prepared['stock_id'] = prepared['stock_id'].astype(str)

    for col in ['stock_name', 'broker', 'branch']:
        if col in prepared.columns:
            prepared[col] = prepared[col].fillna('').astype(str).str.strip()

    return prepared.reset_index(drop=True)


def _prepare_price_data_df(
    df: pd.DataFrame,
    dataset_name: str,
) -> pd.DataFrame:
    """統一整理股價資料，供 CSV 與 API 模式共用。"""
    prepared = df.copy()
    prepared.columns = prepared.columns.astype(str).str.strip()

    rename_map = {}
    if 'max' in prepared.columns and 'high' not in prepared.columns:
        rename_map['max'] = 'high'
    if 'min' in prepared.columns and 'low' not in prepared.columns:
        rename_map['min'] = 'low'
    if rename_map:
        prepared = prepared.rename(columns=rename_map)

    if 'Trading_Volume' in prepared.columns and 'volume' not in prepared.columns:
        prepared['volume'] = pd.to_numeric(prepared['Trading_Volume'], errors='coerce') / TW_STOCK_LOT_SIZE

    if 'stock_name' not in prepared.columns:
        prepared['stock_name'] = ''

    _validate_required_columns(
        prepared,
        ['date', 'stock_id', 'open', 'high', 'low', 'close', 'volume'],
        dataset_name,
    )

    prepared['date'] = pd.to_datetime(prepared['date'], errors='coerce')

    for col in ['open', 'high', 'low', 'close', 'volume']:
        if col in prepared.columns:
            prepared[col] = pd.to_numeric(prepared[col], errors='coerce')

    prepared['stock_id'] = _normalize_stock_ids(prepared['stock_id'])
    prepared = prepared.dropna(subset=['date', 'stock_id', 'close'])
    prepared['stock_id'] = prepared['stock_id'].astype(str)
    prepared['stock_name'] = prepared['stock_name'].fillna('').astype(str).str.strip()

    return prepared.reset_index(drop=True)


def _prepare_holder_data_df(
    df: pd.DataFrame,
    dataset_name: str,
) -> pd.DataFrame:
    """統一整理集保戶數資料，供 CSV 與 API 模式共用。"""
    prepared = df.copy()
    prepared.columns = prepared.columns.astype(str).str.strip()

    if 'date' in prepared.columns and 'week_date' not in prepared.columns:
        prepared = prepared.rename(columns={'date': 'week_date'})

    if 'stock_name' not in prepared.columns:
        prepared['stock_name'] = ''

    if 'holder_count' not in prepared.columns and {'week_date', 'stock_id', 'people'}.issubset(prepared.columns):
        prepared['people'] = pd.to_numeric(prepared['people'], errors='coerce').fillna(0)
        prepared = (
            prepared
            .groupby(['week_date', 'stock_id'], as_index=False)
            .agg({'people': 'sum', 'stock_name': 'last'})
            .rename(columns={'people': 'holder_count'})
        )

    _validate_required_columns(
        prepared,
        ['week_date', 'stock_id', 'holder_count'],
        dataset_name,
    )

    prepared['week_date'] = pd.to_datetime(prepared['week_date'], errors='coerce')
    prepared['holder_count'] = pd.to_numeric(prepared['holder_count'], errors='coerce')

    prepared['stock_id'] = _normalize_stock_ids(prepared['stock_id'])
    prepared = prepared.dropna(subset=['week_date', 'stock_id', 'holder_count'])
    prepared['stock_id'] = prepared['stock_id'].astype(str)
    prepared['stock_name'] = prepared['stock_name'].fillna('').astype(str).str.strip()

    return prepared.reset_index(drop=True)


def _normalize_stock_id_list(stock_ids: Iterable[str]) -> list[str]:
    """將股票代號列表去重並正規化。"""
    normalized_ids: list[str] = []
    seen: set[str] = set()

    for stock_id in stock_ids:
        normalized = _normalize_stock_ids(pd.Series([stock_id])).iloc[0]
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        normalized_ids.append(normalized)

    return normalized_ids


def _extract_broker_name(branch_name: str) -> str:
    """從完整分點名稱中拆出券商品牌名。"""
    if not branch_name:
        return ''

    for separator in (' - ', '-', '－', '–'):
        if separator in branch_name:
            return branch_name.split(separator, 1)[0].strip()

    return branch_name.strip()


def _request_finmind_dataset(
    dataset: str,
    params: Mapping[str, str],
    api_token: str,
    timeout_seconds: int,
) -> pd.DataFrame:
    """呼叫 FinMind dataset API，並統一處理常見錯誤。"""
    if not api_token:
        raise ValueError('缺少 FinMind API token，請設定 FINMIND_API_TOKEN')

    import requests

    response = requests.get(
        FINMIND_API_URL,
        headers={'Authorization': f'Bearer {api_token}'},
        params={'dataset': dataset, **params},
        timeout=timeout_seconds,
    )

    if response.status_code == 401:
        raise ValueError('FinMind API token 無效，請確認 FINMIND_API_TOKEN')
    try:
        payload = response.json()
    except ValueError:
        payload = None

    message = ''
    status = None
    if isinstance(payload, dict):
        message = str(payload.get('msg', '')).strip()
        status = payload.get('status')

    lowered = message.lower()

    if response.status_code == 402 or 'upper limit' in lowered:
        raise ValueError('FinMind API 已達使用上限，請稍後再試或升級方案')
    if (
        response.status_code == 403
        or 'update your user level' in lowered
        or 'your level is' in lowered
        or 'sponsor' in lowered
        or 'backer' in lowered
        or 'permission' in lowered
        or '權限' in message
    ):
        raise ValueError('FinMind API 權限不足，請確認目前帳號已升級到所需方案（backer/sponsor）')
    if response.status_code >= 400:
        raise ValueError(f'FinMind API 請求失敗：{message or f"HTTP {response.status_code}"}')

    if payload is None:
        raise ValueError('FinMind API 回應不是有效 JSON')

    if status and status != 200:
        if 'upper limit' in lowered:
            raise ValueError('FinMind API 已達使用上限，請稍後再試或升級方案')
        if (
            'update your user level' in lowered
            or 'your level is' in lowered
            or 'sponsor' in lowered
            or 'backer' in lowered
            or 'permission' in lowered
            or '權限' in message
        ):
            raise ValueError('FinMind API 權限不足，請確認目前帳號已升級到所需方案（backer/sponsor）')
        raise ValueError(f'FinMind API 回傳錯誤：{message or status}')

    data = payload.get('data', [])
    if not isinstance(data, list):
        raise ValueError('FinMind API 回應格式異常：缺少 data 陣列')

    return pd.DataFrame(data)


def _load_finmind_trader_lookup(api_token: str, timeout_seconds: int) -> dict[str, str]:
    """載入證券商代碼對照表。"""
    trader_df = _request_finmind_dataset(
        FINMIND_TRADER_INFO_DATASET,
        params={},
        api_token=api_token,
        timeout_seconds=timeout_seconds,
    )

    if trader_df.empty:
        return {}

    trader_df = trader_df.copy()
    trader_df['securities_trader_id'] = trader_df['securities_trader_id'].astype(str).str.strip()
    trader_df['securities_trader'] = trader_df['securities_trader'].fillna('').astype(str).str.strip()
    trader_df = trader_df.drop_duplicates(subset=['securities_trader_id'], keep='last')

    return trader_df.set_index('securities_trader_id')['securities_trader'].to_dict()


def _classify_finmind_market_type(value) -> str | None:
    """將 FinMind 股票 type 欄位映射成 app 使用的市場範圍。"""
    if pd.isna(value):
        return None

    text = str(value).strip()
    if not text:
        return None

    lowered = text.lower()
    if lowered in {'twse', 'listed', 'sii'}:
        return 'listed'
    if lowered in {'tpex', 'otc'}:
        return 'otc'
    if 'twse' in lowered or 'listed' in lowered or '上市' in text or 'sii' in lowered:
        return 'listed'
    if 'tpex' in lowered or lowered == 'otc' or '上櫃' in text:
        return 'otc'

    return None


def _market_scope_label(market_scope: str) -> str:
    """將市場範圍代碼轉成人類可讀文字。"""
    return {
        'all': '全部',
        'listed': '上市（TWSE）',
        'otc': '上櫃（OTC）',
    }.get(market_scope, market_scope)


def fetch_stock_info_from_finmind(
    stock_ids: Iterable[str],
    api_token: str,
    timeout_seconds: int = 30,
) -> pd.DataFrame:
    """取得指定股票的名稱與市場資訊。"""
    normalized_ids = _normalize_stock_id_list(stock_ids)

    info_df = _request_finmind_dataset(
        FINMIND_STOCK_INFO_DATASET,
        params={},
        api_token=api_token,
        timeout_seconds=timeout_seconds,
    )

    if info_df.empty:
        return pd.DataFrame(columns=['stock_id', 'stock_name', 'type', 'market'])

    info_df = info_df.copy()
    info_df['stock_id'] = _normalize_stock_ids(info_df['stock_id'])
    info_df['stock_name'] = info_df['stock_name'].fillna('').astype(str).str.strip()
    if 'type' not in info_df.columns:
        info_df['type'] = ''
    info_df['type'] = info_df['type'].fillna('').astype(str).str.strip()
    info_df = info_df.dropna(subset=['stock_id'])

    if normalized_ids:
        info_df = info_df[info_df['stock_id'].isin(normalized_ids)]

    info_df = info_df.drop_duplicates(subset=['stock_id'], keep='last').copy()
    info_df['market'] = info_df['type'].map(_classify_finmind_market_type)

    return info_df[['stock_id', 'stock_name', 'type', 'market']].reset_index(drop=True)


def fetch_stock_name_lookup_from_finmind(
    stock_ids: Iterable[str],
    api_token: str,
    timeout_seconds: int = 30,
) -> dict[str, str]:
    """取得股票代號對應名稱，供多種自動下載資料共用。"""
    info_df = fetch_stock_info_from_finmind(
        stock_ids,
        api_token=api_token,
        timeout_seconds=timeout_seconds,
    )

    if info_df.empty:
        return {}

    return info_df.set_index('stock_id')['stock_name'].to_dict()


def filter_stock_ids_by_market_scope(
    stock_ids: Iterable[str],
    market_scope: str,
    api_token: str,
    timeout_seconds: int = 30,
) -> tuple[list[str], dict[str, str], list[str]]:
    """依市場範圍過濾股票代號，並回傳名稱對照與提示訊息。"""
    normalized_ids = _normalize_stock_id_list(stock_ids)
    if not normalized_ids:
        return [], {}, []

    if market_scope not in {'all', 'listed', 'otc'}:
        raise ValueError(f'不支援的市場範圍：{market_scope}')

    info_df = fetch_stock_info_from_finmind(
        normalized_ids,
        api_token=api_token,
        timeout_seconds=timeout_seconds,
    )
    if info_df.empty:
        raise ValueError('無法從 FinMind 取得股票資訊，請確認股票代號是否有效')

    warnings: list[str] = []
    matched_ids = set(info_df['stock_id'])
    missing_ids = [stock_id for stock_id in normalized_ids if stock_id not in matched_ids]
    if missing_ids:
        warnings.append(
            '以下股票代號在 FinMind 股票清單中找不到，已略過：'
            + ', '.join(missing_ids)
        )

    if market_scope == 'all':
        effective_df = info_df.copy()
    else:
        effective_df = info_df[info_df['market'] == market_scope].copy()

        mismatched_ids = [
            stock_id
            for stock_id in normalized_ids
            if stock_id in matched_ids and stock_id not in set(effective_df['stock_id'])
        ]
        if mismatched_ids:
            warnings.append(
                f'已依股票市場範圍「{_market_scope_label(market_scope)}」排除：'
                + ', '.join(mismatched_ids)
            )

    effective_ids = [stock_id for stock_id in normalized_ids if stock_id in set(effective_df['stock_id'])]
    stock_name_lookup = effective_df.set_index('stock_id')['stock_name'].to_dict()

    return effective_ids, stock_name_lookup, warnings


def _build_finmind_cache_path(
    data_kind: str,
    cache_dir: str | None,
    stock_ids: list[str],
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
) -> Path | None:
    """建立快取檔路徑。"""
    if not cache_dir:
        return None

    cache_root = Path(cache_dir)
    cache_root.mkdir(parents=True, exist_ok=True)

    cache_key = sha256('|'.join(stock_ids).encode('utf-8')).hexdigest()[:12]
    file_name = (
        f'{data_kind}_finmind_{start_date.strftime("%Y%m%d")}_{end_date.strftime("%Y%m%d")}_{cache_key}.csv'
    )
    return cache_root / file_name


def _write_dataframe_cache(
    cache_path: Path,
    df: pd.DataFrame,
    date_columns: Iterable[str],
) -> None:
    """將資料寫入本地快取，並統一格式化日期欄位。"""
    cache_df = df.copy()
    for column in date_columns:
        if column in cache_df.columns:
            cache_df[column] = pd.to_datetime(cache_df[column], errors='coerce').dt.strftime('%Y-%m-%d')
    cache_df.to_csv(cache_path, index=False)


def _collect_finmind_gap_warnings(
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
) -> list[str]:
    """回傳與 FinMind 已知缺資料日期重疊的警示。"""
    warnings: list[str] = []

    for gap_start, gap_end in FINMIND_KNOWN_GAPS:
        if start_date <= gap_end and end_date >= gap_start:
            warnings.append(
                'FinMind 分點資料在 '
                f'{gap_start.strftime("%Y-%m-%d")} ~ {gap_end.strftime("%Y-%m-%d")} '
                '有已知缺漏，期間結果可能不完整。'
            )

    return warnings


def _aggregate_finmind_broker_detail(
    raw_df: pd.DataFrame,
    trader_lookup: Mapping[str, str],
    stock_name_lookup: Mapping[str, str],
) -> pd.DataFrame:
    """將 FinMind 分點逐價資料彙總成既有 broker_trading 契約。"""
    detail_df = raw_df.copy()
    detail_df['stock_id'] = _normalize_stock_ids(detail_df['stock_id']).astype(str)
    detail_df['date'] = pd.to_datetime(detail_df['date'], errors='coerce')
    detail_df['securities_trader_id'] = detail_df['securities_trader_id'].astype(str).str.strip()
    detail_df['price'] = pd.to_numeric(detail_df['price'], errors='coerce')
    detail_df['buy'] = pd.to_numeric(detail_df['buy'], errors='coerce').fillna(0)
    detail_df['sell'] = pd.to_numeric(detail_df['sell'], errors='coerce').fillna(0)
    detail_df['branch'] = (
        detail_df['securities_trader_id'].map(trader_lookup)
        .fillna(detail_df['securities_trader'].fillna(''))
        .astype(str)
        .str.strip()
    )
    detail_df['broker'] = detail_df['branch'].map(_extract_broker_name)
    detail_df['stock_name'] = detail_df['stock_id'].map(stock_name_lookup).fillna('')
    detail_df['buy_value'] = detail_df['buy'] * detail_df['price']

    aggregated_df = (
        detail_df
        .groupby(['date', 'stock_id', 'stock_name', 'broker', 'branch'], as_index=False)
        .agg(
            buy_volume=('buy', 'sum'),
            sell_volume=('sell', 'sum'),
            buy_value=('buy_value', 'sum'),
        )
    )
    aggregated_df['net_buy'] = aggregated_df['buy_volume'] - aggregated_df['sell_volume']
    aggregated_df['buy_avg_price'] = np.where(
        aggregated_df['buy_volume'] > 0,
        aggregated_df['buy_value'] / aggregated_df['buy_volume'],
        np.nan,
    )

    return aggregated_df[
        [
            'date', 'stock_id', 'stock_name', 'broker', 'branch',
            'buy_volume', 'sell_volume', 'net_buy', 'buy_avg_price',
        ]
    ]


def fetch_broker_trading_from_finmind(
    stock_ids: Iterable[str],
    start_date: str,
    end_date: str,
    api_token: str,
    stock_name_lookup: Mapping[str, str] | None = None,
    cache_dir: str | None = None,
    force_refresh: bool = False,
    timeout_seconds: int = 30,
) -> tuple[pd.DataFrame, list[str]]:
    """從 FinMind sponsor API 取得券商分點日彙總資料。"""
    normalized_ids = _normalize_stock_id_list(stock_ids)
    if not normalized_ids:
        raise ValueError('請至少提供一檔有效股票代號')

    start_timestamp = _normalize_date_input(start_date, '開始日期')
    end_timestamp = _normalize_date_input(end_date, '結束日期')

    if end_timestamp < start_timestamp:
        raise ValueError('結束日期不可早於開始日期')
    if start_timestamp < FINMIND_BROKER_DATA_START:
        raise ValueError(
            'FinMind 分點資料僅支援 2021-06-30 之後的日期，'
            '更早資料請改用手動 CSV 匯入'
        )

    cache_path = _build_finmind_cache_path(
        data_kind='broker',
        cache_dir=cache_dir,
        stock_ids=normalized_ids,
        start_date=start_timestamp,
        end_date=end_timestamp,
    )

    warnings = _collect_finmind_gap_warnings(start_timestamp, end_timestamp)

    if cache_path and cache_path.exists() and not force_refresh:
        return load_broker_trading(cache_path), warnings

    trader_lookup = _load_finmind_trader_lookup(api_token, timeout_seconds)
    name_lookup = {str(key): str(value) for key, value in (stock_name_lookup or {}).items()}
    if not name_lookup:
        name_lookup = fetch_stock_name_lookup_from_finmind(
            normalized_ids,
            api_token=api_token,
            timeout_seconds=timeout_seconds,
        )
    frames: list[pd.DataFrame] = []
    query_dates = pd.date_range(start=start_timestamp, end=end_timestamp, freq='D')

    for stock_id in normalized_ids:
        for query_date in query_dates:
            raw_df = _request_finmind_dataset(
                FINMIND_BROKER_DATASET,
                params={
                    'data_id': stock_id,
                    'start_date': query_date.strftime('%Y-%m-%d'),
                },
                api_token=api_token,
                timeout_seconds=timeout_seconds,
            )

            if raw_df.empty:
                continue

            frames.append(
                _aggregate_finmind_broker_detail(
                    raw_df,
                    trader_lookup=trader_lookup,
                    stock_name_lookup=name_lookup,
                )
            )

    if not frames:
        raise ValueError('FinMind 分點資料查無結果，請確認股票代號、日期區間或 sponsor 權限')

    broker_df = pd.concat(frames, ignore_index=True)
    broker_df = _prepare_broker_trading_df(broker_df, 'FinMind 券商分點資料')

    if cache_path:
        _write_dataframe_cache(cache_path, broker_df, ['date'])

    return broker_df, warnings


def fetch_price_data_from_finmind(
    stock_ids: Iterable[str],
    start_date: str,
    end_date: str,
    api_token: str,
    stock_name_lookup: Mapping[str, str] | None = None,
    cache_dir: str | None = None,
    force_refresh: bool = False,
    timeout_seconds: int = 30,
) -> pd.DataFrame:
    """從 FinMind 自動下載股價資料並轉成既有 price_data 契約。"""
    normalized_ids = _normalize_stock_id_list(stock_ids)
    if not normalized_ids:
        raise ValueError('請至少提供一檔有效股票代號')

    start_timestamp = _normalize_date_input(start_date, '股價開始日期')
    end_timestamp = _normalize_date_input(end_date, '股價結束日期')

    if end_timestamp < start_timestamp:
        raise ValueError('股價結束日期不可早於開始日期')
    if start_timestamp < FINMIND_PRICE_DATA_START:
        raise ValueError('FinMind 股價資料最早僅支援 1994-10-01 之後的日期')

    cache_path = _build_finmind_cache_path(
        data_kind='price',
        cache_dir=cache_dir,
        stock_ids=normalized_ids,
        start_date=start_timestamp,
        end_date=end_timestamp,
    )

    if cache_path and cache_path.exists() and not force_refresh:
        return load_price_data(cache_path)

    name_lookup = {str(key): str(value) for key, value in (stock_name_lookup or {}).items()}
    if not name_lookup:
        name_lookup = fetch_stock_name_lookup_from_finmind(
            normalized_ids,
            api_token=api_token,
            timeout_seconds=timeout_seconds,
        )

    frames: list[pd.DataFrame] = []
    for stock_id in normalized_ids:
        raw_df = _request_finmind_dataset(
            FINMIND_PRICE_DATASET,
            params={
                'data_id': stock_id,
                'start_date': start_timestamp.strftime('%Y-%m-%d'),
                'end_date': end_timestamp.strftime('%Y-%m-%d'),
            },
            api_token=api_token,
            timeout_seconds=timeout_seconds,
        )

        if raw_df.empty:
            continue

        mapped_df = raw_df.copy()
        mapped_df['stock_name'] = mapped_df['stock_id'].map(name_lookup).fillna('')
        frames.append(mapped_df)

    if not frames:
        raise ValueError('FinMind 股價資料查無結果，請確認股票代號與日期區間')

    price_df = _prepare_price_data_df(pd.concat(frames, ignore_index=True), 'FinMind 股價資料')

    if cache_path:
        _write_dataframe_cache(cache_path, price_df, ['date'])

    return price_df


def fetch_holder_data_from_finmind(
    stock_ids: Iterable[str],
    start_date: str,
    end_date: str,
    api_token: str,
    stock_name_lookup: Mapping[str, str] | None = None,
    cache_dir: str | None = None,
    force_refresh: bool = False,
    timeout_seconds: int = 30,
) -> pd.DataFrame:
    """從 FinMind 自動下載股權分級資料並彙總成集保戶數契約。"""
    normalized_ids = _normalize_stock_id_list(stock_ids)
    if not normalized_ids:
        raise ValueError('請至少提供一檔有效股票代號')

    start_timestamp = _normalize_date_input(start_date, '集保開始日期')
    end_timestamp = _normalize_date_input(end_date, '集保結束日期')

    if end_timestamp < start_timestamp:
        raise ValueError('集保結束日期不可早於開始日期')
    if start_timestamp < FINMIND_HOLDER_DATA_START:
        raise ValueError('FinMind 集保資料最早僅支援 2010-01-29 之後的日期')

    cache_path = _build_finmind_cache_path(
        data_kind='holder',
        cache_dir=cache_dir,
        stock_ids=normalized_ids,
        start_date=start_timestamp,
        end_date=end_timestamp,
    )

    if cache_path and cache_path.exists() and not force_refresh:
        return load_holder_data(cache_path)

    name_lookup = {str(key): str(value) for key, value in (stock_name_lookup or {}).items()}
    if not name_lookup:
        name_lookup = fetch_stock_name_lookup_from_finmind(
            normalized_ids,
            api_token=api_token,
            timeout_seconds=timeout_seconds,
        )

    frames: list[pd.DataFrame] = []
    for stock_id in normalized_ids:
        raw_df = _request_finmind_dataset(
            FINMIND_HOLDER_DATASET,
            params={
                'data_id': stock_id,
                'start_date': start_timestamp.strftime('%Y-%m-%d'),
                'end_date': end_timestamp.strftime('%Y-%m-%d'),
            },
            api_token=api_token,
            timeout_seconds=timeout_seconds,
        )

        if raw_df.empty:
            continue

        mapped_df = raw_df.copy()
        mapped_df['stock_name'] = mapped_df['stock_id'].map(name_lookup).fillna('')
        frames.append(mapped_df)

    if not frames:
        raise ValueError('FinMind 集保戶數資料查無結果，請確認股票代號、日期區間或 sponsor 權限')

    holder_raw_df = pd.concat(frames, ignore_index=True)
    holder_raw_df['date'] = pd.to_datetime(holder_raw_df['date'], errors='coerce')
    holder_raw_df['people'] = pd.to_numeric(holder_raw_df['people'], errors='coerce').fillna(0)

    holder_df = _prepare_holder_data_df(
        holder_raw_df.rename(columns={'date': 'week_date'}),
        'FinMind 集保戶數資料',
    )

    if cache_path:
        _write_dataframe_cache(cache_path, holder_df, ['week_date'])

    return holder_df


def load_broker_trading(file) -> pd.DataFrame:
    """
    載入券商分點買賣超資料（CSV / Excel 匯入模式）

    必要欄位：date, stock_id, stock_name, broker, branch,
              buy_volume, sell_volume, net_buy
    選擇欄位：buy_avg_price（缺失時將由收盤價補充）

    回傳：清理後的 DataFrame
    """
    try:
        df = pd.read_csv(file)
        return _prepare_broker_trading_df(df, '券商分點資料')

    except Exception as e:
        raise ValueError(f"載入券商分點資料失敗：{e}")


def load_price_data(file) -> pd.DataFrame:
    """
    載入股價資料（CSV / Excel 匯入模式）

    必要欄位：date, stock_id, open, high, low, close, volume
    """
    try:
        df = pd.read_csv(file)
        return _prepare_price_data_df(df, '股價資料')

    except Exception as e:
        raise ValueError(f"載入股價資料失敗：{e}")


def load_holder_data(file) -> pd.DataFrame:
    """
    載入集保戶數資料（CSV / Excel 匯入模式）

    必要欄位：week_date, stock_id, holder_count
    """
    try:
        df = pd.read_csv(file)
        return _prepare_holder_data_df(df, '集保戶數資料')

    except Exception as e:
        raise ValueError(f"載入集保戶數資料失敗：{e}")


def merge_price_to_broker(
    broker_df: pd.DataFrame,
    price_df: pd.DataFrame
) -> pd.DataFrame:
    """
    將每日收盤價合併至券商分點資料，用於補充缺失的買進均價。

    新增欄位 effective_buy_price：
    - 若 buy_avg_price 有值 → 使用 buy_avg_price
    - 若 buy_avg_price 缺失 → 使用當日收盤價近似
    """
    price_simple = (
        price_df[['date', 'stock_id', 'close']]
        .sort_values(['stock_id', 'date'])
        .drop_duplicates(subset=['date', 'stock_id'], keep='last')
        .copy()
    )

    merged = broker_df.merge(
        price_simple,
        on=['date', 'stock_id'],
        how='left'
    )

    # 買進均價缺失時以收盤價代替
    merged['effective_buy_price'] = merged['buy_avg_price'].where(
        merged['buy_avg_price'].notna(),
        merged['close']
    )

    return merged


def filter_by_volume(
    broker_df: pd.DataFrame,
    price_df: pd.DataFrame,
    min_volume: int
) -> pd.DataFrame:
    """
    依據最低平均成交量過濾低流動性股票。

    回傳：過濾後的 broker_df（僅保留成交量達標的股票）
    """
    if min_volume <= 0:
        return broker_df

    avg_vol = price_df.groupby('stock_id')['volume'].mean()
    valid_ids = avg_vol[avg_vol >= min_volume].index.tolist()

    return broker_df[broker_df['stock_id'].isin(valid_ids)].copy()


# ──────────────────────────────────────────────
# TODO：API / 爬蟲模式（未來可補充下列函數）
# ──────────────────────────────────────────────

# def fetch_broker_trading_from_api(
#     stock_id: str, start_date: str, end_date: str
# ) -> pd.DataFrame:
#     """
#     TODO: 從 API 取得券商分點買賣超資料
#     可考慮串接：
#     - 富果 API (Fugle MarketData)：https://developer.fugle.tw/
#     - 永豐金 SinoPac API：https://sinopacapi.github.io/
#     - 台灣證券交易所公開資料：https://www.twse.com.tw/
#     """
#     raise NotImplementedError("API 模式尚未實作，請使用 CSV 匯入模式")


# def fetch_price_from_twse(stock_id: str, date: str) -> pd.DataFrame:
#     """
#     TODO: 從台灣證券交易所取得個股日線資料
#     參考：https://www.twse.com.tw/exchangeReport/STOCK_DAY
#     """
#     raise NotImplementedError("API 模式尚未實作，請使用 CSV 匯入模式")


# def fetch_holder_from_tdcc(stock_id: str) -> pd.DataFrame:
#     """
#     TODO: 從台灣集中保管結算所取得集保戶數統計
#     參考：https://www.tdcc.com.tw/portal/zh/smoffshore/stat
#     """
#     raise NotImplementedError("API 模式尚未實作，請使用 CSV 匯入模式")
