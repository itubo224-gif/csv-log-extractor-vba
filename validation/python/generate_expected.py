import argparse
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd


# ===== 固定値 =====
SHEET_CASES = "1. ケース一覧"
SHEET_COMMON = "2. 共通設定"
SHEET_CONV = "3. 信号変換設定"
SHEET_TIME = "4. 時刻範囲設定"
SHEET_ROW = "5. 行フィルタ設定"

CASE_COL_TEST_ID = "テストID"
CASE_COL_OUTPUT_FILE = "出力ファイル名"
CASE_COL_CONV = "信号変換"
CASE_COL_TIME = "時刻範囲設定"
CASE_COL_ROW = "行フィルタ設定"
CASE_COL_RUN_TARGET = "実行対象"

COMMON_KEY_INPUT_DIR = "入力フォルダPath"
COMMON_KEY_OUTPUT_DIR = "出力フォルダPath"

DEFAULT_DATA_SHEET_NAME = "Data"
SUMMARY_FILE_NAME = "summary.csv"

EPSILON = 1e-9

# ===== 共通設定 =====
COMMON_HEADER_NAME = "項目名"
COMMON_KEY_COMPARE_OUTPUT_DIR = "比較結果フォルダPath"
COMMON_COL_KEY = 1
COMMON_COL_VALUE = 2

# ===== 信号変換設定シート列 =====
CONV_COL_NAME = 1          # 設定名
CONV_COL_SIGNAL = 3        # 信号名
CONV_COL_ONOFF = 4         # ON/OFF
CONV_COL_K = 5             # k
CONV_COL_B = 6             # b

# ===== 時刻範囲設定シート列 =====
TIME_COL_NAME = 1
TIME_COL_ONOFF = 2
TIME_COL_START = 3
TIME_COL_END = 4

# ===== 行フィルタ設定シート列 =====
ROW_COL_NAME = 1
ROW_COL_ONOFF = 2
ROW_COL_SIGNAL = 3
ROW_COL_OPERATOR = 4
ROW_COL_VALUE = 5
ROW_COL_TOL = 6

DETAIL_DIR_NAME = "compare_outputs"

# ===== 共通読込 =====
def load_excel_sheets(book_path: Path) -> Dict[str, pd.DataFrame]:
    return {
        SHEET_CASES: pd.read_excel(book_path, sheet_name=SHEET_CASES, header=1),
        SHEET_COMMON: pd.read_excel(book_path, sheet_name=SHEET_COMMON, header=1),
        SHEET_CONV: pd.read_excel(book_path, sheet_name=SHEET_CONV, header=1),
        SHEET_TIME: pd.read_excel(book_path, sheet_name=SHEET_TIME, header=1),
        SHEET_ROW: pd.read_excel(book_path, sheet_name=SHEET_ROW, header=1),
        # SHEET_CASES: pd.read_excel(book_path, sheet_name=SHEET_CASES),
        # SHEET_COMMON: pd.read_excel(book_path, sheet_name=SHEET_COMMON, header=None),
        # SHEET_CONV: pd.read_excel(book_path, sheet_name=SHEET_CONV),
        # SHEET_TIME: pd.read_excel(book_path, sheet_name=SHEET_TIME),
        # SHEET_ROW: pd.read_excel(book_path, sheet_name=SHEET_ROW),
    }


def load_common_settings(df_common: pd.DataFrame) -> Dict[str, str]:
    settings: Dict[str, str] = {}

    for _, row in df_common.iterrows():
        key = str(row.iloc[COMMON_COL_KEY]).strip()

        # ←ここをConst化
        if key == "" or key == COMMON_HEADER_NAME:
            continue

        value = "" if pd.isna(row.iloc[COMMON_COL_VALUE]) else str(row.iloc[COMMON_COL_VALUE]).strip()
        settings[key] = value

    return settings


def get_case_rows(df_cases: pd.DataFrame, test_id: Optional[str] = None) -> pd.DataFrame:
    df = df_cases.copy()

    if test_id:
        return df[df[CASE_COL_TEST_ID].astype(str).str.strip() == test_id].copy()

    run_target = df[CASE_COL_RUN_TARGET].astype(str).str.strip().str.upper()
    return df[run_target == "ON"].copy()


# ===== 設定読込 =====
def load_conv_config(df_conv: pd.DataFrame, conv_name: str) -> Dict[str, Any]:
    target = df_conv[df_conv.iloc[:, CONV_COL_NAME].astype(str).str.strip() == conv_name].copy()

    if target.empty:
        raise ValueError(f"信号変換設定が見つかりません: {conv_name}")

    signals: List[Dict[str, Any]] = []

    for _, row in target.iterrows():
        signal_name = str(row.iloc[CONV_COL_SIGNAL]).strip()
        convert_on_text = str(row.iloc[CONV_COL_ONOFF]).strip().upper()

        k = 1.0
        b = 0.0
        convert_on = convert_on_text == "ON"

        if convert_on:
            k = float(row.iloc[CONV_COL_K])
            b = float(row.iloc[CONV_COL_B])

        signals.append({
            "signal_name": signal_name,
            "convert_on": convert_on,
            "k": k,
            "b": b,
        })

    return {
        "name": conv_name,
        "signals": signals,
    }


def load_time_config(df_time: pd.DataFrame, time_name: str) -> Dict[str, Any]:
    target = df_time[df_time.iloc[:, TIME_COL_NAME].astype(str).str.strip() == time_name].copy()

    if target.empty:
        raise ValueError(f"時刻範囲設定が見つかりません: {time_name}")

    row = target.iloc[0]
    enabled = str(row.iloc[TIME_COL_ONOFF]).strip().upper() == "ON"

    return {
        "name": time_name,
        "enabled": enabled,
        "start": None if not enabled else float(row.iloc[TIME_COL_START]),
        "end": None if not enabled else float(row.iloc[TIME_COL_END]),
    }


def load_row_filter_config(df_row: pd.DataFrame, row_name: str) -> Dict[str, Any]:
    target = df_row[df_row.iloc[:, ROW_COL_NAME].astype(str).str.strip() == row_name].copy()

    if target.empty:
        raise ValueError(f"行フィルタ設定が見つかりません: {row_name}")

    row = target.iloc[0]
    enabled = str(row.iloc[ROW_COL_ONOFF]).strip().upper() == "ON"

    return {
        "name": row_name,
        "enabled": enabled,
        "signal": None if not enabled else str(row.iloc[ROW_COL_SIGNAL]).strip(),
        "operator": None if not enabled else str(row.iloc[ROW_COL_OPERATOR]).strip(),
        "value": None if not enabled else float(row.iloc[ROW_COL_VALUE]),
        "tolerance": None if not enabled or pd.isna(row.iloc[ROW_COL_TOL]) else float(row.iloc[ROW_COL_TOL]),
    }

def build_case_config(
    case_row: pd.Series,
    common_settings: Dict[str, str],
    df_conv: pd.DataFrame,
    df_time: pd.DataFrame,
    df_row: pd.DataFrame,
) -> Dict[str, Any]:
    test_id = str(case_row[CASE_COL_TEST_ID]).strip()
    output_file = str(case_row[CASE_COL_OUTPUT_FILE]).strip()
    conv_name = str(case_row[CASE_COL_CONV]).strip()
    time_name = str(case_row[CASE_COL_TIME]).strip()
    row_name = str(case_row[CASE_COL_ROW]).strip()

    return {
        "test_id": test_id,
        "output_file": output_file,
        "input_dir": Path(common_settings[COMMON_KEY_INPUT_DIR]),
        "output_dir": Path(common_settings[COMMON_KEY_OUTPUT_DIR]),
        "conv": load_conv_config(df_conv, conv_name),
        "time": load_time_config(df_time, time_name),
        "row": load_row_filter_config(df_row, row_name),
    }


# ===== 入力読込 =====
def load_input_logs(input_dir: Path) -> pd.DataFrame:
    csv_files = sorted(input_dir.glob("*.csv"))

    if not csv_files:
        raise FileNotFoundError(f"入力CSVが見つかりません: {input_dir}")

    frames = []
    for path in csv_files:
        df = pd.read_csv(path)
        df["SourceFile"] = path.name
        frames.append(df)

    return pd.concat(frames, ignore_index=True)


# ===== 期待値生成 =====
def apply_time_filter(df: pd.DataFrame, time_cfg: Dict[str, Any]) -> pd.DataFrame:
    if not time_cfg["enabled"]:
        return df.copy()

    time_col = df.columns[0]
    start = time_cfg["start"]
    end = time_cfg["end"]

    return df[(df[time_col] >= start) & (df[time_col] <= end)].copy()


def compare_with_tolerance(series: pd.Series, operator: str, value: float, tolerance: Optional[float]) -> pd.Series:
    tol = 0.0 if tolerance is None else float(tolerance)

    if operator == "==":
        return (series - value).abs() <= tol
    if operator == "!=":
        return (series - value).abs() > tol
    if operator == ">":
        return series > value
    if operator == ">=":
        return series >= value
    if operator == "<":
        return series < value
    if operator == "<=":
        return series <= value

    raise ValueError(f"未対応の演算子です: {operator}")

def get_row_filter_series(
    df: pd.DataFrame,
    signal: str,
    conv_cfg: Dict[str, Any],
) -> pd.Series:
    if signal not in df.columns:
        raise KeyError(f"行フィルタ対象信号が見つかりません: {signal}")

    for sig_cfg in conv_cfg["signals"]:
        if sig_cfg["signal_name"] != signal:
            continue

        if sig_cfg["convert_on"]:
            return df[signal] * sig_cfg["k"] + sig_cfg["b"]

        return df[signal]

    return df[signal]

def apply_row_filter(df: pd.DataFrame, row_cfg: Dict[str, Any], conv_cfg: Dict[str, Any]) -> pd.DataFrame:
    if not row_cfg["enabled"]:
        return df.copy()

    signal = row_cfg["signal"]
    series = get_row_filter_series(df, signal, conv_cfg)

    mask = compare_with_tolerance(
        series=series,
        operator=row_cfg["operator"],
        value=row_cfg["value"],
        tolerance=row_cfg["tolerance"],
    )
    return df[mask].copy()


def apply_signal_extract_and_convert(df: pd.DataFrame, conv_cfg: Dict[str, Any]) -> pd.DataFrame:
    df = df.reset_index(drop=True)

    time_col = df.columns[0]
    result = pd.DataFrame(index=df.index)
    result[time_col] = df[time_col]

    for sig_cfg in conv_cfg["signals"]:
        signal_name = sig_cfg["signal_name"]

        if signal_name not in df.columns:
            raise KeyError(f"抽出対象信号が見つかりません: {signal_name}")

        if sig_cfg["convert_on"]:
            result[f"{signal_name}_conv"] = df[signal_name] * sig_cfg["k"] + sig_cfg["b"]
        else:
            result[signal_name] = df[signal_name]

    return result


def build_expected_data(df_in: pd.DataFrame, case_cfg: Dict[str, Any]) -> pd.DataFrame:
    df = df_in.copy()
    df = apply_time_filter(df, case_cfg["time"])
    df = apply_row_filter(df, case_cfg["row"], case_cfg["conv"])
    df = apply_signal_extract_and_convert(df, case_cfg["conv"])
    df = df.reset_index(drop=True)
    return df


# ===== 実データ読込 =====
def load_actual_data(output_file_path: Path, data_sheet_name: str = DEFAULT_DATA_SHEET_NAME) -> pd.DataFrame:
    if not output_file_path.exists():
        raise FileNotFoundError(f"VBA出力ファイルが見つかりません: {output_file_path}")

    df = pd.read_excel(output_file_path, sheet_name=data_sheet_name)
    return df.copy()


# ===== 比較 =====
def normalize_for_compare(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    # SourceFile列が実データ側にあっても比較対象外にしたい場合はここで落とす
    if "SourceFile" in out.columns:
        out = out.drop(columns=["SourceFile"])

    for col in out.columns:
        if pd.api.types.is_numeric_dtype(out[col]):
            out[col] = pd.to_numeric(out[col], errors="coerce")
        else:
            out[col] = out[col].astype(str).fillna("")

    out = out.reset_index(drop=True)
    return out


def compare_dataframes(expected_df: pd.DataFrame, actual_df: pd.DataFrame) -> Dict[str, Any]:
    exp = normalize_for_compare(expected_df)
    act = normalize_for_compare(actual_df)

    result: Dict[str, Any] = {
        "columns_match": list(exp.columns) == list(act.columns),
        "row_count_match": len(exp) == len(act),
        "value_match": False,
        "message": "",
    }

    if not result["columns_match"]:
        result["message"] = "列名不一致"
        return result

    if not result["row_count_match"]:
        result["message"] = f"行数不一致 expected={len(exp)} actual={len(act)}"
        return result

    for r in range(len(exp)):
        for c in exp.columns:
            exp_val = exp.at[r, c]
            act_val = act.at[r, c]

            if pd.api.types.is_number(exp_val) and pd.api.types.is_number(act_val):
                if not np.isclose(exp_val, act_val, atol=EPSILON, rtol=0):
                    result["message"] = f"値不一致 row={r + 1}, col={c}, expected={exp_val}, actual={act_val}"
                    return result
            else:
                if str(exp_val) != str(act_val):
                    result["message"] = f"値不一致 row={r + 1}, col={c}, expected={exp_val}, actual={act_val}"
                    return result

    result["value_match"] = True
    result["message"] = "OK"
    return result

def save_case_detail_files(
    output_base_dir: Path,
    test_id: str,
    expected_df: pd.DataFrame,
    actual_df: pd.DataFrame,
) -> None:
    output_base_dir.mkdir(parents=True, exist_ok=True)

    expected_path = output_base_dir / f"{test_id}_expected.csv"
    actual_path = output_base_dir / f"{test_id}_actual.csv"

    expected_df.to_csv(expected_path, index=False, encoding="utf-8-sig")
    actual_df.to_csv(actual_path, index=False, encoding="utf-8-sig")

# ===== 1ケース実行 =====
def run_one_case(case_cfg: Dict[str, Any], detail_dir: Path) -> Dict[str, Any]:
    test_id = case_cfg["test_id"]

    try:
        df_in = load_input_logs(case_cfg["input_dir"])
        expected_df = build_expected_data(df_in, case_cfg)

        actual_path = case_cfg["output_dir"] / case_cfg["output_file"]
        actual_df = load_actual_data(actual_path)

        save_case_detail_files(
            output_base_dir=detail_dir,
            test_id=test_id,
            expected_df=expected_df,
            actual_df=actual_df,
        )

        cmp_result = compare_dataframes(expected_df, actual_df)

        if cmp_result["columns_match"] and cmp_result["row_count_match"] and cmp_result["value_match"]:
            return {
                "test_id": test_id,
                "python_status": "OK",
                "compare_result": "OK",
                "error_message": "",
            }

        return {
            "test_id": test_id,
            "python_status": "OK",
            "compare_result": "NG",
            "error_message": cmp_result["message"],
        }

    except Exception as e:
        return {
            "test_id": test_id,
            "python_status": "ERROR",
            "compare_result": "NG",
            "error_message": str(e),
        }


# ===== メイン =====
def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--book", required=True, help="設定ブックのパス")
    parser.add_argument("--test-id", required=False, help="単体実行時のテストID")
    args = parser.parse_args()

    book_path = Path(args.book)
    if not book_path.exists():
        print(f"ERROR: 設定ブックが見つかりません: {book_path}", file=sys.stderr)
        return 2

    try:
        sheets = load_excel_sheets(book_path)
        common_settings = load_common_settings(sheets[SHEET_COMMON])

        case_rows = get_case_rows(sheets[SHEET_CASES], args.test_id)
        if case_rows.empty:
            print("ERROR: 対象ケースが見つかりません", file=sys.stderr)
            return 2

        summary_rows: List[Dict[str, str]] = []
        compare_output_dir_str = common_settings[COMMON_KEY_COMPARE_OUTPUT_DIR]
        detail_dir = Path(compare_output_dir_str)
        detail_dir.mkdir(parents=True, exist_ok=True)

        for _, case_row in case_rows.iterrows():
            case_cfg = build_case_config(
                case_row=case_row,
                common_settings=common_settings,
                df_conv=sheets[SHEET_CONV],
                df_time=sheets[SHEET_TIME],
                df_row=sheets[SHEET_ROW],
            )
            summary_rows.append(run_one_case(case_cfg, detail_dir))
        summary_df = pd.DataFrame(summary_rows)
        #summary_path = book_path.parent / SUMMARY_FILE_NAME
        summary_path = detail_dir / SUMMARY_FILE_NAME
        summary_df.to_csv(summary_path, index=False, encoding="utf-8-sig")

        if (summary_df["python_status"] == "ERROR").any():
            print(f"ERROR: summary saved to {summary_path}", file=sys.stderr)
            return 2

        if (summary_df["compare_result"] == "NG").any():
            print(f"NG: summary saved to {summary_path}")
            return 1

        print(f"OK: summary saved to {summary_path}")
        return 0

    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())