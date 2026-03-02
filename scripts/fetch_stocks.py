#!/usr/bin/env python3
"""
fetch_stocks.py — 从新浪财经 API 获取全部沪深 A 股股票代码，写入 stocks_all.txt
用法：
    python3 scripts/fetch_stocks.py                  # 输出到 stocks_all.txt
    python3 scripts/fetch_stocks.py -o my_list.txt   # 指定输出文件
    python3 scripts/fetch_stocks.py --sample 100     # 同时生成 100 只样本
"""

import urllib.request
import json
import time
import sys
import argparse
from pathlib import Path


def fetch_node(node: str, total: int, delay: float = 0.05) -> list[str]:
    """分页拉取指定板块所有股票代码。"""
    symbols = []
    per_page = 100
    total_pages = (total + per_page - 1) // per_page
    print(f"  [{node}] {total_pages} 页 × {per_page} 条/页 = 约 {total} 只", flush=True)

    for page in range(1, total_pages + 1):
        url = (
            "http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php"
            f"/Market_Center.getHQNodeData?node={node}&page={page}"
            f"&num={per_page}&sort=symbol&asc=1&dpc=1"
        )
        try:
            with urllib.request.urlopen(url, timeout=15) as r:
                data = json.loads(r.read())
            batch = [d["symbol"] for d in data]
            symbols.extend(batch)
            print(f"    第 {page:3d}/{total_pages} 页: {len(batch)} 只", flush=True)
        except Exception as e:
            print(f"    第 {page} 页失败: {e}，跳过", file=sys.stderr)
        time.sleep(delay)

    return symbols


def get_node_count(node: str) -> int:
    url = (
        "http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php"
        f"/Market_Center.getHQNodeStockCount?node={node}"
    )
    with urllib.request.urlopen(url, timeout=10) as r:
        return int(r.read().strip().strip(b'"'))


def main():
    parser = argparse.ArgumentParser(description="获取沪深 A 股列表")
    parser.add_argument("-o", "--output", default="stocks_all.txt", help="输出文件路径")
    parser.add_argument("--sample", type=int, default=0, help="额外生成前 N 只的样本文件")
    args = parser.parse_args()

    print("正在查询各板块股票数量...")
    sh_count = get_node_count("sh_a")
    sz_count = get_node_count("sz_a")
    print(f"  上证 A 股: {sh_count} 只")
    print(f"  深证 A 股: {sz_count} 只")

    print("\n开始拉取上证 A 股...")
    sh_symbols = fetch_node("sh_a", sh_count)

    print("\n开始拉取深证 A 股...")
    sz_symbols = fetch_node("sz_a", sz_count)

    all_stocks = sorted(set(sh_symbols + sz_symbols))
    total = len(all_stocks)
    print(f"\n合并去重后共 {total} 只股票")

    out = Path(args.output)
    with open(out, "w", encoding="utf-8") as f:
        f.write(f"# 沪深 A 股全量列表（上证 {sh_count} + 深证 {sz_count}）\n")
        f.write(f"# 共 {total} 只，获取时间: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        for s in all_stocks:
            f.write(s + "\n")
    print(f"已写入 {out}（{total} 只）")

    if args.sample > 0:
        sample_path = out.parent / f"stocks_{args.sample}.txt"
        with open(sample_path, "w", encoding="utf-8") as f:
            f.write(f"# 样本 {args.sample} 只（从 stocks_all.txt 取前 {args.sample} 只）\n")
            for s in all_stocks[: args.sample]:
                f.write(s + "\n")
        print(f"已写入样本 {sample_path}（{args.sample} 只）")


if __name__ == "__main__":
    main()
