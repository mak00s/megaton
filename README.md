# megaton

Megaton は Google アナリティクス（GA4／GA3）、Google Sheets、BigQuery を Notebook 上から直感的に扱うためのツール群です。認証からデータ取得、保存までの流れをウィジェット操作でまとめ、コードを書くことなく Google の各サービスを横断できます。

## できること

- **認証の自動化**: OAuth クライアントやサービスアカウントの JSON を読み込み、Notebook 上で安全に接続します。
- **Google アナリティクス連携**: GA4／Universal Analytics のアカウント・プロパティ・レポートをウィザード形式で選択し、API 呼び出しを自動生成します。
- **スプレッドシート・BigQuery とのやり取り**: Notebook で得たデータをそのまま Google Sheets に書き出したり、BigQuery テーブルとして保存できます。
- **データ整形ユーティリティ**: DataFrame の日付型変換、URL クエリの整理、列名変更など、分析前の処理を支援する関数を提供します。
- **Google Drive 連携**: 認証済みの Google Drive からファイルを選択し、レポートの入力や出力に利用できます。

## 代表的なユースケース

- GA レポートを定期的に取得し、Google Sheets へ配布するテンプレートを作成する。
- BigQuery に蓄積したデータや GA4 の生データを Notebook で確認しながら可視化する。
- マーケティングチーム向けに、アカウント選択から指標出力までをワンクリックで行える Notebook を整備する。
- 既存の GA3 プロパティと GA4 プロパティを比較するアドホック分析を効率化する。

## 動作に必要なもの

1. Python 3.7 以上が動作する環境（Jupyter Notebook／JupyterLab／Google Colab など）。
2. Google Cloud Console で有効化した API と、対応する認証情報（クライアントシークレット JSON またはサービスアカウント JSON）。
3. （任意）Google Sheets や BigQuery を利用する場合、該当サービスの API 有効化と権限付与。

## インストール

配布パッケージは PyPI には未公開です。以下のコマンドで依存ライブラリと本体を GitHub から直接インストールできます。

```bash
pip install -r https://raw.githubusercontent.com/mak00s/megaton/main/requirements.txt
pip install -U "git+https://github.com/mak00s/megaton"
```

## はじめ方

1. 認証 JSON を用意し、Notebook からアクセスできる場所に配置します。
2. Notebook で `Megaton` クラスを読み込みます。

```python
from megaton.start import Megaton

app = Megaton(path="/path/to/credentials", use_ga3=True)
```

- `path` にディレクトリを渡すと、含まれる JSON を自動探索し選択メニューが表示されます。
- 単一ファイル、JSON 文字列、辞書形式でも認証可能です。
- `use_ga3=True` を指定すると GA3 のウィジェットも有効化されます。

3. 表示されたウィジェットで認証を完了し、アカウント／プロパティ／データセットを選択します。
4. 取得したデータはテーブル表示で確認でき、必要に応じて CSV・Google Sheets・BigQuery へ保存できます。

## 困ったときは

- **認証が通らない**: JSON がサービスアカウントか OAuth クライアントかを確認し、必要なスコープが有効になっているかをチェックしてください。
- **API が見つからないと言われる**: `install/requirements-*.txt` の追加パッケージを再インストールし、Google Cloud Console 側で API を有効化します。
- **ウィジェットが表示されない**: Notebook／ブラウザを再起動し、ブラウザ拡張によるブロックがないか確認してください。

## ライセンス

MIT License
