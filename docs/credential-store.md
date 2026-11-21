# Credential Store（認証ストア）

Available from Megaton 2.6.0.

1台のMacに1つの置き場（`~/.config/megaton`、または `$MEGATON_HOME`）を作り、Googleの資格情報をすべてそこに集める。コードやAIエージェントはファイルパスではなく**識別子**（`account="sim"`、`sa="corp-ga"`）で資格情報を引くため、repoが変わっても、エージェントが変わっても、同じ手順で認証できる。

## 2系統に分ける理由

| 用途 | 資格情報 | 再認証の頻度 | 識別子 |
|---|---|---|---|
| Workspaceの中身（Docs・Drive・Sheets・Gmail・GA4・GSC・GTM） | ユーザーOAuthトークン（`users/<email>.json`） | Google Cloudのセッション制御の対象外（Cloudスコープを含めない場合）。ただし下記の条件で失効しうる | `account` |
| Google Cloud（BigQuery・Cloud Run など） | サービスアカウント鍵（`sa/<name>.json`） | 期限なし（鍵の削除・無効化、組織ポリシーの鍵有効期限を除く） | `sa` |

gcloudのユーザー認証（`gcloud auth login`）はWorkspace管理者のセッション制御で毎日失効する。APIを叩く用途にはこのストアを使い、gcloudはGCPの操作だけに使う。

**ユーザートークンの失効**: セッション制御の対象外であることは、失効しないことを意味しない。リフレッシュトークンは次の場合に無効になる（[Google公式の失効条件](https://developers.google.com/identity/protocols/oauth2#expiration)）。

- 本人がアカウント設定でアプリのアクセスを取り消した
- 6か月間使われなかった
- パスワードを変更し、トークンにGmailスコープが含まれていた
- 同じOAuthクライアント・同じアカウントで発行数の上限を超え、古いものから無効になった
- Workspace管理者がアプリやスコープを制限した、またはCloudスコープを含むトークンがGoogle Cloudのセッション制御を受けた
- OAuthクライアントのプロジェクトが外部向けかつ公開ステータス「テスト」で、発行から7日が経過した

公開ステータスはGoogle CloudコンソールのOAuth同意画面で確認する。`megaton auth check` の `ok` は確認時点で更新できたことだけを示し、公開ステータスや今後の有効性は保証しない。失効した場合は `check` が `NG` と再認可の `megaton auth login ...` を表示する。

**保存される内容**: トークンの `scopes` は Google が実際に許可した範囲（同意画面で外した分は含まない）。`login` は要求した範囲が許可されなかったとき警告して終了コード1を返す。`import --user-token` は所有者をオンラインで必ず確認し、確認できないトークンは取り込まない。書き込みは一時ファイル＋置換（原子的）。

refresh後も要求scopeを検証する。`granted_scopes=[]` は明示的な空の権限として保存し、
必要なscopeがあればエラーにする。`granted_scopes=None`（応答にscope情報がない場合）
に限り、従来のscope情報を使用する。

## 配置

```
~/.config/megaton/
  credentials.toml        # 別名の台帳（手で編集）
  oauth-client.json       # `megaton auth login` が使うOAuthクライアント（デスクトップ型）
  users/sim@example.com.json
  sa/sa_corp-1872_py-ga-gsc-api.json
```

`credentials.toml`:

```toml
[oauth]
client = "oauth-client.json"

[users]
sim = "sim@example.com"

[sa]
corp-ga = "sa_corp-1872_py-ga-gsc-api.json"
```

鍵ファイルは各Macにコピーして置く（Dropbox等で同期しない）。ディレクトリとファイルは `0700` / `0600`。

## CLI

```bash
megaton auth init                                   # 置き場と台帳の雛形を作る
megaton auth import --oauth-client client.json      # OAuthクライアントを登録
megaton auth import --sa key.json --name corp-ga    # SA鍵を取り込む
megaton auth import --user-token old_token.json     # 既存トークンを取り込む（メールを検証）
megaton auth login --account sim@example.com --scopes docs,drive,sheets
megaton auth list                                   # 何があるか
megaton auth check                                  # 更新できるか（失敗時は直すコマンドを表示）
megaton auth check --account sim                    # 指定アカウントが未登録・失効なら終了コード1
megaton auth token --account sim --scopes docs      # curl用のアクセストークン
megaton auth path --sa corp-ga                      # gspread等にパスを渡したいとき
megaton auth scopes                                 # スコープ別名の一覧
```

`login` は既存トークンのスコープに要求スコープを足して再認可する（1ユーザー1トークン、段階的に拡張）。ブラウザでサインインしたアカウントが `--account` と違えば保存しない。

## Python

```python
from megaton import Megaton
from megaton.auth import CredentialStore

mg = Megaton(sa="corp-ga", headless=True)                       # GA4/GSC/Sheets
mg = Megaton(account="sim", scopes=["sheets"], headless=True)   # ユーザーのトークン

store = CredentialStore()
creds = store.user_credentials("sim", scopes="docs,drive")      # google.oauth2.credentials.Credentials
token = store.user_access_token("sim", "docs")                  # str
path = store.service_account_path("corp-ga")                    # Path（gspread.service_account(filename=path)）
sa_creds = store.service_account_credentials("corp-ga", "sheets,drive")
```

`user_credentials` は要求スコープがトークンに無ければ `CredentialStoreError` を投げる。`hint` に実行すべき `megaton auth login ...` が入るので、エージェントはそれをそのまま案内する。スコープは要求で広がらない（再認可は必ず人がブラウザで行う）。

## curl から使う

```bash
megaton auth token --account sim --scopes docs --check
megaton auth exec --account sim --scopes docs -- sh -c \
  'curl -s -H "Authorization: Bearer $GOOGLE_ACCESS_TOKEN" "https://docs.googleapis.com/v1/documents/<id>?includeTabsContent=true"'
```

`exec` は既定で子プロセスの `GOOGLE_ACCESS_TOKEN` に渡す（`--env NAME` で変更。名前は `_TOKEN` で終わる大文字に限る。`PATH` や `PYTHONWARNINGS` など、プログラムが解釈して値を表示しうる変数には渡さない）。認証失敗時は子を起動せず、成功時は子の終了コードを返す。子からトークンを出力しないこと。`token` の通常モードは従来どおり stdout にトークン1行、エラーは stderr。`2>/dev/null` で捨てず、`exec` または `token --check` を使う。別名一覧は `megaton auth scopes`。`documents` は `docs`、`spreadsheets` は `sheets` を候補として示し、誤記は拒否する。

`token` / `token --check` / `exec` の認証終了コードは 0: 成功、2: 名前・引数誤り、3: スコープ不足、4: アカウント・トークン未登録、5: 更新・ネットワーク・ストア障害。`exec` の起動失敗は 126/127。既存の一括 `auth check` の 0/1 は維持する。`megaton --version` でパッケージ版と読み込み元を確認できる。

## 既存のパス指定からの移行ヒント

`Megaton("/path/key.json")` や `megaton query --credential /path/key.json` のようにファイルパスで認証すると、成功後に標準エラーへ1行だけヒントを出す。同じサービスアカウントがストアにあれば `sa="<名前>"`（CLIは `--sa <名前>`）を、なければ `megaton auth import ...` を案内する。OAuthクライアントのファイルなら `megaton auth login` を案内する。

- 同じファイルについては1プロセスで1回だけ。標準出力には出さない（JSON出力を壊さない）
- CI（`CI` / `GITHUB_ACTIONS`）では出さない。ストアはMacごとの置き場でCIには無いため
- `MEGATON_HINTS=0` で止められる
- ヒントの処理が失敗しても認証には影響しない

## スコープ別名

`docs`, `drive`, `drive.file`, `sheets`, `gmail.readonly`, `gmail.modify`, `gmail.compose`, `calendar`, `analytics`, `analytics.edit`, `webmasters`, `tagmanager`, `tagmanager.edit`, `bigquery`, `cloud-platform` など。`megaton auth scopes` で全一覧。上位スコープ（`drive` は `sheets`/`docs` を含む、`gmail.modify` は `gmail.readonly` を含む）は自動で充足と判定する。

## 別のMacへの複製

1. 新しいMacで `pip install megaton` と `megaton auth init`
2. `credentials.toml`・`oauth-client.json`・`users/`・`sa/` を安全な経路（AirDrop等）でコピー
3. `megaton auth check` で全ユーザーが `ok` になることを確認

## GitHub Actions の鍵なし認証（Workload Identity Federation）

`external_account` 型の認証設定をサポートします。SA鍵と同じファイル指定の入口で
`google.auth.load_credentials_from_file(..., scopes=...)` を使い、成り代わるSAの
メールは `service_account_impersonation_url` から取得します。取得不能なら不明として扱い、
別の資格情報に切り替えません。既存の `service_account` 鍵ファイルの動作は維持します。

```yaml
permissions:
  contents: read
  id-token: write

jobs:
  report:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      - uses: google-github-actions/auth@v3
        with:
          project_id: your-project
          workload_identity_provider: projects/123456789/locations/global/workloadIdentityPools/github/providers/actions
          service_account: report-reader@your-project.iam.gserviceaccount.com
          create_credentials_file: true
          export_environment_variables: true
      - run: python -m pip install 'megaton>=2.9.0'
      - name: Read report
        run: python report.py
```

`auth` が設定する `GOOGLE_APPLICATION_CREDENTIALS` は一時的な認証設定ファイルを指します。
その後の同じjobのstepで使用します。`Megaton` の選択順は `account=` / `sa=`、
明示した `credential`、`MEGATON_CREDS_JSON`、`GOOGLE_APPLICATION_CREDENTIALS`。
選ばれた認証が失敗しても別の資格情報にはフォールバックしません。

```python
# report.py: GA初期化を省き、Sheets/Drive用のスコープを指定
from megaton import Megaton

mg = Megaton(headless=True, initialize_ga=False,
             scopes=['sheets', 'drive'])
# 通常の mg.sheets / mg.sheet 操作で読み取る
```

`external_account` 使用時の `Megaton(..., scopes=...)` には別名またはスコープURLを
指定できます。省略時は従来の `DEFAULT_SCOPES`。GA4利用時は `analytics` 等、
BigQuery利用時は `cloud-platform` 等、その処理に必要なスコープを指定してください。
通常の `mg.sheets` / `mg.sheet` は従来どおり `sheets,drive` スコープを要求します。
読み取り専用スコープを指定する場合は、次の低レベルAPIを利用できます。
Sheetsの低レベルAPIも同じファイルを受け取ります。

```python
import os
from googleapiclient.discovery import build
from megaton.auth import load_service_account_credentials_from_file
from megaton.gsheet_lowlevel import open_spreadsheet

path = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
creds = load_service_account_credentials_from_file(path, [
    'https://www.googleapis.com/auth/documents.readonly',
    'https://www.googleapis.com/auth/drive.readonly',
])
docs = build('docs', 'v1', credentials=creds)
drive = build('drive', 'v3', credentials=creds)
spreadsheet = open_spreadsheet(spreadsheet_id='<id>', credentials_path=path,
    scopes=['https://www.googleapis.com/auth/spreadsheets.readonly'])
```

単発クエリCLIの明示認証の契約は維持します。
GA4/GSCでは `--credential "$GOOGLE_APPLICATION_CREDENTIALS"`、
BigQueryでは同じ指定か `--adc` を使います。`sa=` の既存ファイル参照も
`external_account` に対応しますが、Actionsの一時ファイルを `auth import --sa` で
永続コピーする運用にはしません（importは従来どおり鍵ファイル専用）。

設定ファイルは信頼できるworkflowが生成したものだけを使ってください。
WIFのプロバイダー・対象repo/refの制限・SAへの `roles/iam.workloadIdentityUser` と、
対象APIの権限は管理者があらかじめ設定します。GA4のプロパティ権限や
Sheets/Docs/Driveのファイル共有は対象SAに付与します。OAuthスコープの指定だけで
アクセス権が増えることはありません。鍵の作成や同意画面は不要です。

参考：[公式Actions設定](https://github.com/google-github-actions/auth)、
[google-authローダー](https://google-auth.readthedocs.io/en/latest/reference/google.auth.html)。
